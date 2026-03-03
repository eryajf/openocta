package cron

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Service manages cron jobs.
type Service struct {
	storePath string
	mu        sync.RWMutex
	store     *StoreFile
	deps      *Deps
	done      chan struct{}
}

// cronRunLogEntry mirrors the TS CronRunLogEntry used by the control UI run history.
type cronRunLogEntry struct {
	Ts          int64  `json:"ts"`
	JobID       string `json:"jobId"`
	Action      string `json:"action"`
	Status      string `json:"status,omitempty"`
	Error       string `json:"error,omitempty"`
	Summary     string `json:"summary,omitempty"`
	SessionID   string `json:"sessionId,omitempty"`
	SessionKey  string `json:"sessionKey,omitempty"`
	RunAtMs     *int64 `json:"runAtMs,omitempty"`
	DurationMs  *int64 `json:"durationMs,omitempty"`
	NextRunAtMs *int64 `json:"nextRunAtMs,omitempty"`
}

// resolveRunLogPath returns the JSONL run log path for a job ID.
func (s *Service) resolveRunLogPath(jobID string) string {
	dir := filepath.Dir(s.storePath)
	return filepath.Join(dir, "runs", jobID+".jsonl")
}

// appendRunLogEntry appends one finished-action entry to the job's run log.
func (s *Service) appendRunLogEntry(job CronJob, status, errMsg, summary, sessionKey string, runAtMs, durationMs, nextRunAtMs *int64) {
	entry := cronRunLogEntry{
		Ts:          time.Now().UnixMilli(),
		JobID:       job.ID,
		Action:      "finished",
		Status:      status,
		Error:       errMsg,
		Summary:     summary,
		SessionKey:  sessionKey,
		RunAtMs:     runAtMs,
		DurationMs:  durationMs,
		NextRunAtMs: nextRunAtMs,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	path := s.resolveRunLogPath(job.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(data, '\n'))
}

// SetDeps sets execution dependencies (call after creation from gateway).
func (s *Service) SetDeps(deps *Deps) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deps = deps
}

// NewService creates a new cron service.
func NewService(storePath string) (*Service, error) {
	if storePath == "" {
		storePath = filepath.Join(".openocta", "cron", "jobs.json")
	}
	// 确保存储路径所在目录存在，不存在则创建
	dir := filepath.Dir(storePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}
	store, err := LoadStore(storePath)
	if err != nil {
		return nil, err
	}
	return &Service{
		storePath: storePath,
		store:     store,
	}, nil
}

// List returns all jobs, optionally including disabled.
func (s *Service) List(includeDisabled bool) ([]CronJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []CronJob
	for _, j := range s.store.Jobs {
		if j.Enabled || includeDisabled {
			out = append(out, j)
		}
	}
	return out, nil
}

// JobCreate is the input for adding a job.
type JobCreate struct {
	Name          string
	Schedule      CronSchedule
	Payload       CronPayload
	SessionTarget string
	WakeMode      string
	Enabled       bool
}

// Add adds a new job.
func (s *Service) Add(input JobCreate) (CronJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	j := CronJob{
		ID:            uuid.New().String(),
		Name:          input.Name,
		Enabled:       input.Enabled,
		CreatedAtMs:   now,
		UpdatedAtMs:   now,
		Schedule:      input.Schedule,
		SessionTarget: input.SessionTarget,
		WakeMode:      input.WakeMode,
		Payload:       input.Payload,
	}
	if j.SessionTarget == "" {
		j.SessionTarget = "main"
	}
	if j.WakeMode == "" {
		j.WakeMode = "next-heartbeat"
	}
	s.store.Jobs = append(s.store.Jobs, j)
	return j, SaveStore(s.storePath, s.store)
}

// JobPatch is a partial update for a job.
type JobPatch struct {
	Enabled  *bool
	Name     string
	Schedule *CronSchedule
}

// Update updates a job by ID.
func (s *Service) Update(id string, patch JobPatch) (CronJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.store.Jobs {
		if s.store.Jobs[i].ID == id {
			if patch.Enabled != nil {
				s.store.Jobs[i].Enabled = *patch.Enabled
			}
			if patch.Name != "" {
				s.store.Jobs[i].Name = patch.Name
			}
			if patch.Schedule != nil {
				s.store.Jobs[i].Schedule = *patch.Schedule
			}
			s.store.Jobs[i].UpdatedAtMs = time.Now().UnixMilli()
			j := s.store.Jobs[i]
			return j, SaveStore(s.storePath, s.store)
		}
	}
	return CronJob{}, nil // not found
}

// Remove removes a job by ID.
func (s *Service) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := range s.store.Jobs {
		if j.ID == id {
			s.store.Jobs = append(s.store.Jobs[:i], s.store.Jobs[i+1:]...)
			return SaveStore(s.storePath, s.store)
		}
	}
	return nil
}

// Run runs a job by ID. mode is "due" or "force".
func (s *Service) Run(id string, mode string) error {
	startMs := time.Now().UnixMilli()

	// Snapshot job and deps under lock so we can safely execute without holding the mutex.
	s.mu.Lock()
	var job *CronJob
	for i := range s.store.Jobs {
		if s.store.Jobs[i].ID == id {
			job = &s.store.Jobs[i]
			break
		}
	}
	if job == nil {
		s.mu.Unlock()
		return nil
	}
	jobCopy := *job
	deps := s.deps
	s.mu.Unlock()

	status := "ok"
	errMsg := ""
	var sessionKey string

	// Validate payload/sessionTarget combinations (mirrors TS semantics).
	if jobCopy.SessionTarget == "main" {
		if jobCopy.Payload.Kind != "systemEvent" {
			status = "skipped"
			errMsg = `main job requires payload.kind="systemEvent"`
		}
	} else if jobCopy.SessionTarget == "isolated" {
		if jobCopy.Payload.Kind != "agentTurn" {
			status = "skipped"
			errMsg = `isolated job requires payload.kind="agentTurn"`
		} else {
			// 对于隔离会话，统一使用 agent:main:cron:<jobId> 作为 sessionKey，
			// 便于网关和 UI 统一识别和管理 cron 会话。
			sessionKey = "agent:main:cron:" + jobCopy.ID
		}
	}

	// Execute side effects when not skipped and deps are available.
	if status != "skipped" {
		if deps == nil {
			status = "error"
			errMsg = "cron deps not configured"
		} else if jobCopy.SessionTarget == "main" && jobCopy.Payload.Kind == "systemEvent" {
			if deps.EnqueueSystemEvent != nil {
				deps.EnqueueSystemEvent(jobCopy.Payload.Text)
			}
			if jobCopy.WakeMode == "now" && deps.RequestHeartbeatNow != nil {
				deps.RequestHeartbeatNow("agent:main:cron:" + id)
			}
		} else if jobCopy.SessionTarget == "isolated" && jobCopy.Payload.Kind == "agentTurn" {
			// Prefer RunCronChat so that cron runs go through chat.send and
			// produce proper transcripts and session store entries. Fall back
			// to RunIsolatedAgentJob for backwards compatibility.
			if deps.RunCronChat != nil {
				deps.RunCronChat(jobCopy, sessionKey, jobCopy.Payload.Message)
			} else if deps.RunIsolatedAgentJob != nil {
				deps.RunIsolatedAgentJob(jobCopy, jobCopy.Payload.Message)
			}
		}
	}

	endMs := time.Now().UnixMilli()
	durationMs := endMs - startMs

	// Update job state and persist.
	s.mu.Lock()
	defer s.mu.Unlock()
	var nextRunAtMs *int64
	for i := range s.store.Jobs {
		if s.store.Jobs[i].ID == id {
			s.store.Jobs[i].State.LastRunAtMs = &endMs
			s.store.Jobs[i].State.LastStatus = status
			if errMsg != "" {
				s.store.Jobs[i].State.LastError = errMsg
			} else {
				s.store.Jobs[i].State.LastError = ""
			}
			s.store.Jobs[i].State.RunningAtMs = nil
			s.store.Jobs[i].State.LastDurationMs = &durationMs
			if status == "ok" {
				s.store.Jobs[i].State.ConsecutiveErrors = 0
			} else if status == "error" {
				s.store.Jobs[i].State.ConsecutiveErrors++
			}
			next := ComputeNextRunAtMs(s.store.Jobs[i].Schedule, endMs)
			s.store.Jobs[i].State.NextRunAtMs = &next
			nextRunAtMs = &next
			_ = SaveStore(s.storePath, s.store)
			// Use the full job value for logging.
			jobCopy = s.store.Jobs[i]
			break
		}
	}

	// Append run log entry without holding the lock.
	runAt := startMs
	s.appendRunLogEntry(jobCopy, status, errMsg, "", sessionKey, &runAt, &durationMs, nextRunAtMs)

	return nil
}

// RecomputeNextRuns updates NextRunAtMs for all jobs and persists.
func (s *Service) RecomputeNextRuns() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	for i := range s.store.Jobs {
		next := ComputeNextRunAtMs(s.store.Jobs[i].Schedule, now)
		s.store.Jobs[i].State.NextRunAtMs = &next
	}
	return SaveStore(s.storePath, s.store)
}

// NextWakeAtMs returns the soonest next run time in ms, or 0.
func (s *Service) NextWakeAtMs() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var min int64
	for _, j := range s.store.Jobs {
		if !j.Enabled || j.State.NextRunAtMs == nil {
			continue
		}
		n := *j.State.NextRunAtMs
		if n > 0 && (min == 0 || n < min) {
			min = n
		}
	}
	return min
}

// dueJobIDs returns job IDs that are due (NextRunAtMs <= nowMs). Caller holds no lock.
func (s *Service) dueJobIDs(nowMs int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []string
	for _, j := range s.store.Jobs {
		if !j.Enabled || j.State.NextRunAtMs == nil {
			continue
		}
		if *j.State.NextRunAtMs <= nowMs {
			ids = append(ids, j.ID)
		}
	}
	return ids
}

const maxTimerSleepMs = 60000

// Start starts the timer loop (recompute next runs, then sleep/wake and execute due jobs).
// Call Stop() to stop the loop.
func (s *Service) Start() {
	s.mu.Lock()
	if s.done != nil {
		s.mu.Unlock()
		return
	}
	s.done = make(chan struct{})
	s.mu.Unlock()
	_ = s.RecomputeNextRuns()
	go func() {
		for {
			nextMs := s.NextWakeAtMs()
			nowMs := time.Now().UnixMilli()
			sleepMs := int64(maxTimerSleepMs)
			if nextMs > 0 && nextMs > nowMs {
				d := nextMs - nowMs
				if d < sleepMs {
					sleepMs = d
				}
			}
			select {
			case <-time.After(time.Duration(sleepMs) * time.Millisecond):
				// fall through and run due jobs
			case <-s.done:
				return
			}
			nowMs = time.Now().UnixMilli()
			for _, id := range s.dueJobIDs(nowMs) {
				_ = s.Run(id, "due")
			}
			_ = s.RecomputeNextRuns()
		}
	}()
}

// Stop stops the timer loop.
func (s *Service) Stop() {
	s.mu.Lock()
	done := s.done
	s.done = nil
	s.mu.Unlock()
	if done != nil {
		close(done)
	}
}
