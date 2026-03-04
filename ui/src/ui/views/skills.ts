import { html, nothing } from "lit";
import type { SkillMessageMap } from "../controllers/skills.ts";
import type { SkillStatusEntry, SkillStatusReport } from "../types.ts";
import { clampText } from "../format.ts";
import { t } from "../strings.js";

type SkillGroup = {
  id: string;
  label: string;
  skills: SkillStatusEntry[];
};

function getSkillSourceGroups(): Array<{ id: string; label: string; sources: string[] }> {
  return [
    { id: "workspace", label: t("skillsWorkspace"), sources: ["openclaw-workspace"] },
    { id: "built-in", label: t("skillsBuiltIn"), sources: ["openclaw-bundled"] },
    { id: "installed", label: t("skillsInstalled"), sources: ["openclaw-managed"] },
    { id: "extra", label: t("skillsExtra"), sources: ["openclaw-extra"] },
  ];
}

function groupSkills(skills: SkillStatusEntry[]): SkillGroup[] {
  const sourceGroups = getSkillSourceGroups();
  const groups = new Map<string, SkillGroup>();
  for (const def of sourceGroups) {
    groups.set(def.id, { id: def.id, label: def.label, skills: [] });
  }
  const builtInGroup = sourceGroups.find((group) => group.id === "built-in");
  const other: SkillGroup = { id: "other", label: t("skillsOther"), skills: [] };
  for (const skill of skills) {
    const match = skill.bundled
      ? builtInGroup
      : sourceGroups.find((group) => group.sources.includes(skill.source));
    if (match) {
      groups.get(match.id)?.skills.push(skill);
    } else {
      other.skills.push(skill);
    }
  }
  const ordered = sourceGroups
    .map((group) => groups.get(group.id))
    .filter((group): group is SkillGroup => Boolean(group && group.skills.length > 0));
  if (other.skills.length > 0) {
    ordered.push(other);
  }
  return ordered;
}

export type SkillViewMode = "list" | "card";

export type SkillsProps = {
  loading: boolean;
  report: SkillStatusReport | null;
  error: string | null;
  filter: string;
  edits: Record<string, string>;
  busyKey: string | null;
  messages: SkillMessageMap;
  addModalOpen: boolean;
  uploadName: string;
  uploadFile: File | null;
  uploadError: string | null;
  uploadTemplate: string | null;
  uploadBusy: boolean;
  viewMode: SkillViewMode;
  onFilterChange: (next: string) => void;
  onRefresh: () => void;
  onViewModeChange: (mode: SkillViewMode) => void;
  onAddClick: () => void;
  onAddClose: () => void;
  onUploadNameChange: (next: string) => void;
  onUploadFileChange: (file: File | null) => void;
  onUploadSubmit: () => void;
  onToggle: (skillKey: string, enabled: boolean) => void;
  onEdit: (skillKey: string, value: string) => void;
  onSaveKey: (skillKey: string) => void;
  onInstall: (skillKey: string, name: string, installId: string) => void;
  onDelete: (skillKey: string) => void;
  selectedSkillKey: string | null;
  onSkillDetailClick: (skillKey: string | null) => void;
};

export function renderSkills(props: SkillsProps) {
  const skills = props.report?.skills ?? [];
  const filter = props.filter.trim().toLowerCase();
  const filtered = filter
    ? skills.filter((skill) =>
        [skill.name, skill.description, skill.source].join(" ").toLowerCase().includes(filter),
      )
    : skills;
  const groups = groupSkills(filtered);

  return html`
    <section class="card">
      <div class="row" style="justify-content: space-between; align-items: center;">
        <div>
          <div class="card-title">${t("skillsTitle")}</div>
          <div class="card-sub">${t("skillsSub")}</div>
        </div>
        <div class="row" style="gap: 8px; align-items: center;">
          <div class="row" style="gap: 4px;" title=${t("mcpViewList")}>
            <button
              type="button"
              class="btn ${props.viewMode === "list" ? "primary" : ""}"
              style="padding: 6px 10px;"
              @click=${() => props.onViewModeChange("list")}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="8" y1="6" x2="21" y2="6"/>
                <line x1="8" y1="12" x2="21" y2="12"/>
                <line x1="8" y1="18" x2="21" y2="18"/>
                <line x1="3" y1="6" x2="3.01" y2="6"/>
                <line x1="3" y1="12" x2="3.01" y2="12"/>
                <line x1="3" y1="18" x2="3.01" y2="18"/>
              </svg>
            </button>
            <button
              type="button"
              class="btn ${props.viewMode === "card" ? "primary" : ""}"
              style="padding: 6px 10px;"
              @click=${() => props.onViewModeChange("card")}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="3" width="7" height="7"/>
                <rect x="14" y="3" width="7" height="7"/>
                <rect x="3" y="14" width="7" height="7"/>
                <rect x="14" y="14" width="7" height="7"/>
              </svg>
            </button>
          </div>
          <button class="btn primary" ?disabled=${props.loading} @click=${props.onAddClick}>
            ${t("skillsAdd")}
          </button>
          <button class="btn" ?disabled=${props.loading} @click=${props.onRefresh}>
            ${props.loading ? t("commonLoading") : t("commonRefresh")}
          </button>
        </div>
      </div>

      ${
        props.addModalOpen
          ? html`
              <div class="modal-overlay" @click=${props.onAddClose}>
                <div class="modal card" @click=${(e: Event) => e.stopPropagation()}>
                  <div class="card-title">${t("skillsAddSkill")}</div>
                  <div class="field" style="margin-top: 12px;">
                    <span>${t("skillsUploadName")}</span>
                    <input
                      type="text"
                      .value=${props.uploadName}
                      @input=${(e: Event) =>
                        props.onUploadNameChange((e.target as HTMLInputElement).value)}
                      placeholder=${t("skillsUploadNamePlaceholder")}
                      pattern="[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}"
                    />
                  </div>
                  <div class="field" style="margin-top: 12px;">
                    <span>${t("skillsUploadFile")}</span>
                    <input
                      type="file"
                      accept=".md,.zip"
                      @change=${(e: Event) => {
                        const input = e.target as HTMLInputElement;
                        props.onUploadFileChange(input.files?.[0] ?? null);
                      }}
                    />
                    <div class="muted" style="margin-top: 4px; font-size: 0.9em;">
                      ${t("skillsUploadFileHint")}
                    </div>
                  </div>
                  ${
                    props.uploadError
                      ? html`
                          <div class="callout danger" style="margin-top: 12px;">
                            ${props.uploadError}
                          </div>
                        `
                      : nothing
                  }
                  ${
                    props.uploadTemplate
                      ? html`
                          <details class="muted" style="margin-top: 12px;">
                            <summary>Template</summary>
                            <pre
                              style="
                                margin-top: 8px;
                                padding: 12px;
                                background: var(--bg-muted, #f5f5f5);
                                border-radius: 6px;
                                overflow: auto;
                                max-height: 200px;
                                font-size: 0.85em;
                                white-space: pre-wrap;
                              "
                            >${props.uploadTemplate}</pre>
                          </details>
                        `
                      : nothing
                  }
                  <div class="row" style="margin-top: 16px; justify-content: flex-end; gap: 8px;">
                    <button class="btn" ?disabled=${props.uploadBusy} @click=${props.onAddClose}>
                      ${t("commonCancel")}
                    </button>
                    <button
                      class="btn primary"
                      ?disabled=${props.uploadBusy || !props.uploadName.trim() || !props.uploadFile}
                      @click=${props.onUploadSubmit}
                    >
                      ${props.uploadBusy ? t("commonLoading") : t("skillsUploadSubmit")}
                    </button>
                  </div>
                </div>
              </div>
            `
          : nothing
      }

      <div class="filters" style="margin-top: 14px;">
        <label class="field" style="flex: 1;">
          <span>${t("commonFilter")}</span>
          <input
            .value=${props.filter}
            @input=${(e: Event) => props.onFilterChange((e.target as HTMLInputElement).value)}
            placeholder=${t("skillsSearchPlaceholder")}
          />
        </label>
        <div class="muted">${filtered.length} ${t("skillsShown")}</div>
      </div>

      ${
        props.error
          ? html`<div class="callout danger" style="margin-top: 12px;">${props.error}</div>`
          : nothing
      }

      ${
        props.selectedSkillKey
          ? (() => {
              const skill = skills.find((s) => s.skillKey === props.selectedSkillKey);
              return skill
                ? renderSkillDetailModal(skill, () => props.onSkillDetailClick(null))
                : nothing;
            })()
          : nothing
      }

      ${
        filtered.length === 0
          ? html`
              <div class="muted" style="margin-top: 16px">No skills found.</div>
            `
          : html`
            <div class="agent-skills-groups" style="margin-top: 16px;">
              ${groups.map((group) => {
                const collapsedByDefault = group.id === "workspace" || group.id === "built-in";
                return html`
                  <details class="agent-skills-group" ?open=${!collapsedByDefault}>
                    <summary class="agent-skills-header">
                      <span>${group.label}</span>
                      <span class="muted">${group.skills.length}</span>
                    </summary>
                    ${
                      props.viewMode === "list"
                        ? html`
                            <div class="skills-table table" style="margin-top: 8px;">
                              <div class="skills-table-head table-head">
                                <div>${t("mcpTableName")}</div>
                                <div>${t("skillsSource")}</div>
                                <div>Status</div>
                                <div>${t("mcpTableActions")}</div>
                              </div>
                              ${group.skills.map((skill) => renderSkillListRow(skill, props))}
                            </div>
                          `
                        : html`
                            <div class="skills-card-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; margin-top: 8px;">
                              ${group.skills.map((skill) => renderSkillCard(skill, props))}
                            </div>
                          `
                    }
                  </details>
                `;
              })}
            </div>
          `
      }
    </section>
  `;
}

function renderSkillListRow(skill: SkillStatusEntry, props: SkillsProps) {
  const busy = props.busyKey === skill.skillKey;
  const canInstall = skill.install.length > 0 && skill.missing.bins.length > 0;
  const statusLabel = skill.disabled ? "disabled" : skill.eligible ? "eligible" : "blocked";
  return html`
    <div
      class="skills-table-row table-row ${props.selectedSkillKey === skill.skillKey ? "list-item-selected" : ""}"
      style="cursor: pointer;"
      @click=${() => props.onSkillDetailClick(skill.skillKey)}
    >
      <div class="skills-table-cell">
        <div style="font-weight: 500;">${skill.emoji ? `${skill.emoji} ` : ""}${skill.name}</div>
        <div class="muted" style="font-size: 12px; margin-top: 2px;">${clampText(skill.description, 120)}</div>
      </div>
      <div class="skills-table-cell muted" style="font-size: 13px;">
        ${skill.source}
      </div>
      <div class="skills-table-cell">
        <span class="chip ${skill.eligible && !skill.disabled ? "chip-ok" : "chip-warn"}" style="font-size: 12px;">
          ${statusLabel}
        </span>
      </div>
      <div class="skills-table-cell row" style="gap: 6px; justify-content: flex-start;" @click=${(e: Event) => e.stopPropagation()}>
        ${
          (skill.source === "openclaw-managed" || skill.source === "openclaw-extra") && props.onDelete
            ? html`
                <button
                  class="btn btn--sm"
                  style="color: var(--danger-color, #d14343);"
                  ?disabled=${busy}
                  @click=${(e: Event) => {
                    e.stopPropagation();
                    if (confirm(t("skillsDeleteConfirm"))) props.onDelete(skill.skillKey);
                  }}
                >
                  ${t("skillsDelete")}
                </button>
              `
            : nothing
        }
        <button
          class="btn btn--sm ${!skill.disabled ? "btn-ok" : ""}"
          ?disabled=${busy}
          @click=${(e: Event) => {
            e.stopPropagation();
            props.onToggle(skill.skillKey, skill.disabled);
          }}
        >
          ${skill.disabled ? "Enable" : "Disable"}
        </button>
        ${
          canInstall
            ? html`
                <button
                  class="btn btn--sm"
                  ?disabled=${busy}
                  @click=${(e: Event) => {
                    e.stopPropagation();
                    props.onInstall(skill.skillKey, skill.name, skill.install[0].id);
                  }}
                >
                  ${busy ? "Installing…" : skill.install[0].label}
                </button>
              `
            : nothing
        }
      </div>
    </div>
  `;
}

function renderSkillCard(skill: SkillStatusEntry, props: SkillsProps) {
  const busy = props.busyKey === skill.skillKey;
  const canInstall = skill.install.length > 0 && skill.missing.bins.length > 0;
  const statusLabel = skill.disabled ? "disabled" : skill.eligible ? "eligible" : "blocked";
  return html`
    <div
      class="skills-server-card ${props.selectedSkillKey === skill.skillKey ? "list-item-selected" : ""}"
      style="cursor: pointer;"
      @click=${() => props.onSkillDetailClick(skill.skillKey)}
    >
      <div class="skills-server-card__header">
        <div class="skills-server-card__icon">
          ${skill.emoji
            ? html`<span style="font-size: 20px;">${skill.emoji}</span>`
            : html`
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                  <path d="M2 17l10 5 10-5"/>
                </svg>
              `
          }
        </div>
        <div class="skills-server-card__title-row" style="min-width: 0;">
          <span class="skills-server-card__name">${skill.name}</span>
          <span class="chip ${skill.eligible && !skill.disabled ? "chip-ok" : "chip-warn"}" style="font-size: 11px;">${statusLabel}</span>
        </div>
      </div>
      <div class="skills-server-card__sub muted" style="font-size: 12px;">${clampText(skill.description, 80)}</div>
      <div class="skills-server-card__footer" @click=${(e: Event) => e.stopPropagation()}>
        ${
          (skill.source === "openclaw-managed" || skill.source === "openclaw-extra") && props.onDelete
            ? html`
                <button
                  class="btn btn--sm"
                  style="color: var(--danger-color, #d14343); padding: 4px 8px;"
                  ?disabled=${busy}
                  @click=${(e: Event) => {
                    e.stopPropagation();
                    if (confirm(t("skillsDeleteConfirm"))) props.onDelete(skill.skillKey);
                  }}
                  title=${t("skillsDelete")}
                >
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="3 6 5 6 21 6"/>
                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                    <line x1="10" y1="11" x2="10" y2="17"/>
                    <line x1="14" y1="11" x2="14" y2="17"/>
                  </svg>
                </button>
              `
            : nothing
        }
        <button
          class="btn btn--sm ${!skill.disabled ? "btn-ok" : ""}"
          ?disabled=${busy}
          @click=${(e: Event) => {
            e.stopPropagation();
            props.onToggle(skill.skillKey, skill.disabled);
          }}
        >
          ${skill.disabled ? "Enable" : "Disable"}
        </button>
        ${
          canInstall
            ? html`
                <button
                  class="btn btn--sm"
                  ?disabled=${busy}
                  @click=${(e: Event) => {
                    e.stopPropagation();
                    props.onInstall(skill.skillKey, skill.name, skill.install[0].id);
                  }}
                >
                  ${busy ? "Installing…" : skill.install[0].label}
                </button>
              `
            : nothing
        }
      </div>
    </div>
  `;
}

function renderSkill(skill: SkillStatusEntry, props: SkillsProps) {
  const busy = props.busyKey === skill.skillKey;
  const apiKey = props.edits[skill.skillKey] ?? "";
  const message = props.messages[skill.skillKey] ?? null;
  const canInstall = skill.install.length > 0 && skill.missing.bins.length > 0;
  const showBundledBadge = Boolean(skill.bundled && skill.source !== "openclaw-bundled");
  const missing = [
    ...skill.missing.bins.map((b) => `bin:${b}`),
    ...skill.missing.env.map((e) => `env:${e}`),
    ...skill.missing.config.map((c) => `config:${c}`),
    ...skill.missing.os.map((o) => `os:${o}`),
  ];
  const reasons: string[] = [];
  if (skill.disabled) {
    reasons.push("disabled");
  }
  if (skill.blockedByAllowlist) {
    reasons.push("blocked by allowlist");
  }
  return html`
    <div
      class="list-item"
      style="cursor: pointer;"
      @click=${() => props.onSkillDetailClick(skill.skillKey)}
    >
      <div class="list-main">
        <div class="list-title">
          ${skill.emoji ? `${skill.emoji} ` : ""}${skill.name}
        </div>
        <div class="list-sub">${clampText(skill.description, 140)}</div>
        <div class="chip-row" style="margin-top: 6px;">
          <span class="chip">${skill.source}</span>
          ${
            showBundledBadge
              ? html`
                  <span class="chip">bundled</span>
                `
              : nothing
          }
          <span class="chip ${skill.eligible ? "chip-ok" : "chip-warn"}">
            ${skill.eligible ? "eligible" : "blocked"}
          </span>
          ${
            skill.disabled
              ? html`
                  <span class="chip chip-warn">disabled</span>
                `
              : nothing
          }
        </div>
        ${
          missing.length > 0
            ? html`
              <div class="muted" style="margin-top: 6px;">
                Missing: ${missing.join(", ")}
              </div>
            `
            : nothing
        }
        ${
          reasons.length > 0
            ? html`
              <div class="muted" style="margin-top: 6px;">
                Reason: ${reasons.join(", ")}
              </div>
            `
            : nothing
        }
      </div>
      <div class="list-meta">
        <div class="row" style="justify-content: flex-end; flex-wrap: wrap; gap: 8px;">
        ${
          (skill.source === "openclaw-managed" || skill.source === "openclaw-extra") &&
          props.onDelete
            ? html`
                  <button
                    class="btn"
                    style="color: var(--danger-color, #d14343);"
                    ?disabled=${busy}
                    @click=${(e: Event) => {
                      e.stopPropagation();
                      if (confirm(t("skillsDeleteConfirm"))) {
                        props.onDelete(skill.skillKey);
                      }
                    }}
                  >
                    ${t("skillsDelete")}
                  </button>
                `
              : nothing
          }
          <button
            class="btn"
            ?disabled=${busy}
            @click=${(e: Event) => {
              e.stopPropagation();
              props.onToggle(skill.skillKey, skill.disabled);
            }}
          >
            ${skill.disabled ? "Enable" : "Disable"}
          </button>
          ${
            canInstall
              ? html`<button
                class="btn"
                ?disabled=${busy}
                @click=${(e: Event) => {
                  e.stopPropagation();
                  props.onInstall(skill.skillKey, skill.name, skill.install[0].id);
                }}
              >
                ${busy ? "Installing…" : skill.install[0].label}
              </button>`
              : nothing
          }
        </div>
        ${
          message
            ? html`<div
              class="muted"
              style="margin-top: 8px; color: ${
                message.kind === "error"
                  ? "var(--danger-color, #d14343)"
                  : "var(--success-color, #0a7f5a)"
              };"
            >
              ${message.message}
            </div>`
            : nothing
        }
        ${
          skill.primaryEnv
            ? html`
              <div class="field" style="margin-top: 10px;">
                <span>API key</span>
                <input
                  type="password"
                  .value=${apiKey}
                  @input=${(e: Event) => {
                    e.stopPropagation();
                    props.onEdit(skill.skillKey, (e.target as HTMLInputElement).value);
                  }}
                />
              </div>
              <button
                class="btn primary"
                style="margin-top: 8px;"
                ?disabled=${busy}
                @click=${(e: Event) => {
                  e.stopPropagation();
                  props.onSaveKey(skill.skillKey);
                }}
              >
                Save key
              </button>
            `
            : nothing
        }
      </div>
    </div>
  `;
}

function renderSkillDetailModal(skill: SkillStatusEntry, onClose: () => void) {
  const missing = [
    ...skill.missing.bins.map((b) => `bin:${b}`),
    ...skill.missing.env.map((e) => `env:${e}`),
    ...skill.missing.config.map((c) => `config:${c}`),
    ...skill.missing.os.map((o) => `os:${o}`),
  ];
  return html`
    <div class="modal-overlay" @click=${onClose}>
      <div class="modal card" style="max-width: 480px;" @click=${(e: Event) => e.stopPropagation()}>
        <div class="row" style="justify-content: space-between; align-items: center;">
          <div class="card-title">${skill.emoji ? `${skill.emoji} ` : ""}${skill.name}</div>
          <button class="btn" @click=${onClose}>×</button>
        </div>
        <div class="card-sub" style="margin-top: 4px;">${skill.description}</div>
        <div class="status-list" style="margin-top: 16px;">
          <div>
            <span class="label">${t("skillsSource")}</span>
            <span>${skill.source}</span>
          </div>
          <div>
            <span class="label">${t("skillsPath")}</span>
            <span class="monospace" style="font-size: 12px;">${skill.filePath}</span>
          </div>
          <div>
            <span class="label">${t("skillsEligible")}</span>
            <span>${skill.eligible ? t("commonYes") : t("commonNo")}</span>
          </div>
          <div>
            <span class="label">${t("skillsDisabled")}</span>
            <span>${skill.disabled ? t("commonYes") : t("commonNo")}</span>
          </div>
          ${
            skill.requirements.bins.length > 0
              ? html`
                  <div>
                    <span class="label">${t("skillsRequiresBins")}</span>
                    <span>${skill.requirements.bins.join(", ")}</span>
                  </div>
                `
              : nothing
          }
          ${
            skill.requirements.env.length > 0
              ? html`
                  <div>
                    <span class="label">${t("skillsRequiresEnv")}</span>
                    <span>${skill.requirements.env.join(", ")}</span>
                  </div>
                `
              : nothing
          }
          ${
            skill.requirements.config.length > 0
              ? html`
                  <div>
                    <span class="label">${t("skillsRequiresConfig")}</span>
                    <span>${skill.requirements.config.join(", ")}</span>
                  </div>
                `
              : nothing
          }
          ${
            missing.length > 0
              ? html`
                  <div>
                    <span class="label">${t("skillsMissing")}</span>
                    <span>${missing.join(", ")}</span>
                  </div>
                `
              : nothing
          }
        </div>
        ${
          skill.homepage
            ? html`
                <div style="margin-top: 12px;">
                  <a href=${skill.homepage} target="_blank" rel="noopener noreferrer">${skill.homepage}</a>
                </div>
              `
            : nothing
        }
      </div>
    </div>
  `;
}
