import { html, nothing, type TemplateResult } from "lit";
import { t } from "../strings.js";

export type McpServerEntry = {
  enabled?: boolean | null;
  command?: string;
  args?: string[];
  env?: Record<string, string>;
  url?: string;
  service?: string;
  serviceUrl?: string;
  toolPrefix?: string;
};

export type McpViewMode = "list" | "card";

export type McpProps = {
  servers: Record<string, McpServerEntry>;
  loading: boolean;
  saving: boolean;
  selectedKey: string | null;
  viewMode: McpViewMode;
  addModalOpen: boolean;
  addName: string;
  addDraft: McpServerEntry;
  addConnectionType: "stdio" | "url" | "service";
  addEditMode: "form" | "raw";
  addFormDirty: boolean;
  addRawJson: string;
  addRawError: string | null;
  editMode: "form" | "raw";
  editConnectionType: "stdio" | "url" | "service";
  formDirty: boolean;
  rawJson: string;
  rawError: string | null;
  onRefresh: () => void;
  onViewModeChange: (mode: McpViewMode) => void;
  onAddServer: () => void;
  onAddClose: () => void;
  onAddNameChange: (name: string) => void;
  onAddFormPatch: (patch: Partial<McpServerEntry>) => void;
  onAddRawChange: (json: string) => void;
  onAddConnectionTypeChange: (type: "stdio" | "url" | "service") => void;
  onAddEditModeChange: (mode: "form" | "raw") => void;
  onAddSubmit: () => void;
  onSelect: (key: string | null) => void;
  onToggle: (key: string, enabled: boolean) => void;
  onFormPatch: (key: string, patch: Partial<McpServerEntry>) => void;
  onRawChange: (key: string, json: string) => void;
  onEditModeChange: (mode: "form" | "raw") => void;
  onEditConnectionTypeChange: (type: "stdio" | "url" | "service") => void;
  onSave: () => void;
  onCancel: () => void;
  onDelete: (key: string) => void;
};

function resolveServerLabel(key: string): string {
  const labels: Record<string, string> = {
    prometheus: "Prometheus",
    elasticsearch: "Elasticsearch",
    filesystem: "Filesystem",
  };
  return labels[key.toLowerCase()] ?? key.charAt(0).toUpperCase() + key.slice(1);
}

function getConnectionType(entry: McpServerEntry): string {
  if (entry.command) {
    return "stdio";
  }
  if (entry.url) {
    return "url";
  }
  if (entry.service && entry.serviceUrl) {
    return "service";
  }
  return "—";
}

function formatEnvForEdit(env: Record<string, string> | undefined): string {
  if (!env || typeof env !== "object") return "";
  return Object.entries(env)
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
}

function isAddFormValid(props: McpProps): boolean {
  if (props.addEditMode === "raw") return !props.addRawError;
  const d = props.addDraft;
  const t = props.addConnectionType ?? "stdio";
  if (t === "stdio") return !!d?.command?.trim();
  if (t === "url") return !!d?.url?.trim();
  if (t === "service") return !!d?.service?.trim() && !!d?.serviceUrl?.trim();
  return false;
}

function parseEnvFromEdit(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of text.split(/\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const eq = trimmed.indexOf("=");
    if (eq > 0) {
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (key) out[key] = val;
    }
  }
  return out;
}

function renderAddConnectionTypeFields(
  type: "stdio" | "url" | "service",
  draft: McpServerEntry | undefined,
  onPatch: (p: Partial<McpServerEntry>) => void,
): TemplateResult {
  const MCP_COMMAND_OPTIONS = ["npx", "docker", "uv"] as const;
  if (type === "stdio") {
    const cmd = draft?.command ?? "npx";
    const sel = MCP_COMMAND_OPTIONS.includes(cmd as (typeof MCP_COMMAND_OPTIONS)[number]) ? cmd : "npx";
    return html`
      <div class="field">
        <span>${t("mcpCommand")} *</span>
        <select
          .value=${sel}
          @change=${(e: Event) => onPatch({ command: (e.target as HTMLSelectElement).value })}
        >
          ${MCP_COMMAND_OPTIONS.map((c) => html`<option value=${c}>${c}</option>`)}
        </select>
      <div class="field">
        <span>${t("mcpArgs")}</span>
        <input
          type="text"
          .value={(draft?.args ?? []).join(" ")}
          placeholder="-y prometheus-mcp-server"
          @input=${(e: Event) => {
            const val = (e.target as HTMLInputElement).value;
            onPatch({ args: val.trim() ? val.trim().split(/\s+/) : [] });
          }}
        />
      </div>
      <div class="field">
        <span>${t("mcpEnv")}</span>
        <textarea
          style="min-height: 80px; font-family: var(--mono); font-size: 12px;"
          placeholder=${t("mcpEnvPlaceholder")}
          .value=${formatEnvForEdit(draft?.env)}
          @input=${(e: Event) => {
            const val = (e.target as HTMLTextAreaElement).value;
            onPatch({ env: parseEnvFromEdit(val) });
          }}
        ></textarea>
      </div>
    `;
  }
  if (type === "url") {
    return html`
      <div class="field">
        <span>${t("mcpUrl")} *</span>
        <input
          type="text"
          .value=${draft?.url ?? ""}
          placeholder="https://mcp.example.com/sse"
          @input=${(e: Event) => onPatch({ url: (e.target as HTMLInputElement).value })}
        />
      </div>
    `;
  }
  return html`
    <div class="field">
      <span>${t("mcpService")} *</span>
      <input
        type="text"
        .value=${draft?.service ?? ""}
        placeholder="prometheus"
        @input=${(e: Event) => onPatch({ service: (e.target as HTMLInputElement).value })}
      />
    </div>
    <div class="field">
      <span>${t("mcpServiceUrl")} *</span>
      <input
        type="text"
        .value=${draft?.serviceUrl ?? ""}
        placeholder="http://localhost:9090"
        @input=${(e: Event) => onPatch({ serviceUrl: (e.target as HTMLInputElement).value })}
      />
    </div>
  `;
}

function renderEditConnectionTypeFields(
  type: "stdio" | "url" | "service",
  selected: McpServerEntry,
  selectedKey: string,
  onFormPatch: (key: string, p: Partial<McpServerEntry>) => void,
): TemplateResult {
  const MCP_COMMAND_OPTIONS_EDIT = ["npx", "docker", "uv"] as const;
  if (type === "stdio") {
    const cmd = selected.command ?? "npx";
    const sel = MCP_COMMAND_OPTIONS_EDIT.includes(cmd as (typeof MCP_COMMAND_OPTIONS_EDIT)[number]) ? cmd : "npx";
    return html`
      <div class="field">
        <span>${t("mcpCommand")} *</span>
        <select
          .value=${sel}
          @change=${(e: Event) =>
            onFormPatch(selectedKey, { command: (e.target as HTMLSelectElement).value })}
        >
          ${MCP_COMMAND_OPTIONS_EDIT.map((c) => html`<option value=${c}>${c}</option>`)}
        </select>
      <div class="field">
        <span>${t("mcpArgs")}</span>
        <input
          type="text"
          .value=${(selected.args ?? []).join(" ")}
          placeholder="-y prometheus-mcp-server"
          @input=${(e: Event) => {
            const val = (e.target as HTMLInputElement).value;
            onFormPatch(selectedKey, { args: val.trim() ? val.trim().split(/\s+/) : [] });
          }}
        />
      </div>
      <div class="field">
        <span>${t("mcpEnv")}</span>
        <textarea
          style="min-height: 80px; font-family: var(--mono); font-size: 12px;"
          placeholder=${t("mcpEnvPlaceholder")}
          .value=${formatEnvForEdit(selected.env)}
          @input=${(e: Event) => {
            const val = (e.target as HTMLTextAreaElement).value;
            onFormPatch(selectedKey, { env: parseEnvFromEdit(val) });
          }}
        ></textarea>
      </div>
    `;
  }
  if (type === "url") {
    return html`
      <div class="field">
        <span>${t("mcpUrl")} *</span>
        <input
          type="text"
          .value=${selected.url ?? ""}
          placeholder="https://mcp.example.com/sse"
          @input=${(e: Event) =>
            onFormPatch(selectedKey, { url: (e.target as HTMLInputElement).value })}
        />
      </div>
    `;
  }
  return html`
    <div class="field">
      <span>${t("mcpService")} *</span>
      <input
        type="text"
        .value=${selected.service ?? ""}
        placeholder="prometheus"
        @input=${(e: Event) =>
          onFormPatch(selectedKey, { service: (e.target as HTMLInputElement).value })}
      />
    </div>
    <div class="field">
      <span>${t("mcpServiceUrl")} *</span>
      <input
        type="text"
        .value=${selected.serviceUrl ?? ""}
        placeholder="http://localhost:9090"
        @input=${(e: Event) =>
          onFormPatch(selectedKey, { serviceUrl: (e.target as HTMLInputElement).value })}
      />
    </div>
  `;
}

export function renderMcp(props: McpProps) {
  const entries = Object.entries(props.servers ?? {});
  const selected = props.selectedKey ? props.servers[props.selectedKey] : null;

  return html`
    <section class="card">
      <div class="row" style="justify-content: space-between; align-items: center;">
        <div>
          <div class="card-title">${t("navTitleMcp")}</div>
          <div class="card-sub">${t("subtitleMcp")}</div>
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
          <button class="btn primary" ?disabled=${props.loading} @click=${props.onAddServer}>
            ${t("mcpAddServer")}
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
                <div class="modal card" style="max-width: 520px;" @click=${(e: Event) => e.stopPropagation()}>
                  <div class="card-title">${t("mcpAddServer")}</div>
                  <div class="field" style="margin-top: 12px;">
                    <span>${t("mcpServerName")} *</span>
                    <input
                      type="text"
                      .value=${props.addName}
                      @input=${(e: Event) => props.onAddNameChange((e.target as HTMLInputElement).value)}
                      placeholder="prometheus, my-mcp"
                    />
                  </div>
                  <div class="row" style="margin: 12px 0; gap: 8px;">
                    <button
                      class="btn ${props.addEditMode === "form" ? "primary" : ""}"
                      @click=${() => props.onAddEditModeChange("form")}
                    >
                      ${t("mcpFormMode")}
                    </button>
                    <button
                      class="btn ${props.addEditMode === "raw" ? "primary" : ""}"
                      @click=${() => props.onAddEditModeChange("raw")}
                    >
                      ${t("mcpRawMode")}
                    </button>
                  </div>
                  ${
                    props.addEditMode === "form"
                      ? html`
                          <div class="config-form" id="mcp-add-form">
                            <div class="mcp-connection-tabs" style="display: flex; gap: 4px; margin-bottom: 16px; border-bottom: 1px solid var(--input, #333); padding-bottom: 4px;">
                              <button
                                type="button"
                                class="btn ${(props.addConnectionType ?? "stdio") === "stdio" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onAddConnectionTypeChange("stdio")}
                              >
                                ${t("mcpConnectionTypeStdio")}
                              </button>
                              <button
                                type="button"
                                class="btn ${(props.addConnectionType ?? "stdio") === "url" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onAddConnectionTypeChange("url")}
                              >
                                ${t("mcpConnectionTypeUrl")}
                              </button>
                              <button
                                type="button"
                                class="btn ${(props.addConnectionType ?? "stdio") === "service" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onAddConnectionTypeChange("service")}
                              >
                                ${t("mcpConnectionTypeService")}
                              </button>
                            </div>
                            <div class="mcp-connection-fields" style="margin-bottom: 12px;">
                              ${renderAddConnectionTypeFields(
                                props.addConnectionType === "stdio" || props.addConnectionType === "url" || props.addConnectionType === "service"
                                  ? props.addConnectionType
                                  : "stdio",
                                props.addDraft,
                                (p) => props.onAddFormPatch(p),
                              )}
                            </div>
                            <div class="field">
                              <span>${t("mcpToolPrefix")}</span>
                              <input
                                type="text"
                                .value=${props.addDraft?.toolPrefix ?? ""}
                                placeholder="Optional"
                                @input=${(e: Event) =>
                                  props.onAddFormPatch({ toolPrefix: (e.target as HTMLInputElement).value })}
                              />
                            </div>
                          </div>
                        `
                      : html`
                          <div class="field">
                            <span>${t("mcpRawJson")}</span>
                            <textarea
                              style="min-height: 180px; font-family: var(--mono);"
                              .value=${props.addRawJson}
                              @input=${(e: Event) =>
                                props.onAddRawChange((e.target as HTMLTextAreaElement).value)}
                            ></textarea>
                            ${
                              props.addRawError
                                ? html`<div class="callout danger" style="margin-top: 8px;">${props.addRawError}</div>`
                                : nothing
                            }
                          </div>
                        `
                  }
                  <div class="row" style="margin-top: 16px; gap: 8px; justify-content: flex-end;">
                    <button class="btn" @click=${props.onAddClose}>${t("commonCancel")}</button>
                    <button
                      class="btn primary"
                      ?disabled=${props.saving || !props.addName.trim() || !isAddFormValid(props)}
                      @click=${props.onAddSubmit}
                    >
                      ${props.saving ? t("commonSaving") : t("mcpAddServer")}
                    </button>
                  </div>
                </div>
              </div>
            `
          : nothing
      }

      <div class="mcp-server-list" style="margin-top: 16px;">
        ${
          entries.length === 0
            ? html`
                <div class="muted" style="margin-top: 12px;">
                  ${t("mcpNoServers")}
                </div>
              `
            : props.viewMode === "list"
              ? html`
                  <div class="mcp-table table" style="margin-top: 0;">
                    <div class="mcp-table-head table-head">
                      <div>${t("mcpTableName")}</div>
                      <div>${t("mcpTableType")}</div>
                      <div>${t("mcpTableStatus")}</div>
                      <div>${t("mcpTableActions")}</div>
                    </div>
                    ${entries.map(
                      ([key, entry]) => html`
                        <div
                          class="mcp-table-row table-row ${props.selectedKey === key ? "list-item-selected" : ""}"
                          style="cursor: pointer;"
                          @click=${() => props.onSelect(props.selectedKey === key ? null : key)}
                        >
                          <div class="mcp-table-cell" style="font-weight: 500;">
                            ${resolveServerLabel(key)}
                          </div>
                          <div class="mcp-table-cell muted" style="font-size: 13px;">
                            ${getConnectionType(entry)}
                          </div>
                          <div class="mcp-table-cell">
                            <span class="chip ${entry.enabled !== false ? "chip-ok" : ""}" style="font-size: 12px;">
                              ${entry.enabled !== false ? t("mcpEnabled") : t("mcpDisabled")}
                            </span>
                          </div>
                          <div class="mcp-table-cell row" style="gap: 6px; justify-content: flex-start;" @click=${(e: Event) => e.stopPropagation()}>
                            <button
                              class="btn btn--sm ${entry.enabled !== false ? "btn-ok" : ""}"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                props.onToggle(key, entry.enabled === false);
                              }}
                            >
                              ${entry.enabled !== false ? t("mcpEnabled") : t("mcpDisabled")}
                            </button>
                            <button
                              class="btn btn--sm"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                props.onSelect(props.selectedKey === key ? null : key);
                              }}
                            >
                              ${t("channelsConfigure")}
                            </button>
                            <button
                              class="btn btn--sm"
                              style="color: var(--danger-color, #d14343);"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                if (confirm(t("mcpDeleteConfirm"))) {
                                  props.onDelete(key);
                                }
                              }}
                            >
                              ${t("commonDelete")}
                            </button>
                          </div>
                        </div>
                      `,
                    )}
                  </div>
                `
              : html`
                  <div class="mcp-card-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px;">
                    ${entries.map(
                      ([key, entry]) => html`
                        <div
                          class="mcp-server-card ${props.selectedKey === key ? "list-item-selected" : ""}"
                          style="cursor: pointer;"
                          @click=${() => props.onSelect(props.selectedKey === key ? null : key)}
                        >
                          <div class="mcp-server-card__header">
                            <div class="mcp-server-card__icon">
                              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="4" y="4" width="8" height="8" rx="1"/>
                                <rect x="12" y="12" width="8" height="8" rx="1"/>
                              </svg>
                            </div>
                            <div class="mcp-server-card__title-row" style="min-width: 0;">
                              <span class="mcp-server-card__name">${resolveServerLabel(key)}</span>
                              <span class="chip" style="font-size: 11px;">${getConnectionType(entry)}</span>
                            </div>
                          </div>
                          <div class="mcp-server-card__footer" @click=${(e: Event) => e.stopPropagation()}>
                            <button
                              class="btn btn--sm ${entry.enabled !== false ? "btn-ok" : ""}"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                props.onToggle(key, entry.enabled === false);
                              }}
                            >
                              ${entry.enabled !== false ? t("mcpEnabled") : t("mcpDisabled")}
                            </button>
                            <button
                              class="btn btn--sm"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                props.onSelect(props.selectedKey === key ? null : key);
                              }}
                            >
                              ${t("channelsConfigure")}
                            </button>
                            <button
                              class="btn btn--sm"
                              style="color: var(--danger-color, #d14343); padding: 4px 8px;"
                              ?disabled=${props.saving}
                              @click=${(e: Event) => {
                                e.stopPropagation();
                                if (confirm(t("mcpDeleteConfirm"))) {
                                  props.onDelete(key);
                                }
                              }}
                              title=${t("commonDelete")}
                            >
                              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="3 6 5 6 21 6"/>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                                <line x1="10" y1="11" x2="10" y2="17"/>
                                <line x1="14" y1="11" x2="14" y2="17"/>
                              </svg>
                            </button>
                          </div>
                        </div>
                      `,
                    )}
                  </div>
                `
        }
      </div>
    </section>

    ${
      props.selectedKey && selected
        ? html`
            <div class="channel-panel-overlay" @click=${(e: Event) => {
              if ((e.target as HTMLElement).classList.contains("channel-panel-overlay")) {
                props.onCancel();
              }
            }}>
              <div class="channel-panel card" @click=${(e: Event) => e.stopPropagation()}>
                <div class="channel-panel-header row" style="justify-content: space-between; align-items: center;">
                  <div class="card-title">${resolveServerLabel(props.selectedKey)} ${t("configSettingsTitle")}</div>
                  <button class="btn" @click=${props.onCancel}>×</button>
                </div>
                <div class="channel-panel-content">
                  <div class="row" style="margin-bottom: 12px; gap: 8px;">
                    <button
                      class="btn ${props.editMode === "form" ? "primary" : ""}"
                      @click=${() => props.onEditModeChange("form")}
                    >
                      ${t("mcpFormMode")}
                    </button>
                    <button
                      class="btn ${props.editMode === "raw" ? "primary" : ""}"
                      @click=${() => {
                        props.onEditModeChange("raw");
                        props.onRawChange(props.selectedKey!, JSON.stringify(selected, null, 2));
                      }}
                    >
                      ${t("mcpRawMode")}
                    </button>
                  </div>

                  ${
                    props.editMode === "form"
                      ? html`
                          <div class="config-form">
                            <div class="field">
                              <span>${t("mcpEnabled")}</span>
                              <div class="row" style="align-items: center; gap: 8px;">
                                <input
                                  type="checkbox"
                                  ?checked=${selected.enabled !== false}
                                  @change=${(e: Event) =>
                                    props.onFormPatch(props.selectedKey!, {
                                      enabled: (e.target as HTMLInputElement).checked,
                                    })}
                                />
                              </div>
                            </div>
                            <div class="mcp-connection-tabs" style="display: flex; gap: 4px; margin-bottom: 16px; border-bottom: 1px solid var(--input, #333); padding-bottom: 4px;">
                              <button
                                type="button"
                                class="btn ${(props.editConnectionType ?? "stdio") === "stdio" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onEditConnectionTypeChange("stdio")}
                              >
                                ${t("mcpConnectionTypeStdio")}
                              </button>
                              <button
                                type="button"
                                class="btn ${(props.editConnectionType ?? "stdio") === "url" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onEditConnectionTypeChange("url")}
                              >
                                ${t("mcpConnectionTypeUrl")}
                              </button>
                              <button
                                type="button"
                                class="btn ${(props.editConnectionType ?? "stdio") === "service" ? "primary" : ""}"
                                style="flex: 1; min-width: 0;"
                                @click=${() => props.onEditConnectionTypeChange("service")}
                              >
                                ${t("mcpConnectionTypeService")}
                              </button>
                            </div>
                            <div class="mcp-connection-fields" style="margin-bottom: 12px;">
                              ${renderEditConnectionTypeFields(
                                props.editConnectionType === "stdio" || props.editConnectionType === "url" || props.editConnectionType === "service"
                                  ? props.editConnectionType
                                  : "stdio",
                                selected,
                                props.selectedKey!,
                                props.onFormPatch,
                              )}
                            </div>
                            <div class="field">
                              <span>${t("mcpToolPrefix")}</span>
                              <input
                                type="text"
                                .value=${selected.toolPrefix ?? ""}
                                placeholder="Optional"
                                @input=${(e: Event) =>
                                  props.onFormPatch(props.selectedKey!, {
                                    toolPrefix: (e.target as HTMLInputElement).value,
                                  })}
                              />
                            </div>
                          </div>
                        `
                      : html`
                          <div class="field">
                            <span>${t("mcpRawJson")}</span>
                            <textarea
                              style="min-height: 200px; font-family: var(--mono);"
                              .value=${props.rawJson}
                              @input=${(e: Event) =>
                                props.onRawChange(props.selectedKey!, (e.target as HTMLTextAreaElement).value)}
                            ></textarea>
                            ${
                              props.rawError
                                ? html`<div class="callout danger" style="margin-top: 8px;">${props.rawError}</div>`
                                : nothing
                            }
                          </div>
                        `
                  }

                  <div class="row" style="margin-top: 16px; gap: 8px;">
                    <button
                      class="btn primary"
                      ?disabled=${props.saving || (!props.formDirty && props.editMode === "form")}
                      @click=${props.onSave}
                    >
                      ${props.saving ? t("commonSaving") : t("commonSave")}
                    </button>
                    <button class="btn" ?disabled=${props.saving} @click=${props.onCancel}>
                      ×
                    </button>
                  </div>
                </div>
              </div>
            </div>
          `
        : nothing
    }
  `;
}
