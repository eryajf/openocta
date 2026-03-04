import type { OpenClawApp } from "./app.ts";
import { loadConfig } from "./controllers/config.ts";
import { saveConfigPatch } from "./controllers/config.ts";
import { cloneConfigObject } from "./controllers/config/form-utils.ts";
import type { McpServerEntry } from "./views/mcp.ts";

export function handleMcpRefresh(host: OpenClawApp) {
  loadConfig(host);
}

export function handleMcpViewModeChange(host: OpenClawApp, mode: "list" | "card") {
  host.mcpViewMode = mode;
}

export function handleMcpAddServer(host: OpenClawApp) {
  host.mcpAddModalOpen = true;
  host.mcpAddName = "";
  host.mcpAddDraft = { enabled: true, command: "npx" };
  host.mcpAddConnectionType = "stdio";
  host.mcpAddEditMode = "form";
  host.mcpAddRawJson = JSON.stringify({ enabled: true }, null, 2);
  host.mcpAddRawError = null;
}

export function handleMcpAddClose(host: OpenClawApp) {
  host.mcpAddModalOpen = false;
  host.mcpAddName = "";
  host.mcpAddRawError = null;
}

export function handleMcpAddNameChange(host: OpenClawApp, name: string) {
  host.mcpAddName = name;
}

export function handleMcpAddFormPatch(host: OpenClawApp, patch: Partial<McpServerEntry>) {
  host.mcpAddDraft = { ...host.mcpAddDraft, ...patch };
}

export function handleMcpAddConnectionTypeChange(host: OpenClawApp, type: "stdio" | "url" | "service") {
  host.mcpAddConnectionType = type;
}

export function handleMcpAddRawChange(host: OpenClawApp, json: string) {
  host.mcpAddRawJson = json;
  try {
    const parsed = JSON.parse(json) as McpServerEntry;
    host.mcpAddDraft = parsed;
    host.mcpAddRawError = null;
  } catch {
    host.mcpAddRawError = "Invalid JSON";
  }
}

export function handleMcpAddEditModeChange(host: OpenClawApp, mode: "form" | "raw") {
  host.mcpAddEditMode = mode;
  if (mode === "raw") {
    host.mcpAddRawJson = JSON.stringify(host.mcpAddDraft, null, 2);
  }
}

export async function handleMcpAddSubmit(host: OpenClawApp) {
  const name = host.mcpAddName?.trim();
  if (!name) {
    return;
  }
  const key = name.toLowerCase().replace(/\s+/g, "-");
  if (host.mcpAddEditMode === "raw") {
    try {
      host.mcpAddDraft = JSON.parse(host.mcpAddRawJson) as McpServerEntry;
    } catch {
      host.mcpAddRawError = "Invalid JSON";
      return;
    }
  } else {
    const type = host.mcpAddConnectionType;
    const d = host.mcpAddDraft as McpServerEntry;
    if (type === "stdio" && !d.command?.trim()) return;
    if (type === "url" && !d.url?.trim()) return;
    if (type === "service" && (!d.service?.trim() || !d.serviceUrl?.trim())) return;
  }
  if (!host.configForm && host.configSnapshot?.config) {
    host.configForm = cloneConfigObject(host.configSnapshot.config as Record<string, unknown>);
  }
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.mcp) {
    base.mcp = { servers: {} };
  }
  const mcp = base.mcp as { servers?: Record<string, McpServerEntry> };
  if (!mcp.servers) {
    mcp.servers = {};
  }
  mcp.servers[key] = { ...host.mcpAddDraft, enabled: host.mcpAddDraft.enabled ?? true };
  host.configForm = base;
  host.configFormDirty = true;
  await saveConfigPatch(host, { mcp: base.mcp });
  host.mcpAddModalOpen = false;
  host.mcpAddName = "";
}

function inferConnectionType(entry: McpServerEntry | undefined): "stdio" | "url" | "service" {
  if (!entry) return "stdio";
  if (entry.command) return "stdio";
  if (entry.url) return "url";
  if (entry.service && entry.serviceUrl) return "service";
  return "stdio";
}

export function handleMcpSelect(host: OpenClawApp, key: string | null) {
  host.mcpSelectedKey = key;
  host.mcpRawError = null;
  if (key) {
    const servers = (host.configForm?.mcp as { servers?: Record<string, McpServerEntry> })?.servers ?? {};
    const entry = servers[key];
    host.mcpRawJson = entry ? JSON.stringify(entry, null, 2) : "{}";
    host.mcpEditConnectionType = inferConnectionType(entry);
  }
}

export function handleMcpEditConnectionTypeChange(host: OpenClawApp, type: "stdio" | "url" | "service") {
  host.mcpEditConnectionType = type;
}

export function handleMcpToggle(host: OpenClawApp, key: string, enabled: boolean) {
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.mcp) {
    base.mcp = { servers: {} };
  }
  const mcp = base.mcp as { servers?: Record<string, McpServerEntry> };
  if (!mcp.servers) {
    mcp.servers = {};
  }
  if (!mcp.servers[key]) {
    mcp.servers[key] = {};
  }
  mcp.servers[key] = { ...mcp.servers[key], enabled };
  host.configForm = base;
  host.configFormDirty = true;
  saveConfigPatch(host, { mcp: base.mcp });
}

export function handleMcpFormPatch(host: OpenClawApp, key: string, patch: Partial<McpServerEntry>) {
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.mcp) {
    base.mcp = { servers: {} };
  }
  const mcp = base.mcp as { servers?: Record<string, McpServerEntry> };
  if (!mcp.servers) {
    mcp.servers = {};
  }
  const current = mcp.servers[key] ?? {};
  mcp.servers[key] = { ...current, ...patch };
  host.configForm = base;
  host.configFormDirty = true;
  host.mcpFormDirty = true;
}

export function handleMcpRawChange(host: OpenClawApp, key: string, json: string) {
  host.mcpRawJson = json;
  try {
    const parsed = JSON.parse(json) as McpServerEntry;
    const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
    if (!base.mcp) {
      base.mcp = { servers: {} };
    }
    const mcp = base.mcp as { servers?: Record<string, McpServerEntry> };
    if (!mcp.servers) {
      mcp.servers = {};
    }
    mcp.servers[key] = parsed;
    host.configForm = base;
    host.configFormDirty = true;
    host.mcpRawError = null;
  } catch {
    host.mcpRawError = "Invalid JSON";
  }
}

export function handleMcpSave(host: OpenClawApp) {
  if (!host.mcpSelectedKey) {
    return;
  }
  if (host.mcpEditMode === "raw") {
    try {
      JSON.parse(host.mcpRawJson);
    } catch {
      host.mcpRawError = "Invalid JSON";
      return;
    }
  }
  const patch = { mcp: { servers: host.configForm?.mcp ? (host.configForm.mcp as { servers?: Record<string, McpServerEntry> }).servers : {} } };
  saveConfigPatch(host, patch);
  host.mcpFormDirty = false;
  host.mcpSelectedKey = null;
}

export function handleMcpCancel(host: OpenClawApp) {
  host.mcpSelectedKey = null;
  host.mcpRawError = null;
  if (host.mcpFormDirty) {
    loadConfig(host);
  }
}

export function handleMcpDelete(host: OpenClawApp, key: string) {
  const base = host.configForm ?? host.configSnapshot?.config;
  const mcp = base?.mcp as { servers?: Record<string, McpServerEntry> } | undefined;
  if (mcp?.servers && key in mcp.servers) {
    // Backend mergePatch deletes keys when patch value is null
    saveConfigPatch(host, { mcp: { servers: { [key]: null } } });
    // Update local form immediately so UI reflects deletion
    if (host.configForm && host.configForm.mcp && typeof host.configForm.mcp === "object") {
      const s = (host.configForm.mcp as { servers?: Record<string, McpServerEntry> }).servers;
      if (s && key in s) {
        const next = { ...s };
        delete next[key];
        (host.configForm.mcp as { servers: Record<string, McpServerEntry> }).servers = next;
      }
    }
  }
  if (host.mcpSelectedKey === key) {
    host.mcpSelectedKey = null;
  }
}
