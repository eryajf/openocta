import type { OpenClawApp } from "./app.ts";
import { loadConfig } from "./controllers/config.ts";
import { saveConfigPatch } from "./controllers/config.ts";
import { cloneConfigObject, setPathValue } from "./controllers/config/form-utils.ts";
import type { AddModelForm, AddProviderForm, ModelProvider } from "./views/models.ts";

export function handleModelsRefresh(host: OpenClawApp) {
  loadConfig(host);
}

export function handleModelsAddProvider(host: OpenClawApp) {
  host.modelsAddProviderModalOpen = true;
  host.modelsAddProviderForm = {
    providerId: "",
    displayName: "",
    baseUrl: "",
    apiKey: "",
    apiKeyPrefix: "",
  };
}

export function handleModelsAddProviderModalClose(host: OpenClawApp) {
  host.modelsAddProviderModalOpen = false;
}

export function handleModelsAddProviderFormChange(host: OpenClawApp, form: Partial<AddProviderForm>) {
  host.modelsAddProviderForm = { ...host.modelsAddProviderForm, ...form };
}

export function handleModelsAddProviderSubmit(host: OpenClawApp) {
  const { providerId, displayName, baseUrl, apiKey, apiKeyPrefix } = host.modelsAddProviderForm;
  if (!providerId.trim() || !displayName.trim()) return;
  const key = providerId.trim().toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9_-]/g, "");
  if (!key) return;
  if (!host.configForm && host.configSnapshot?.config) {
    host.configForm = cloneConfigObject(host.configSnapshot.config as Record<string, unknown>);
  }
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.models) {
    base.models = { mode: "merge", providers: {} };
  }
  const models = base.models as { mode?: string; providers?: Record<string, ModelProvider> };
  if (!models.providers) {
    models.providers = {};
  }
  if (models.providers[key]) {
    host.modelsAddProviderModalOpen = false;
    host.modelsSelectedProvider = key;
    return;
  }
  models.providers[key] = {
    displayName: displayName.trim(),
    baseUrl: baseUrl.trim() || undefined,
    apiKey: apiKey.trim() || undefined,
    apiKeyPrefix: apiKeyPrefix.trim() || undefined,
    api: "openai-completions",
  };
  host.configForm = base;
  host.configFormDirty = true;
  host.modelsFormDirty = true;
  host.modelsAddProviderModalOpen = false;
  host.modelsSelectedProvider = key;
}

export function handleModelsSelect(host: OpenClawApp, key: string | null) {
  host.modelsSelectedProvider = key;
}

export function handleModelsPatch(host: OpenClawApp, key: string, patch: Partial<ModelProvider>) {
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.models) {
    base.models = { mode: "merge", providers: {} };
  }
  const models = base.models as { mode?: string; providers?: Record<string, ModelProvider> };
  if (!models.providers) {
    models.providers = {};
  }
  const current = models.providers[key] ?? {};
  models.providers[key] = { ...current, ...patch };
  host.configForm = base;
  host.configFormDirty = true;
  host.modelsFormDirty = true;
}

export function handleModelsAddModel(host: OpenClawApp, providerKey: string) {
  host.modelsAddModelModalOpen = true;
  host.modelsAddModelForm = { modelId: "", modelName: "" };
}

export function handleModelsAddModelModalClose(host: OpenClawApp) {
  host.modelsAddModelModalOpen = false;
}

export function handleModelsAddModelFormChange(host: OpenClawApp, form: Partial<AddModelForm>) {
  host.modelsAddModelForm = { ...host.modelsAddModelForm, ...form };
}

export function handleModelsAddModelSubmit(host: OpenClawApp, providerKey: string) {
  const { modelId, modelName } = host.modelsAddModelForm;
  if (!modelId.trim() || !modelName.trim()) return;
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.models) {
    base.models = { mode: "merge", providers: {} };
  }
  const models = base.models as { mode?: string; providers?: Record<string, ModelProvider> };
  if (!models.providers) {
    models.providers = {};
  }
  const prov = models.providers[providerKey] ?? {};
  const existing = prov.models ?? [];
  if (existing.some((m) => m.id === modelId.trim())) {
    host.modelsAddModelModalOpen = false;
    return;
  }
  models.providers[providerKey] = {
    ...prov,
    models: [...existing, { id: modelId.trim(), name: modelName.trim() }],
  };
  host.configForm = base;
  host.configFormDirty = true;
  host.modelsFormDirty = true;
  host.modelsAddModelModalOpen = false;
}

export function handleModelsPatchModelEnv(host: OpenClawApp, providerKey: string, modelId: string, envVars: Record<string, string>) {
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.env) {
    base.env = { vars: {}, modelEnv: {} };
  }
  const env = base.env as { vars?: Record<string, string>; modelEnv?: Record<string, Record<string, string>> };
  if (!env.modelEnv) {
    env.modelEnv = {};
  }
  const modelRef = `${providerKey}/${modelId}`;
  // Keep __new__ in form state for UI add-row placeholder; filtered on save
  env.modelEnv[modelRef] = { ...envVars };
  host.configForm = base;
  host.configFormDirty = true;
  host.modelsFormDirty = true;
}

export function handleModelsRemoveModel(host: OpenClawApp, providerKey: string, modelId: string) {
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {});
  if (!base.models?.providers) return;
  const prov = base.models.providers[providerKey];
  if (!prov?.models) return;
  base.models.providers[providerKey] = {
    ...prov,
    models: prov.models.filter((m) => m.id !== modelId),
  };
  host.configForm = base;
  host.configFormDirty = true;
  host.modelsFormDirty = true;
}

function collectEnvVarsFromProviders(providers: Record<string, ModelProvider>): Record<string, string> {
  const collected: Record<string, string> = {};
  for (const prov of Object.values(providers)) {
    const ev = prov.envVars ?? {};
    for (const [k, v] of Object.entries(ev)) {
      if (!k || k === "__new__") continue;
      if (collected[k] !== undefined && collected[k] !== v) {
        return { __conflict: k };
      }
      collected[k] = v;
    }
  }
  return collected;
}

function sanitizeProviderForSave(prov: ModelProvider): ModelProvider {
  const ev = prov.envVars ?? {};
  const sanitized: Record<string, string> = {};
  for (const [k, v] of Object.entries(ev)) {
    if (k && k !== "__new__") sanitized[k] = v;
  }
  return { ...prov, envVars: Object.keys(sanitized).length ? sanitized : undefined };
}

export function handleModelsSave(host: OpenClawApp) {
  host.modelsSaveError = null;
  const providers = (host.configForm?.models as { providers?: Record<string, ModelProvider> })?.providers ?? {};
  const conflict = collectEnvVarsFromProviders(providers);
  if (conflict.__conflict) {
    host.modelsSaveError = conflict.__conflict;
    return;
  }
  const existingEnv = (host.configForm?.env as { vars?: Record<string, string> })?.vars ?? {};
  const mergedEnv = { ...existingEnv, ...conflict };
  const sanitizedProviders: Record<string, ModelProvider> = {};
  for (const [k, v] of Object.entries(providers)) {
    sanitizedProviders[k] = sanitizeProviderForSave(v);
  }
  const patch: Record<string, unknown> = {
    models: { ...host.configForm?.models, providers: sanitizedProviders },
  };
  const envForm = host.configForm?.env as { vars?: Record<string, string>; modelEnv?: Record<string, Record<string, string>> } | undefined;
  const modelEnv = envForm?.modelEnv ?? {};
  const sanitizedModelEnv: Record<string, Record<string, string> | null> = {};
  for (const [k, v] of Object.entries(modelEnv)) {
    if (!v || typeof v !== "object") continue;
    const sanitized: Record<string, string> = {};
    for (const [ek, ev] of Object.entries(v)) {
      if (ek && ek !== "__new__") sanitized[ek] = ev;
    }
    if (Object.keys(sanitized).length > 0) {
      sanitizedModelEnv[k] = sanitized;
    } else {
      sanitizedModelEnv[k] = null;
    }
  }
  patch.env = { vars: mergedEnv, modelEnv: sanitizedModelEnv };
  saveConfigPatch(host, patch);
  host.modelsFormDirty = false;
  host.modelsSelectedProvider = null;
}

export function handleModelsCancel(host: OpenClawApp) {
  host.modelsSelectedProvider = null;
  host.modelsSaveError = null;
  if (host.modelsFormDirty) {
    loadConfig(host);
  }
}

export function handleModelsUseModelClick(host: OpenClawApp, provider: string) {
  host.modelsUseModelModalOpen = true;
  host.modelsUseModelModalProvider = provider;
}

export function handleModelsUseModelModalClose(host: OpenClawApp) {
  host.modelsUseModelModalOpen = false;
  host.modelsUseModelModalProvider = null;
}

export function handleModelsUseModel(host: OpenClawApp, provider: string, modelId: string) {
  const modelRef = `${provider}/${modelId}`;
  const base = cloneConfigObject(host.configForm ?? host.configSnapshot?.config ?? {}) as Record<string, unknown>;
  setPathValue(base, ["agents", "defaults", "model", "primary"], modelRef);
  host.configForm = base;
  host.configFormDirty = true;
  saveConfigPatch(host, { agents: base.agents });
  host.modelsUseModelModalOpen = false;
  host.modelsUseModelModalProvider = null;
}
