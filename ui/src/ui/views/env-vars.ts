import { html, nothing } from "lit";
import { t } from "../strings.js";

export type ShellEnvConfig = {
  enabled?: boolean;
  timeoutMs?: number;
};

export type EnvVarsProps = {
  vars: Record<string, string>;
  modelEnv: Record<string, Record<string, string>>;
  shellEnv: ShellEnvConfig | null;
  dirty: boolean;
  loading: boolean;
  saving: boolean;
  connected: boolean;
  onVarsChange: (vars: Record<string, string>) => void;
  onModelEnvChange: (modelEnv: Record<string, Record<string, string>>) => void;
  onShellEnvChange: (shellEnv: ShellEnvConfig | null) => void;
  onSave: () => void;
  onReload: () => void;
};

function renderVarsTable(
  vars: Record<string, string>,
  connected: boolean,
  onVarsChange: (vars: Record<string, string>) => void,
  onAdd: () => void,
) {
  const entries = Object.entries(vars);
  const handleKeyChange = (index: number, newKey: string) => {
    const keys = Object.keys(vars);
    const values = Object.values(vars);
    keys[index] = newKey;
    const next: Record<string, string> = {};
    keys.forEach((k, i) => {
      next[k] = values[i] ?? "";
    });
    onVarsChange(next);
  };
  const handleValueChange = (index: number, newValue: string) => {
    const keys = Object.keys(vars);
    const values = [...Object.values(vars)];
    values[index] = newValue;
    const next: Record<string, string> = {};
    keys.forEach((k, i) => {
      next[k] = values[i] ?? "";
    });
    onVarsChange(next);
  };
  const handleDelete = (index: number) => {
    const keys = Object.keys(vars).filter((_, i) => i !== index);
    const values = Object.values(vars).filter((_, i) => i !== index);
    const next: Record<string, string> = {};
    keys.forEach((k, i) => {
      next[k] = values[i] ?? "";
    });
    onVarsChange(next);
  };

  return html`
    ${entries.length === 0
      ? html`
          <p class="env-vars__empty">${t("envVarsEmpty")}</p>
          <button class="btn btn--secondary" ?disabled=${!connected} @click=${onAdd}>
            ${t("envVarsAdd")}
          </button>
        `
      : html`
          <table class="env-vars__table">
            <thead>
              <tr>
                <th>${t("envVarsKey")}</th>
                <th>${t("envVarsValue")}</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${entries.map(
                ([key, value], index) => html`
                  <tr>
                    <td>
                      <input
                        class="env-vars__input"
                        type="text"
                        .value=${key}
                        placeholder=${t("envVarsKeyPlaceholder")}
                        ?disabled=${!connected}
                        @input=${(e: Event) => {
                          const target = e.target as HTMLInputElement;
                          handleKeyChange(index, target.value);
                        }}
                      />
                    </td>
                    <td>
                      <input
                        class="env-vars__input"
                        type="text"
                        .value=${value}
                        placeholder=${t("envVarsValuePlaceholder")}
                        ?disabled=${!connected}
                        @input=${(e: Event) => {
                          const target = e.target as HTMLInputElement;
                          handleValueChange(index, target.value);
                        }}
                      />
                    </td>
                    <td>
                      <button
                        class="btn btn--ghost env-vars__delete"
                        ?disabled=${!connected}
                        @click=${() => handleDelete(index)}
                        title=${t("envVarsDelete")}
                      >
                        ${t("envVarsDelete")}
                      </button>
                    </td>
                  </tr>
                `,
              )}
            </tbody>
          </table>
          <button class="btn btn--secondary env-vars__add" ?disabled=${!connected} @click=${onAdd}>
            ${t("envVarsAdd")}
          </button>
        `}
  `;
}

export function renderEnvVars(props: EnvVarsProps) {
  const { vars, modelEnv, shellEnv, dirty, loading, saving, connected, onVarsChange, onModelEnvChange, onShellEnvChange, onSave, onReload } = props;
  const canSave = connected && dirty && !saving && !loading;

  const handleVarsAdd = () => {
    onVarsChange({ ...vars, "": "" });
  };

  const modelEnvEntries = Object.entries(modelEnv ?? {});

  return html`
    <div class="env-vars">
      <div class="env-vars__toolbar">
        <button
          class="btn btn--secondary"
          ?disabled=${loading || !connected}
          @click=${onReload}
          title=${t("overviewRefresh")}
        >
          ${loading ? "…" : t("overviewRefresh")}
        </button>
        <button
          class="btn btn--primary"
          ?disabled=${!canSave}
          @click=${onSave}
          title=${t("envVarsSave")}
        >
          ${saving ? "…" : t("envVarsSave")}
        </button>
      </div>
      ${dirty ? html`<p class="env-vars__dirty">${t("configUnsavedChanges")}</p>` : nothing}

      <div class="env-vars__sections">
        <section class="env-vars__section card" style="margin-bottom: 16px;">
          <h3 class="card-title" style="margin-bottom: 8px;">${t("envVarsSection")}</h3>
          <p class="muted" style="font-size: 12px; margin-bottom: 12px;">${t("configEnvVarsDesc")}</p>
          <div class="env-vars__list">
            ${renderVarsTable(vars, connected, onVarsChange, handleVarsAdd)}
          </div>
        </section>

        <section class="env-vars__section card" style="margin-bottom: 16px;">
          <h3 class="card-title" style="margin-bottom: 8px;">${t("envModelEnvSection")}</h3>
          <p class="muted" style="font-size: 12px; margin-bottom: 12px;">${t("envVarsKeyPlaceholder")} = ${t("envVarsValuePlaceholder")}，按 provider/modelId 分组</p>
          <div class="env-vars__list">
            ${modelEnvEntries.length === 0
              ? html`<p class="env-vars__empty">${t("envVarsEmpty")}</p>`
              : html`
                  ${modelEnvEntries.map(
                    ([modelRef, envVars]) => html`
                      <div style="margin-bottom: 16px; padding: 12px; border: 1px solid var(--border-color, #eee); border-radius: 8px;">
                        <div style="font-weight: 500; margin-bottom: 8px;"><code>${modelRef}</code></div>
                        ${renderVarsTable(
                          envVars ?? {},
                          connected,
                          (next) => {
                            const updated = { ...modelEnv };
                            updated[modelRef] = next;
                            onModelEnvChange(updated);
                          },
                          () => {
                            const updated = { ...modelEnv };
                            updated[modelRef] = { ...(updated[modelRef] ?? {}), "": "" };
                            onModelEnvChange(updated);
                          },
                        )}
                        <button
                          class="btn btn--ghost btn--sm"
                          style="margin-top: 8px;"
                          ?disabled=${!connected}
                          @click=${() => {
                            const updated = { ...modelEnv };
                            delete updated[modelRef];
                            onModelEnvChange(updated);
                          }}
                        >
                          ${t("commonDelete")} ${modelRef}
                        </button>
                      </div>
                    `,
                  )}
                  <button
                    class="btn btn--secondary"
                    ?disabled=${!connected}
                    @click=${() => {
                      const modelRef = prompt("provider/modelId (e.g. ollama/llama3.3):")?.trim();
                      if (modelRef) {
                        onModelEnvChange({ ...modelEnv, [modelRef]: {} });
                      }
                    }}
                  >
                    + ${t("envVarsAdd")} modelEnv
                  </button>
                `}
          </div>
        </section>

        <section class="env-vars__section card" style="margin-bottom: 16px;">
          <h3 class="card-title" style="margin-bottom: 8px;">${t("envShellEnvSection")}</h3>
          <p class="muted" style="font-size: 12px; margin-bottom: 12px;">Shell 环境导入配置</p>
          <div class="config-form">
            <div class="field">
              <span>Enabled</span>
              <input
                type="checkbox"
                ?checked=${shellEnv?.enabled ?? false}
                ?disabled=${!connected}
                @change=${(e: Event) => {
                  const checked = (e.target as HTMLInputElement).checked;
                  onShellEnvChange({ ...(shellEnv ?? {}), enabled: checked });
                }}
              />
            </div>
            <div class="field">
              <span>Timeout (ms)</span>
              <input
                type="number"
                .value=${String(shellEnv?.timeoutMs ?? "")}
                placeholder="e.g. 5000"
                ?disabled=${!connected}
                @input=${(e: Event) => {
                  const v = (e.target as HTMLInputElement).value;
                  const n = v ? parseInt(v, 10) : undefined;
                  onShellEnvChange({ ...(shellEnv ?? {}), timeoutMs: n });
                }}
              />
            </div>
          </div>
        </section>
      </div>
    </div>
  `;
}
