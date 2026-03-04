import { html, nothing } from "lit";
import type { ConfigUiHints } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { t } from "../strings.js";
import {
  getChannelFormDef,
  getValueAtPath,
  type ChannelFieldDef,
} from "./channels-config-fields.ts";

type ChannelConfigFormProps = {
  channelId: string;
  configValue: Record<string, unknown> | null;
  schema: unknown;
  uiHints: ConfigUiHints;
  disabled: boolean;
  onPatch: (path: Array<string | number>, value: unknown) => void;
};

function resolveChannelValue(
  config: Record<string, unknown>,
  channelId: string,
): Record<string, unknown> {
  const channels = (config.channels ?? {}) as Record<string, unknown>;
  const fromChannels = channels[channelId];
  const fallback = config[channelId];
  const resolved =
    (fromChannels && typeof fromChannels === "object"
      ? (fromChannels as Record<string, unknown>)
      : null) ??
    (fallback && typeof fallback === "object" ? (fallback as Record<string, unknown>) : null);
  return resolved ?? {};
}

function formatFieldValue(raw: unknown, field: ChannelFieldDef): string {
  if (raw == null) {
    return "";
  }
  if (field.type === "boolean") {
    return raw ? "true" : "false";
  }
  if (field.type === "string[]") {
    return Array.isArray(raw) ? raw.join(", ") : typeof raw === "string" ? raw : "";
  }
  return String(raw);
}

function parseFieldValue(value: string, field: ChannelFieldDef): unknown {
  if (field.type === "boolean") {
    return value === "true" || value === "1" || value.toLowerCase() === "yes";
  }
  if (field.type === "number") {
    const n = parseInt(value, 10);
    return isNaN(n) ? undefined : n;
  }
  if (field.type === "string[]") {
    return value.trim() ? value.split(/,\s*/).map((s) => s.trim()).filter(Boolean) : [];
  }
  return value;
}

export function renderChannelConfigForm(props: ChannelConfigFormProps) {
  const formDef = getChannelFormDef(props.channelId);
  const configValue = props.configValue ?? {};
  const value = resolveChannelValue(configValue, props.channelId) as Record<string, unknown>;

  if (!formDef) {
    return html`
      <div class="callout danger">${t("channelsConfigSchemaUnavailable")}</div>
    `;
  }

  return html`
    <div class="config-form">
      ${formDef.fields.map((field) => {
        const raw = getValueAtPath(value, field.path);
        const displayValue = formatFieldValue(raw, field);
        const basePath = ["channels", props.channelId, ...field.path];
        return html`
          <div class="field">
            <span>
              ${field.label}
              ${field.required ? html`<span style="color: var(--danger-color);">*</span>` : ""}
            </span>
            ${field.type === "boolean"
              ? html`
                  <div class="row" style="align-items: center; gap: 8px;">
                    <input
                      type="checkbox"
                      ?checked=${raw === true}
                      ?disabled=${props.disabled}
                      @change=${(e: Event) =>
                        props.onPatch(basePath, (e.target as HTMLInputElement).checked)}
                    />
                  </div>
                `
              : html`
                  <input
                    type="${field.type === "number" ? "number" : "text"}"
                    .value=${displayValue}
                    placeholder=${field.placeholder ?? ""}
                    ?disabled=${props.disabled}
                    @input=${(e: Event) => {
                      const v = (e.target as HTMLInputElement).value;
                      props.onPatch(basePath, parseFieldValue(v, field));
                    }}
                  />
                `}
          </div>
        `;
      })}
    </div>
  `;
}

export function renderChannelConfigSection(params: { channelId: string; props: ChannelsProps }) {
  const { channelId, props } = params;
  const disabled = props.configSaving;
  return html`
    <div style="margin-top: 16px;">
      ${renderChannelConfigForm({
        channelId,
        configValue: props.configForm,
        schema: props.configSchema,
        uiHints: props.configUiHints,
        disabled,
        onPatch: props.onConfigPatch,
      })}
      <div class="row" style="margin-top: 12px;">
        <button
          class="btn primary"
          ?disabled=${disabled || !props.configFormDirty}
          @click=${() => props.onConfigSave()}
        >
          ${props.configSaving ? t("commonSaving") : t("commonSave")}
        </button>
        <button
          class="btn"
          ?disabled=${disabled}
          @click=${() => props.onConfigReload()}
        >
          ${t("commonReload")}
        </button>
      </div>
    </div>
  `;
}

const CHANNEL_LABELS: Record<string, string> = {
  whatsapp: "WhatsApp",
  telegram: "Telegram",
  discord: "Discord",
  googlechat: "Google Chat",
  slack: "Slack",
  signal: "Signal",
  imessage: "iMessage",
  nostr: "Nostr",
  dingtalk: "DingTalk",
  feishu: "Feishu",
  wework: "WeWork",
  qq: "QQ",
};

/** Renders the slide-out panel for channel config (CoPaw-style). Shown when selectedChannelId is set. */
export function renderChannelConfigPanel(props: ChannelsProps) {
  const channelId = props.selectedChannelId;
  if (!channelId) {
    return nothing;
  }
  const label = CHANNEL_LABELS[channelId.toLowerCase()] ?? channelId.charAt(0).toUpperCase() + channelId.slice(1);
  return html`
    <div
      class="channel-panel-overlay"
      @click=${(e: Event) => {
        if ((e.target as HTMLElement).classList.contains("channel-panel-overlay")) {
          props.onChannelSelect(null);
        }
      }}
    >
      <div class="channel-panel card" @click=${(e: Event) => e.stopPropagation()}>
        <div class="channel-panel-header row" style="justify-content: space-between; align-items: center;">
          <div class="card-title">${label} ${t("configSettingsTitle")}</div>
          <button class="btn" @click=${() => props.onChannelSelect(null)}>×</button>
        </div>
        <div class="channel-panel-content">
          ${renderChannelConfigSection({ channelId, props })}
        </div>
      </div>
    </div>
  `;
}
