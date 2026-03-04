import { html, nothing } from "lit";
import type { GoogleChatStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderGoogleChatCard(params: {
  props: ChannelsProps;
  googleChat?: GoogleChatStatus | null;
  accountCountLabel: unknown;
}) {
  const { props, googleChat, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelGoogleChat")}</div>
      <div class="card-sub">${t("channelGoogleChatSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${googleChat ? (googleChat.configured ? t("commonYes") : t("commonNo")) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${googleChat ? (googleChat.running ? t("commonYes") : t("commonNo")) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelCredential")}</span>
          <span>${googleChat?.credentialSource ?? t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelAudience")}</span>
          <span>
            ${
              googleChat?.audienceType
                ? `${googleChat.audienceType}${googleChat.audience ? ` · ${googleChat.audience}` : ""}`
                : t("commonNA")
            }
          </span>
        </div>
        <div>
          <span class="label">${t("channelLastStart")}</span>
          <span>${googleChat?.lastStartAt ? formatAgo(googleChat.lastStartAt) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastProbe")}</span>
          <span>${googleChat?.lastProbeAt ? formatAgo(googleChat.lastProbeAt) : t("commonNA")}</span>
        </div>
      </div>

      ${
        googleChat?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${googleChat.lastError}
          </div>`
          : nothing
      }

      ${
        googleChat?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${googleChat.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${googleChat.probe.status ?? ""} ${googleChat.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("googlechat")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
