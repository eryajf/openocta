import { html, nothing } from "lit";
import type { IMessageStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderIMessageCard(params: {
  props: ChannelsProps;
  imessage?: IMessageStatus | null;
  accountCountLabel: unknown;
}) {
  const { props, imessage, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelIMessage")}</div>
      <div class="card-sub">${t("channelIMessageSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${imessage?.configured ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${imessage?.running ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastStart")}</span>
          <span>${imessage?.lastStartAt ? formatAgo(imessage.lastStartAt) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastProbe")}</span>
          <span>${imessage?.lastProbeAt ? formatAgo(imessage.lastProbeAt) : t("commonNA")}</span>
        </div>
      </div>

      ${
        imessage?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${imessage.lastError}
          </div>`
          : nothing
      }

      ${
        imessage?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${imessage.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${imessage.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("imessage")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
