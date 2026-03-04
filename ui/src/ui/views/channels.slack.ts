import { html, nothing } from "lit";
import type { SlackStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderSlackCard(params: {
  props: ChannelsProps;
  slack?: SlackStatus | null;
  accountCountLabel: unknown;
}) {
  const { props, slack, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelSlack")}</div>
      <div class="card-sub">${t("channelSlackSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${slack?.configured ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${slack?.running ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastStart")}</span>
          <span>${slack?.lastStartAt ? formatAgo(slack.lastStartAt) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastProbe")}</span>
          <span>${slack?.lastProbeAt ? formatAgo(slack.lastProbeAt) : t("commonNA")}</span>
        </div>
      </div>

      ${
        slack?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${slack.lastError}
          </div>`
          : nothing
      }

      ${
        slack?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${slack.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${slack.probe.status ?? ""} ${slack.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("slack")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
