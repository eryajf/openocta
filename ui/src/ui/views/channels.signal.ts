import { html, nothing } from "lit";
import type { SignalStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderSignalCard(params: {
  props: ChannelsProps;
  signal?: SignalStatus | null;
  accountCountLabel: unknown;
}) {
  const { props, signal, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelSignal")}</div>
      <div class="card-sub">${t("channelSignalSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${signal?.configured ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${signal?.running ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelBaseUrl")}</span>
          <span>${signal?.baseUrl ?? t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastStart")}</span>
          <span>${signal?.lastStartAt ? formatAgo(signal.lastStartAt) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastProbe")}</span>
          <span>${signal?.lastProbeAt ? formatAgo(signal.lastProbeAt) : t("commonNA")}</span>
        </div>
      </div>

      ${
        signal?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${signal.lastError}
          </div>`
          : nothing
      }

      ${
        signal?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${signal.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${signal.probe.status ?? ""} ${signal.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("signal")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
