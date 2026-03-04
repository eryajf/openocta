import { html, nothing } from "lit";
import type { DiscordStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderDiscordCard(params: {
  props: ChannelsProps;
  discord?: DiscordStatus | null;
  accountCountLabel: unknown;
}) {
  const { props, discord, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelDiscord")}</div>
      <div class="card-sub">${t("channelDiscordSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${discord?.configured ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${discord?.running ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastStart")}</span>
          <span>${discord?.lastStartAt ? formatAgo(discord.lastStartAt) : t("commonNA")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastProbe")}</span>
          <span>${discord?.lastProbeAt ? formatAgo(discord.lastProbeAt) : t("commonNA")}</span>
        </div>
      </div>

      ${
        discord?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${discord.lastError}
          </div>`
          : nothing
      }

      ${
        discord?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${discord.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${discord.probe.status ?? ""} ${discord.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("discord")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
