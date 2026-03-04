import { html, nothing } from "lit";
import type { ChannelAccountSnapshot, TelegramStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";

export function renderTelegramCard(params: {
  props: ChannelsProps;
  telegram?: TelegramStatus;
  telegramAccounts: ChannelAccountSnapshot[];
  accountCountLabel: unknown;
}) {
  const { props, telegram, telegramAccounts, accountCountLabel } = params;
  const hasMultipleAccounts = telegramAccounts.length > 1;

  const renderAccountCard = (account: ChannelAccountSnapshot) => {
    const probe = account.probe as { bot?: { username?: string } } | undefined;
    const botUsername = probe?.bot?.username;
    const label = account.name || account.accountId;
    return html`
      <div class="account-card">
        <div class="account-card-header">
          <div class="account-card-title">
            ${botUsername ? `@${botUsername}` : label}
          </div>
          <div class="account-card-id">${account.accountId}</div>
        </div>
        <div class="status-list account-card-status">
          <div>
            <span class="label">${t("channelRunning")}</span>
            <span>${account.running ? t("commonYes") : t("commonNo")}</span>
          </div>
          <div>
            <span class="label">${t("channelConfigured")}</span>
            <span>${account.configured ? t("commonYes") : t("commonNo")}</span>
          </div>
          <div>
            <span class="label">${t("channelLastInbound")}</span>
            <span>${account.lastInboundAt ? formatAgo(account.lastInboundAt) : t("commonNA")}</span>
          </div>
          ${
            account.lastError
              ? html`
                <div class="account-card-error">
                  ${account.lastError}
                </div>
              `
              : nothing
          }
        </div>
      </div>
    `;
  };

  return html`
    <div class="card">
      <div class="card-title">${t("channelTelegram")}</div>
      <div class="card-sub">${t("channelTelegramSub")}</div>
      ${accountCountLabel}

      ${
        hasMultipleAccounts
          ? html`
            <div class="account-card-list">
              ${telegramAccounts.map((account) => renderAccountCard(account))}
            </div>
          `
          : html`
            <div class="status-list" style="margin-top: 16px;">
              <div>
                <span class="label">${t("channelConfigured")}</span>
                <span>${telegram?.configured ? t("commonYes") : t("commonNo")}</span>
              </div>
              <div>
                <span class="label">${t("channelRunning")}</span>
                <span>${telegram?.running ? t("commonYes") : t("commonNo")}</span>
              </div>
              <div>
                <span class="label">${t("channelMode")}</span>
                <span>${telegram?.mode ?? t("commonNA")}</span>
              </div>
              <div>
                <span class="label">${t("channelLastStart")}</span>
                <span>${telegram?.lastStartAt ? formatAgo(telegram.lastStartAt) : t("commonNA")}</span>
              </div>
              <div>
                <span class="label">${t("channelLastProbe")}</span>
                <span>${telegram?.lastProbeAt ? formatAgo(telegram.lastProbeAt) : t("commonNA")}</span>
              </div>
            </div>
          `
      }

      ${
        telegram?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${telegram.lastError}
          </div>`
          : nothing
      }

      ${
        telegram?.probe
          ? html`<div class="callout" style="margin-top: 12px;">
            ${t("channelProbe")} ${telegram.probe.ok ? t("channelProbeOk") : t("channelProbeFailed")} ·
            ${telegram.probe.status ?? ""} ${telegram.probe.error ?? ""}
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 12px;">
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("channelProbe")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("telegram")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
