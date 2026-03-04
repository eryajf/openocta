import { html, nothing } from "lit";
import type { WhatsAppStatus } from "../types.ts";
import type { ChannelsProps } from "./channels.types.ts";
import { formatAgo } from "../format.ts";
import { t } from "../strings.js";
import { formatDuration } from "./channels.shared.ts";

export function renderWhatsAppCard(params: {
  props: ChannelsProps;
  whatsapp?: WhatsAppStatus;
  accountCountLabel: unknown;
}) {
  const { props, whatsapp, accountCountLabel } = params;

  return html`
    <div class="card">
      <div class="card-title">${t("channelWhatsApp")}</div>
      <div class="card-sub">${t("channelWhatsAppSub")}</div>
      ${accountCountLabel}

      <div class="status-list" style="margin-top: 16px;">
        <div>
          <span class="label">${t("channelConfigured")}</span>
          <span>${whatsapp?.configured ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelLinked")}</span>
          <span>${whatsapp?.linked ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelRunning")}</span>
          <span>${whatsapp?.running ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelConnected")}</span>
          <span>${whatsapp?.connected ? t("commonYes") : t("commonNo")}</span>
        </div>
        <div>
          <span class="label">${t("channelLastConnect")}</span>
          <span>
            ${whatsapp?.lastConnectedAt ? formatAgo(whatsapp.lastConnectedAt) : t("commonNA")}
          </span>
        </div>
        <div>
          <span class="label">${t("channelLastMessage")}</span>
          <span>
            ${whatsapp?.lastMessageAt ? formatAgo(whatsapp.lastMessageAt) : t("commonNA")}
          </span>
        </div>
        <div>
          <span class="label">${t("channelAuthAge")}</span>
          <span>
            ${whatsapp?.authAgeMs != null ? formatDuration(whatsapp.authAgeMs) : t("commonNA")}
          </span>
        </div>
      </div>

      ${
        whatsapp?.lastError
          ? html`<div class="callout danger" style="margin-top: 12px;">
            ${whatsapp.lastError}
          </div>`
          : nothing
      }

      ${
        props.whatsappMessage
          ? html`<div class="callout" style="margin-top: 12px;">
            ${props.whatsappMessage}
          </div>`
          : nothing
      }

      ${
        props.whatsappQrDataUrl
          ? html`<div class="qr-wrap">
            <img src=${props.whatsappQrDataUrl} alt="WhatsApp QR" />
          </div>`
          : nothing
      }

      <div class="row" style="margin-top: 14px; flex-wrap: wrap;">
        <button
          class="btn primary"
          ?disabled=${props.whatsappBusy}
          @click=${() => props.onWhatsAppStart(false)}
        >
          ${props.whatsappBusy ? t("channelWhatsAppWorking") : t("channelShowQr")}
        </button>
        <button
          class="btn"
          ?disabled=${props.whatsappBusy}
          @click=${() => props.onWhatsAppStart(true)}
        >
          ${t("channelRelink")}
        </button>
        <button
          class="btn"
          ?disabled=${props.whatsappBusy}
          @click=${() => props.onWhatsAppWait()}
        >
          ${t("channelWaitForScan")}
        </button>
        <button
          class="btn danger"
          ?disabled=${props.whatsappBusy}
          @click=${() => props.onWhatsAppLogout()}
        >
          ${t("channelLogout")}
        </button>
        <button class="btn" @click=${() => props.onRefresh(true)}>
          ${t("commonRefresh")}
        </button>
        <button class="btn primary" @click=${() => props.onChannelSelect("whatsapp")}>
          ${t("channelsConfigure")}
        </button>
      </div>
    </div>
  `;
}
