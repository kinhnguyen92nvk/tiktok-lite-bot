import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";

const app = express();
app.use(express.json());

const BOT_TOKEN = process.env.BOT_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

function mustEnv(name) {
  if (!process.env[name]) throw new Error(`Missing env: ${name}`);
}

function getSheetsClient() {
  mustEnv("GOOGLE_CREDENTIALS");
  const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);

  const auth = new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return google.sheets({ version: "v4", auth });
}

async function tgSendMessage(chatId, text) {
  await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text }),
  });
}

async function appendWalletLog({ wallet, type, amount, source, note }) {
  const sheets = getSheetsClient();
  const time = new Date().toISOString();

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: "WALLET_LOG!A:F",
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[time, wallet, type, amount, source, note]],
    },
  });
}

// Health check
app.get("/", (req, res) => res.status(200).send("OK"));

// Telegram webhook
app.post("/webhook", async (req, res) => {
  try {
    const msg = req.body?.message;
    if (!msg?.chat?.id) return res.sendStatus(200);

    const chatId = msg.chat.id;
    const text = (msg.text || "").trim();

    if (text === "/start") {
      await tgSendMessage(
        chatId,
        "✅ Bot test OK.\nGõ: ping\nHoặc: them 2k hana ads"
      );
      return res.sendStatus(200);
    }

    if (text.toLowerCase() === "ping") {
      await tgSendMessage(chatId, "pong ✅");
      return res.sendStatus(200);
    }

    // TEST: them 2k hana ads
    // Format: them <amount> <wallet> [source]
    const m = text.match(/^them\s+([0-9]+(\.[0-9]+)?)(k|m)?\s+(uri|hana|kt)(?:\s+(\S+))?$/i);
    if (m) {
      const num = Number(m[1]);
      const unit = (m[3] || "").toLowerCase();
      const wallet = m[4].toLowerCase();
      const source = (m[5] || "other").toLowerCase();

      let amount = Math.round(num);
      if (unit === "k") amount = Math.round(num * 1000);
      if (unit === "m") amount = Math.round(num * 1000000);

      await appendWalletLog({
        wallet,
        type: "ADD",
        amount,
        source,
        note: text,
      });

      await tgSendMessage(chatId, `✅ Đã ghi WALLET_LOG: +${amount} vào ví ${wallet.toUpperCase()} (${source})`);
      return res.sendStatus(200);
    }

    await tgSendMessage(chatId, "Mình nhận được tin nhắn. Test OK ✅\nGõ: ping hoặc them 2k hana ads");
    return res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error:", e);
    return res.sendStatus(200);
  }
});

async function setWebhook() {
  mustEnv("BOT_TOKEN");
  mustEnv("WEBHOOK_URL");
  const url = `${WEBHOOK_URL.replace(/\/$/, "")}/webhook`;

  const r = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  const data = await r.json();
  console.log("setWebhook:", data);
}

const PORT = process.env.PORT || 10000;
app.listen(PORT, async () => {
  console.log("Server listening on", PORT);
  try {
    setWebhook();
  } catch (e) {
    console.error("setWebhook failed:", e);
  }
});
