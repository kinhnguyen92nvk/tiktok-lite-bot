/**
 * TIKTOK_LITE_BOT - CommonJS
 * Telegram bot + Google Sheets DB
 *
 * Auth: Secret File JSON + GOOGLE_APPLICATION_CREDENTIALS path
 * -> read JSON via fs -> useServiceAccountAuth()
 */

const fs = require("fs");
const TelegramBot = require("node-telegram-bot-api");
const { GoogleSpreadsheet } = require("google-spreadsheet");
const cron = require("node-cron");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezone);

const TZ = process.env.TZ || "Asia/Seoul";

// ===== ENV =====
const BOT_TOKEN = process.env.BOT_TOKEN;
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const ADMIN_ID = String(process.env.ADMIN_TELEGRAM_ID || "");
const ADC_PATH = process.env.GOOGLE_APPLICATION_CREDENTIALS;

if (!BOT_TOKEN || !SHEET_ID) {
  console.error("Missing env vars. Need BOT_TOKEN and GOOGLE_SHEET_ID");
  process.exit(1);
}

if (!ADC_PATH) {
  console.error(
    "Missing GOOGLE_APPLICATION_CREDENTIALS. Set it to /etc/secrets/<your-secret-file>.json"
  );
  process.exit(1);
}

// ===== BOT =====
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// ===== Session state =====
const sessions = new Map(); // chatId -> { pending: {...} }

// ===== Google Sheet =====
const doc = new GoogleSpreadsheet(SHEET_ID);
const sheets = {}; // tabName -> worksheet

function nowSeoul() {
  return dayjs().tz(TZ);
}

// ===== Money parser =====
// 100k => 100000, 0.5k => 500, 120000 => 120000
function parseMoney(input) {
  if (!input) return null;
  const s = String(input).trim().toLowerCase().replace(/,/g, "");
  const m = s.match(/^(\d+(\.\d+)?)(k)?$/);
  if (!m) return null;
  const num = Number(m[1]);
  if (Number.isNaN(num)) return null;
  return m[3] ? Math.round(num * 1000) : Math.round(num);
}

function fmtMoney(n) {
  if (n == null) return "";
  return Number(n).toLocaleString("en-US");
}

function isAdmin(msg) {
  return ADMIN_ID && String(msg.from?.id || "") === ADMIN_ID;
}

// ===== Sheet helpers =====
async function appendRow(tab, rowObj) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing sheet tab ${tab}`);
  return await ws.addRow(rowObj);
}

async function getAllRows(tab) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing sheet tab ${tab}`);
  return await ws.getRows();
}

async function logUndo(action, payload) {
  if (!sheets["UNDO_LOG"]) return;
  await appendRow("UNDO_LOG", {
    timestamp: nowSeoul().format(),
    action,
    payload: JSON.stringify(payload),
  });
}

// ===== Init Google Sheet (FIXED AUTH) =====
async function initSheet() {
  // Read creds from secret file path
  let creds;
  try {
    const raw = fs.readFileSync(ADC_PATH, "utf8");
    creds = JSON.parse(raw);
  } catch (e) {
    console.error("Failed to read GOOGLE_APPLICATION_CREDENTIALS file:", ADC_PATH);
    console.error(e);
    process.exit(1);
  }

  if (!creds.client_email || !creds.private_key) {
    console.error("Invalid service account JSON: missing client_email/private_key");
    process.exit(1);
  }

  await doc.useServiceAccountAuth({
    client_email: creds.client_email,
    private_key: creds.private_key,
  });

  await doc.loadInfo();

  const requiredTabs = [
    "SETTINGS",
    "WALLETS",
    "WALLET_LOG",
    "PHONES",
    "LOTS",
    "LOT_RESULT",
    "PHONE_PROFIT_LOG",
    "INVITES",
    "CHECKIN_REWARD",
    "GAME_REVENUE",
    "UNDO_LOG",
  ];

  for (const name of requiredTabs) {
    const ws = doc.sheetsByTitle[name];
    if (!ws) {
      console.warn(`⚠️ Missing tab: ${name} (please create it in Google Sheet)`);
    } else {
      sheets[name] = ws;
    }
  }

  console.log("✅ Sheet loaded:", doc.title);
}

// ===== Wallets =====
async function upsertWalletBalance(wallet, delta, ref = "", note = "") {
  if (!sheets["WALLETS"] || !sheets["WALLET_LOG"]) {
    throw new Error("Missing WALLETS or WALLET_LOG tab");
  }

  const rows = await getAllRows("WALLETS");
  let row = rows.find((r) => String(r.Wallet || "").toLowerCase() === wallet);

  if (!row) {
    row = await appendRow("WALLETS", { Wallet: wallet, Balance: 0 });
  }

  const current = Number(String(row.Balance || "0").replace(/,/g, "")) || 0;
  const next = current + delta;
  row.Balance = next;
  await row.save();

  await appendRow("WALLET_LOG", {
    timestamp: nowSeoul().format(),
    wallet,
    amount: delta,
    type: delta < 0 ? "debit" : "credit",
    ref,
    note,
  });

  return next;
}

// ===== Commands menu =====
async function setupBotCommands() {
  await bot.setMyCommands([
    { command: "start", description: "Bắt đầu / hướng dẫn nhanh" },
    { command: "help", description: "Xem lệnh" },
    { command: "baocao", description: "Báo cáo tháng (tổng quan)" },
    { command: "pending", description: "Danh sách invite pending / quá hạn" },
    { command: "undo", description: "Hoàn tác lệnh gần nhất (khung)" }
  ]);
}

// ===== Core game =====
async function addGameRevenue(chatId, game, amount, type, note = "", meta = {}) {
  if (!sheets["GAME_REVENUE"]) throw new Error("Missing GAME_REVENUE tab");

  await appendRow("GAME_REVENUE", {
    timestamp: nowSeoul().format(),
    game,
    type,
    amount,
    note,
    chatId,
    ...meta,
  });

  await logUndo("ADD_GAME_REVENUE", { chatId, game, amount, type, note, meta });
}

async function createInvite(chatId, game, name, email) {
  if (!sheets["INVITES"]) throw new Error("Missing INVITES tab");

  const invitedAt = nowSeoul();
  const due = invitedAt.add(14, "day");

  await appendRow("INVITES", {
    timestamp: invitedAt.format(),
    game,
    name,
    email,
    time_invited: invitedAt.format(),
    due_date: due.format(),
    status: "pending",
    chatId,
    last_reminded_at: "",
  });

  await logUndo("ADD_INVITE", {
    chatId,
    game,
    name,
    email,
    time_invited: invitedAt.format(),
    due_date: due.format(),
  });

  return { due };
}

async function markInviteDoneAndAddCheckin(chatId, game, name, email, reward) {
  if (!sheets["INVITES"] || !sheets["CHECKIN_REWARD"]) {
    throw new Error("Missing INVITES or CHECKIN_REWARD tab");
  }

  const rows = await getAllRows("INVITES");

  const target = rows
    .filter(
      (r) =>
        String(r.status || "").toLowerCase() === "pending" &&
        String(r.game || "").toLowerCase() === game &&
        ((email && String(r.email || "").toLowerCase() === email.toLowerCase()) ||
          String(r.name || "").toLowerCase() === name.toLowerCase())
    )
    .sort(
      (a, b) =>
        new Date(b.time_invited || b.timestamp) -
        new Date(a.time_invited || a.timestamp)
    )[0];

  if (!target) throw new Error(`Không tìm thấy invite pending cho ${game} ${name}`);

  await appendRow("CHECKIN_REWARD", {
    timestamp: nowSeoul().format(),
    game,
    name: target.name,
    email: target.email,
    reward,
    due_date: target.due_date,
    chatId,
  });

  await addGameRevenue(
    chatId,
    game,
    reward,
    "checkin_reward",
    `checkin 14 ngày: ${target.name}`,
    { name: target.name, email: target.email }
  );

  target.status = "done";
  target.checkin_reward = reward;
  target.completed_at = nowSeoul().format();
  await target.save();

  await logUndo("DONE_INVITE_CHECKIN", {
    inviteRowId: target._rowNumber,
    chatId,
    game,
    reward,
  });
}

// ===== Normalize game token =====
function normalizeGameToken(tok) {
  const t = String(tok || "").toLowerCase();
  if (t === "hopqua" || t === "hq" || t === "hh") return "hq";
  if (t === "qr") return "qr";
  if (t === "dabong" || t === "db") return "db";
  return t;
}

// ===== Reports =====
async function reportMonth(chatId, ym = nowSeoul().format("YYYY-MM")) {
  if (!sheets["GAME_REVENUE"]) throw new Error("Missing GAME_REVENUE tab");

  const rows = await getAllRows("GAME_REVENUE");
  const monthRows = rows.filter((r) => String(r.timestamp || "").startsWith(ym));

  const byGame = {};
  for (const r of monthRows) {
    const g = String(r.game || "unknown");
    byGame[g] = (byGame[g] || 0) + (Number(r.amount) || 0);
  }
  const total = Object.values(byGame).reduce((a, v) => a + v, 0);

  let text = `📊 Báo cáo tháng ${ym}\n`;
  text += `• Tổng thu TikTok: ${fmtMoney(total)}\n`;
  for (const [g, v] of Object.entries(byGame)) {
    text += `  - ${g}: ${fmtMoney(v)}\n`;
  }

  await bot.sendMessage(chatId, text);
}

async function listPending(chatId) {
  if (!sheets["INVITES"]) throw new Error("Missing INVITES tab");

  const invites = await getAllRows("INVITES");
  const now = nowSeoul();

  const pending = invites
    .filter((r) => String(r.status || "").toLowerCase() === "pending")
    .map((r) => {
      const due = dayjs(r.due_date).tz(TZ);
      return { r, due, overdue: due.isValid() && due.isBefore(now) };
    })
    .sort((a, b) => a.due.valueOf() - b.due.valueOf());

  if (pending.length === 0) {
    await bot.sendMessage(chatId, "✅ Không có invite pending.");
    return;
  }

  let text = `🕒 Pending invites (${pending.length})\n`;
  for (const { r, due, overdue } of pending.slice(0, 50)) {
    const dueStr = due.isValid() ? due.format("ddd DD/MM") : "invalid";
    text += `• ${overdue ? "⚠️" : "⏳"} ${r.game} - ${r.name} (${r.email}) due: ${dueStr}\n`;
  }
  await bot.sendMessage(chatId, text);
}

// ===== Reminder cron =====
async function runDueCheck() {
  try {
    if (!sheets["INVITES"]) return;

    const invites = await getAllRows("INVITES");
    const now = nowSeoul();

    for (const r of invites) {
      if (String(r.status || "").toLowerCase() !== "pending") continue;

      const due = dayjs(r.due_date).tz(TZ);
      if (!due.isValid()) continue;

      if (now.isAfter(due) || now.isSame(due)) {
        const last = r.last_reminded_at ? dayjs(r.last_reminded_at).tz(TZ) : null;
        const remindedToday =
          last && last.isValid() && last.format("YYYY-MM-DD") === now.format("YYYY-MM-DD");
        if (remindedToday) continue;

        const chatId = r.chatId;
        const gameLabel = String(r.game || "").toLowerCase() === "hq" ? "Hopqua" : "QR";

        sessions.set(String(chatId), {
          pending: {
            type: "ask_checkin_reward",
            data: { game: String(r.game || "").toLowerCase(), name: r.name, email: r.email },
          },
        });

        await bot.sendMessage(chatId, `${gameLabel} ${r.name} = bao nhiêu? (vd: 60k)`);
        r.last_reminded_at = now.format();
        await r.save();
      }
    }
  } catch (e) {
    console.error("runDueCheck error:", e.message);
  }
}

function startCron() {
  cron.schedule("0 10 * * *", runDueCheck, { timezone: TZ });
  cron.schedule("15 * * * *", runDueCheck, { timezone: TZ });
}

// ===== Session helpers =====
function getSession(chatId) {
  return sessions.get(String(chatId));
}
function clearSession(chatId) {
  sessions.delete(String(chatId));
}

// ===== Follow-up handlers =====
async function handleCheckinAnswer(chatId, text, context) {
  const reward = parseMoney(text);
  if (reward == null) {
    await bot.sendMessage(chatId, "Không parse được tiền. Ví dụ: 60k hoặc 30000");
    return;
  }
  const { game, name, email } = context.data;
  await markInviteDoneAndAddCheckin(chatId, game, name, email, reward);
  clearSession(chatId);
  await bot.sendMessage(chatId, `✅ Checkin ${game} ${name}: +${fmtMoney(reward)}`);
}

function parseCommand(text) {
  const raw = String(text || "").trim();
  const parts = raw.split(/\s+/);
  return { raw, parts };
}

// ===== Main message handler =====
bot.on("message", async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text || "";

  try {
    // Follow-up
    const sess = getSession(chatId);
    if (sess?.pending?.type) {
      const pending = sess.pending;
      if (pending.type === "ask_checkin_reward") {
        await handleCheckinAnswer(chatId, text, pending);
        return;
      }
    }

    // Slash commands
    if (text.startsWith("/start")) {
      await bot.sendMessage(
        chatId,
        "✅ TIKTOK_LITE_BOT\n\n" +
          "Gõ nhanh:\n" +
          "• dabong 100k\n" +
          "• hopqua Khanh mail@gmail.com\n" +
          "• hopqua 200k\n" +
          "• qr Khanh mail@gmail.com\n" +
          "• qr 57k\n" +
          "• them 0.5k\n\n" +
          "Báo cáo:\n" +
          "• /baocao\n" +
          "• /pending\n"
      );
      return;
    }

    if (text.startsWith("/help")) {
      await bot.sendMessage(
        chatId,
        "📌 Lệnh:\n" +
          "GAME:\n" +
          "- dabong 100k\n" +
          "- hopqua <Name> <Email>\n" +
          "- hopqua 200k\n" +
          "- qr <Name> <Email>\n" +
          "- qr 57k\n" +
          "- them 0.5k\n\n" +
          "BÁO CÁO:\n" +
          "- /baocao\n" +
          "- /pending\n"
      );
      return;
    }

    if (text.startsWith("/baocao")) {
      await reportMonth(chatId);
      return;
    }

    if (text.startsWith("/pending")) {
      await listPending(chatId);
      return;
    }

    if (text.startsWith("/undo")) {
      await bot.sendMessage(chatId, "⚠️ /undo: hiện mới log UNDO_LOG. Muốn rollback thật mình sẽ làm tiếp.");
      return;
    }

    // Free text commands
    const { parts } = parseCommand(text);
    if (parts.length === 0) return;

    const cmd = parts[0].toLowerCase();

    // GAME: dabong
    if (cmd === "dabong" || cmd === "db") {
      const amount = parseMoney(parts[1]);
      if (amount == null) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: dabong 100k");
        return;
      }
      await addGameRevenue(chatId, "db", amount, "invite_reward", "dabong invite reward");
      await bot.sendMessage(chatId, `✅ DB +${fmtMoney(amount)}`);
      return;
    }

    // GAME: hopqua
    if (cmd === "hopqua" || cmd === "hq") {
      if (parts.length === 2) {
        const amount = parseMoney(parts[1]);
        if (amount == null) {
          await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: hopqua 200k hoặc hopqua Khanh mail@gmail.com");
          return;
        }
        await addGameRevenue(chatId, "hq", amount, "invite_reward", "hopqua invite reward");
        await bot.sendMessage(chatId, `✅ HQ +${fmtMoney(amount)}`);
        return;
      }

      if (parts.length >= 3) {
        const name = parts[1];
        const email = parts[2];
        const { due } = await createInvite(chatId, "hq", name, email);
        await bot.sendMessage(
          chatId,
          `✅ Đã lưu invite HQ: ${name} (${email})\n⏰ Due: ${due.format("ddd DD/MM")} (${TZ})`
        );
        return;
      }

      await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: hopqua 200k hoặc hopqua Khanh mail@gmail.com");
      return;
    }

    // GAME: qr
    if (cmd === "qr") {
      if (parts.length === 2) {
        const amount = parseMoney(parts[1]);
        if (amount == null) {
          await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: qr 57k hoặc qr Khanh mail@gmail.com");
          return;
        }
        await addGameRevenue(chatId, "qr", amount, "invite_reward", "qr invite reward");
        await bot.sendMessage(chatId, `✅ QR +${fmtMoney(amount)}`);
        return;
      }

      if (parts.length >= 3) {
        const name = parts[1];
        const email = parts[2];
        const { due } = await createInvite(chatId, "qr", name, email);
        await bot.sendMessage(
          chatId,
          `✅ Đã lưu invite QR: ${name} (${email})\n⏰ Due: ${due.format("ddd DD/MM")} (${TZ})`
        );
        return;
      }

      await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: qr 57k hoặc qr Khanh mail@gmail.com");
      return;
    }

    // other income
    if (cmd === "them") {
      const amount = parseMoney(parts[1]);
      if (amount == null) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: them 0.5k");
        return;
      }
      await addGameRevenue(chatId, "other", amount, "other_income", "other income");
      await bot.sendMessage(chatId, `✅ THÊM +${fmtMoney(amount)}`);
      return;
    }

    // admin set wallet (optional)
    if (cmd === "chinh") {
      if (!isAdmin(msg)) {
        await bot.sendMessage(chatId, "⛔ Bạn không có quyền dùng lệnh này.");
        return;
      }
      const wallet = (parts[1] || "").toLowerCase();
      const amount = parseMoney(parts[2]);
      if (!["uri", "hana", "kt"].includes(wallet) || amount == null) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: chinh hana 500k");
        return;
      }

      const rows = await getAllRows("WALLETS");
      let row = rows.find((r) => String(r.Wallet || "").toLowerCase() === wallet);
      if (!row) row = await appendRow("WALLETS", { Wallet: wallet, Balance: 0 });

      const current = Number(String(row.Balance || "0").replace(/,/g, "")) || 0;
      const delta = amount - current;

      row.Balance = amount;
      await row.save();

      await appendRow("WALLET_LOG", {
        timestamp: nowSeoul().format(),
        wallet,
        amount: delta,
        type: "admin_set",
        ref: "ADMIN_SET",
        note: `set balance to ${amount}`,
      });

      await bot.sendMessage(chatId, `✅ Set ví ${wallet} = ${fmtMoney(amount)} (delta ${fmtMoney(delta)})`);
      return;
    }

    await bot.sendMessage(chatId, "Mình không hiểu lệnh. Gõ /help để xem cú pháp.");
  } catch (e) {
    console.error("handler error:", e);
    await bot.sendMessage(chatId, `❌ Lỗi: ${e.message}`);
  }
});

// ===== Boot =====
(async () => {
  await initSheet();
  await setupBotCommands();
  startCron();
  console.log("✅ Bot started.");
})();
