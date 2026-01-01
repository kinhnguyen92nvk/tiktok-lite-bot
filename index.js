/**
 * TikTok Lite Bot (Polling-only)
 * - Telegram: node-telegram-bot-api (polling)
 * - Google Sheet DB: google-spreadsheet v4 (service account JSON via secret file)
 *
 * ENV required:
 *   BOT_TOKEN
 *   GOOGLE_SHEET_ID
 *   GOOGLE_APPLICATION_CREDENTIALS  (e.g. /etc/secrets/google-service-account.json)
 * Optional:
 *   ADMIN_TELEGRAM_ID
 *   TZ (default Asia/Seoul)
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

if (!BOT_TOKEN) {
  console.error("Missing BOT_TOKEN");
  process.exit(1);
}
if (!SHEET_ID) {
  console.error("Missing GOOGLE_SHEET_ID");
  process.exit(1);
}
if (!ADC_PATH) {
  console.error(
    "Missing GOOGLE_APPLICATION_CREDENTIALS. Example: /etc/secrets/google-service-account.json"
  );
  process.exit(1);
}

// ===== Telegram bot (POLLING ONLY) =====
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// ===== Google Sheet =====
const doc = new GoogleSpreadsheet(SHEET_ID);
const sheets = {}; // tabName -> worksheet

// ===== Session memory (due-date Q&A) =====
const sessions = new Map(); // chatId -> { pending: {type, data} }

function nowTz() {
  return dayjs().tz(TZ);
}

// ===== Money parser =====
// Supports: 100k => 100000, 0.5k => 500, 120000 => 120000, 12,000 => 12000
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
  return Number(n || 0).toLocaleString("en-US");
}

function isAdmin(msg) {
  return ADMIN_ID && String(msg.from?.id || "") === ADMIN_ID;
}

// ===== Sheet helpers =====
async function appendRow(tab, rowObj) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing tab: ${tab}`);
  return ws.addRow(rowObj);
}

async function getAllRows(tab) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing tab: ${tab}`);
  return ws.getRows();
}

async function logUndo(action, payload) {
  if (!sheets["UNDO_LOG"]) return;
  await appendRow("UNDO_LOG", {
    timestamp: nowTz().format(),
    action,
    payload: JSON.stringify(payload),
  });
}

// ===== Init sheet (FIXED for google-spreadsheet v4) =====
async function initSheet() {
  let creds;
  try {
    creds = JSON.parse(fs.readFileSync(ADC_PATH, "utf8"));
  } catch (e) {
    console.error("Failed to read service account JSON:", ADC_PATH);
    console.error(e);
    process.exit(1);
  }

  // v4: pass full JSON object
  await doc.useServiceAccountAuth(creds);

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
      console.warn(`⚠️ Missing tab in sheet: ${name}`);
    } else {
      sheets[name] = ws;
    }
  }

  console.log("✅ Sheet loaded:", doc.title);
}

// ===== Commands menu =====
async function setupBotCommands() {
  await bot.setMyCommands([
    { command: "start", description: "Bắt đầu / hướng dẫn nhanh" },
    { command: "help", description: "Xem lệnh" },
    { command: "baocao", description: "Báo cáo tháng (tổng thu)" },
    { command: "pending", description: "Danh sách invite pending / quá hạn" },
    { command: "undo", description: "Hoàn tác (mới log UNDO_LOG)" },
  ]);
}

// ===== Game revenue =====
async function addGameRevenue(chatId, game, amount, type, note = "", meta = {}) {
  if (!sheets["GAME_REVENUE"]) throw new Error("Missing GAME_REVENUE tab");

  await appendRow("GAME_REVENUE", {
    timestamp: nowTz().format(),
    game,
    type,
    amount,
    note,
    chatId,
    ...meta,
  });

  await logUndo("ADD_GAME_REVENUE", { chatId, game, amount, type, note, meta });
}

// ===== Invites =====
async function createInvite(chatId, game, name, email) {
  if (!sheets["INVITES"]) throw new Error("Missing INVITES tab");

  const invitedAt = nowTz();
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
    .sort((a, b) => new Date(b.time_invited || b.timestamp) - new Date(a.time_invited || a.timestamp))[0];

  if (!target) throw new Error(`Không tìm thấy invite pending cho ${game} ${name}`);

  await appendRow("CHECKIN_REWARD", {
    timestamp: nowTz().format(),
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
  target.completed_at = nowTz().format();
  await target.save();

  await logUndo("DONE_INVITE_CHECKIN", {
    inviteRowNumber: target._rowNumber,
    chatId,
    game,
    reward,
  });
}

// ===== Reports =====
async function reportMonth(chatId, ym = nowTz().format("YYYY-MM")) {
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
  const now = nowTz();

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
    const now = nowTz();

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
        const game = String(r.game || "").toLowerCase();
        const gameLabel = game === "hq" ? "Hopqua" : game === "qr" ? "QR" : game;

        sessions.set(String(chatId), {
          pending: {
            type: "ask_checkin_reward",
            data: { game, name: r.name, email: r.email },
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
  // check hourly + daily 10:00
  cron.schedule("15 * * * *", runDueCheck, { timezone: TZ });
  cron.schedule("0 10 * * *", runDueCheck, { timezone: TZ });
}

// ===== Follow-up handler =====
async function handleCheckinAnswer(chatId, text, pending) {
  const reward = parseMoney(text);
  if (reward == null) {
    await bot.sendMessage(chatId, "Không parse được tiền. Ví dụ: 60k hoặc 30000");
    return;
  }

  const { game, name, email } = pending.data;
  await markInviteDoneAndAddCheckin(chatId, game, name, email, reward);

  sessions.delete(String(chatId));
  await bot.sendMessage(chatId, `✅ Checkin ${game} ${name}: +${fmtMoney(reward)}`);
}

// ===== Message handler =====
bot.on("message", async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text || "";

  try {
    // follow-up
    const sess = sessions.get(String(chatId));
    if (sess?.pending?.type === "ask_checkin_reward") {
      await handleCheckinAnswer(chatId, text, sess.pending);
      return;
    }

    // slash commands
    if (text.startsWith("/start")) {
      await bot.sendMessage(
        chatId,
        "✅ TIKTOK_LITE_BOT (Polling)\n\n" +
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

    // free-text commands
    const parts = String(text).trim().split(/\s+/);
    if (!parts[0]) return;

    const cmd = parts[0].toLowerCase();

    // db / dabong
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

    // hopqua / hq
    if (cmd === "hopqua" || cmd === "hq") {
      // hopqua 200k
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

      // hopqua Name Email
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

    // qr
    if (cmd === "qr") {
      // qr 57k
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

      // qr Name Email
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

    // admin example: chinh hana 500k (optional)
    if (cmd === "chinh") {
      if (!isAdmin(msg)) {
        await bot.sendMessage(chatId, "⛔ Bạn không có quyền dùng lệnh này.");
        return;
      }
      await bot.sendMessage(chatId, "Lệnh admin chưa implement đầy đủ ở bản này.");
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
  console.log("✅ TikTok Lite Bot started (polling).");
})();
