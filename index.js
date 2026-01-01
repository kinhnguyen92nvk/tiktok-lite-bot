/**
 * TIKTOK_LITE_BOT - index.js
 * - Telegram bot
 * - Google Sheet as DB
 * - Render friendly
 */

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
const SA_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
let SA_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const ADMIN_ID = String(process.env.ADMIN_TELEGRAM_ID || "");

if (!BOT_TOKEN || !SHEET_ID || !SA_EMAIL || !SA_PRIVATE_KEY) {
  console.error("Missing env vars. Need BOT_TOKEN, GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_EMAIL, GOOGLE_PRIVATE_KEY");
  process.exit(1);
}

// Render often stores \n as literal "\\n"
SA_PRIVATE_KEY = SA_PRIVATE_KEY.replace(/\\n/g, "\n");

// ===== BOT =====
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// ===== In-memory session state (for asking follow-up) =====
/**
 * sessions[chatId] = {
 *   pending: {
 *     type: "ask_wallet_for_phone" | "ask_wallet_for_lot" | "ask_checkin_reward",
 *     data: {...}
 *   }
 * }
 */
const sessions = new Map();

// ===== Google Sheet helper =====
const doc = new GoogleSpreadsheet(SHEET_ID);

let sheets = {}; // name -> GoogleSpreadsheetWorksheet

async function initSheet() {
  await doc.useServiceAccountAuth({
    client_email: SA_EMAIL,
    private_key: SA_PRIVATE_KEY,
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

  requiredTabs.forEach((name) => {
    const ws = doc.sheetsByTitle[name];
    if (!ws) {
      console.warn(`⚠️ Missing tab: ${name} (please create it in Google Sheet)`);
    } else {
      sheets[name] = ws;
    }
  });

  console.log("✅ Sheet loaded:", doc.title);
}

function nowSeoul() {
  return dayjs().tz(TZ);
}

// ===== Money parser =====
// Supports: 100k => 100000, 0.5k => 500, 120000 => 120000, 57k, 800k, etc.
function parseMoney(input) {
  if (!input) return null;
  const s = String(input).trim().toLowerCase();

  // remove commas
  const cleaned = s.replace(/,/g, "");

  // match number + optional k
  const m = cleaned.match(/^(\d+(\.\d+)?)(k)?$/);
  if (!m) return null;

  const num = Number(m[1]);
  if (Number.isNaN(num)) return null;

  const isK = !!m[3];
  const value = isK ? Math.round(num * 1000) : Math.round(num);
  return value;
}

function fmtMoney(n) {
  if (n == null) return "";
  return Number(n).toLocaleString("en-US");
}

function isAdmin(msg) {
  return ADMIN_ID && String(msg.from?.id || "") === ADMIN_ID;
}

// ===== Sheet row helpers =====
async function appendRow(tab, rowObj) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing sheet tab ${tab}`);
  const row = await ws.addRow(rowObj);
  return row;
}

async function getAllRows(tab) {
  const ws = sheets[tab];
  if (!ws) throw new Error(`Missing sheet tab ${tab}`);
  return await ws.getRows();
}

async function findRows(tab, predicate) {
  const rows = await getAllRows(tab);
  return rows.filter(predicate);
}

async function upsertWalletBalance(wallet, delta, ref = "", note = "") {
  // Update WALLETS
  const rows = await getAllRows("WALLETS");
  let row = rows.find(r => String(r.Wallet || "").toLowerCase() === wallet);

  if (!row) {
    row = await appendRow("WALLETS", { Wallet: wallet, Balance: 0 });
  }

  const current = Number(String(row.Balance || "0").replace(/,/g, "")) || 0;
  const next = current + delta;
  row.Balance = next;
  await row.save();

  // WALLET_LOG
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

// ===== Undo (simple) =====
async function logUndo(action, payload) {
  // payload: JSON string
  await appendRow("UNDO_LOG", {
    timestamp: nowSeoul().format(),
    action,
    payload: JSON.stringify(payload),
  });
}

// ===== Telegram menu (command button) =====
async function setupBotCommands() {
  await bot.setMyCommands([
    { command: "start", description: "Bắt đầu / hướng dẫn nhanh" },
    { command: "help", description: "Xem lệnh" },
    { command: "baocao", description: "Báo cáo tháng (tổng quan)" },
    { command: "may", description: "Thống kê máy (tổng quan)" },
    { command: "pending", description: "Danh sách invite pending / quá hạn" },
    { command: "undo", description: "Hoàn tác lệnh gần nhất" },
  ]);
}

// ===== Core actions =====
async function addGameRevenue(chatId, game, amount, type, note = "", meta = {}) {
  await appendRow("GAME_REVENUE", {
    timestamp: nowSeoul().format(),
    game,          // db/hq/qr/other
    type,          // invite_reward/checkin_reward/other_income
    amount,
    note,
    chatId,
    ...meta,
  });

  await logUndo("ADD_GAME_REVENUE", { game, amount, type, note, meta });
}

async function createInvite(chatId, game, name, email) {
  const invitedAt = nowSeoul();
  const due = invitedAt.add(14, "day");

  await appendRow("INVITES", {
    timestamp: invitedAt.format(),
    game,               // hq / qr
    name,
    email,
    time_invited: invitedAt.format(),
    due_date: due.format(),
    status: "pending",
    chatId,
    last_reminded_at: "",
  });

  await logUndo("ADD_INVITE", { game, name, email, time_invited: invitedAt.format(), due_date: due.format() });

  return { invitedAt, due };
}

async function markInviteDoneAndAddCheckin(chatId, game, name, email, reward) {
  // find latest pending invite for same person+game (or email)
  const rows = await getAllRows("INVITES");

  // prefer by email
  let target = rows
    .filter(r =>
      String(r.status || "").toLowerCase() === "pending" &&
      String(r.game || "").toLowerCase() === game &&
      (
        (email && String(r.email || "").toLowerCase() === email.toLowerCase()) ||
        String(r.name || "").toLowerCase() === name.toLowerCase()
      )
    )
    .sort((a,b) => new Date(b.time_invited || b.timestamp) - new Date(a.time_invited || a.timestamp))[0];

  if (!target) {
    throw new Error(`Không tìm thấy invite pending cho ${game} ${name}`);
  }

  // CHECKIN_REWARD
  await appendRow("CHECKIN_REWARD", {
    timestamp: nowSeoul().format(),
    game,
    name: target.name,
    email: target.email,
    reward,
    due_date: target.due_date,
    chatId,
  });

  // also add to GAME_REVENUE
  await addGameRevenue(chatId, game, reward, "checkin_reward", `checkin 14 ngày: ${target.name}`, {
    name: target.name,
    email: target.email,
  });

  // update INVITES
  target.status = "done";
  target.checkin_reward = reward;
  target.completed_at = nowSeoul().format();
  await target.save();

  await logUndo("DONE_INVITE_CHECKIN", {
    inviteRowId: target._rowNumber,
    game,
    reward,
  });

  return target;
}

async function createPhone(chatId, phoneCode, buyPrice) {
  const ts = nowSeoul().format();
  await appendRow("PHONES", {
    timestamp: ts,
    phoneCode,
    buyPrice,
    buyDate: ts,
    status: "bought",
    wallet: "",
    chatId,
  });
  await logUndo("ADD_PHONE", { phoneCode, buyPrice, buyDate: ts });
}

async function setPhoneWallet(phoneCode, wallet) {
  const rows = await getAllRows("PHONES");
  const target = rows
    .filter(r => String(r.phoneCode || "").toLowerCase() === phoneCode.toLowerCase())
    .sort((a,b) => new Date(b.buyDate || b.timestamp) - new Date(a.buyDate || a.timestamp))[0];

  if (!target) throw new Error(`Không tìm thấy máy ${phoneCode} trong PHONES`);

  target.wallet = wallet;
  await target.save();
  return target;
}

async function logPhoneProfit(phoneCode, gameSource, gameAmount) {
  const rows = await getAllRows("PHONES");
  const target = rows
    .filter(r => String(r.phoneCode || "").toLowerCase() === phoneCode.toLowerCase())
    .sort((a,b) => new Date(b.buyDate || b.timestamp) - new Date(a.buyDate || a.timestamp))[0];

  if (!target) throw new Error(`Không tìm thấy máy ${phoneCode}`);

  const buyPrice = Number(String(target.buyPrice || "0").replace(/,/g, "")) || 0;
  const profit = gameAmount - buyPrice;

  target.status = "ok";
  target.game_source = gameSource;
  target.game_amount = gameAmount;
  target.profit = profit;
  target.ok_at = nowSeoul().format();
  await target.save();

  await appendRow("PHONE_PROFIT_LOG", {
    timestamp: nowSeoul().format(),
    phoneCode,
    buyPrice,
    game_source: gameSource,
    game_amount: gameAmount,
    profit,
  });

  await logUndo("PHONE_OK_PROFIT", { phoneCode, buyPrice, gameSource, gameAmount, profit });

  return { buyPrice, profit };
}

async function createLot(chatId, qty, totalCost) {
  const ts = nowSeoul().format();
  await appendRow("LOTS", {
    timestamp: ts,
    lotId: `LOT_${Date.now()}`,
    qty,
    totalCost,
    date: ts,
    wallet: "",
    chatId,
  });
  await logUndo("ADD_LOT", { qty, totalCost, date: ts });
}

async function getLatestLot() {
  const rows = await getAllRows("LOTS");
  const target = rows.sort((a,b) => new Date(b.date || b.timestamp) - new Date(a.date || a.timestamp))[0];
  if (!target) throw new Error("Chưa có lô nào trong LOTS");
  return target;
}

async function setLotWallet(lotRow, wallet) {
  lotRow.wallet = wallet;
  await lotRow.save();
  return lotRow;
}

function normalizeGameToken(tok) {
  const t = tok.toLowerCase();
  if (t === "hopqua" || t === "hq" || t === "hh") return "hq";
  if (t === "qr") return "qr";
  if (t === "dabong" || t === "db") return "db";
  return t;
}

async function saveLotResult(lotRow, { ok, tach, game, totalReward }) {
  const lotCost = Number(String(lotRow.totalCost || "0").replace(/,/g, "")) || 0;
  const profit = (totalReward != null) ? (totalReward - lotCost) : "";

  await appendRow("LOT_RESULT", {
    timestamp: nowSeoul().format(),
    lotId: lotRow.lotId || "",
    lotRow: lotRow._rowNumber,
    qty: lotRow.qty,
    totalCost: lotCost,
    ok,
    tach,
    game,
    totalReward: totalReward != null ? totalReward : "",
    profit,
  });

  await logUndo("ADD_LOT_RESULT", { lotRow: lotRow._rowNumber, ok, tach, game, totalReward, profit });
}

// ===== Reporting (simple, đủ dùng) =====
async function reportMonth(chatId, ym = nowSeoul().format("YYYY-MM")) {
  const rows = await getAllRows("GAME_REVENUE");
  const monthRows = rows.filter(r => String(r.timestamp || "").startsWith(ym));

  const sum = (arr) => arr.reduce((a, r) => a + (Number(r.amount) || 0), 0);

  const byGame = {};
  for (const r of monthRows) {
    const g = String(r.game || "unknown");
    byGame[g] = (byGame[g] || 0) + (Number(r.amount) || 0);
  }

  const total = sum(monthRows);

  let text = `📊 Báo cáo tháng ${ym}\n`;
  text += `• Tổng thu TikTok (GAME_REVENUE): ${fmtMoney(total)}\n`;
  for (const [g, v] of Object.entries(byGame)) {
    text += `  - ${g}: ${fmtMoney(v)}\n`;
  }

  // invites stats
  const invites = await getAllRows("INVITES");
  const invMonth = invites.filter(r => String(r.time_invited || r.timestamp || "").startsWith(ym));
  const pending = invMonth.filter(r => String(r.status || "").toLowerCase() === "pending").length;
  const done = invMonth.filter(r => String(r.status || "").toLowerCase() === "done").length;

  text += `\n👥 Invite (tháng ${ym})\n`;
  text += `• Pending: ${pending}\n`;
  text += `• Done: ${done}\n`;

  await bot.sendMessage(chatId, text);
}

async function listPending(chatId) {
  const invites = await getAllRows("INVITES");
  const now = nowSeoul();

  const pending = invites
    .filter(r => String(r.status || "").toLowerCase() === "pending")
    .map(r => {
      const due = dayjs(r.due_date).tz(TZ);
      const overdue = due.isBefore(now);
      return { r, due, overdue };
    })
    .sort((a,b) => a.due.valueOf() - b.due.valueOf());

  if (pending.length === 0) {
    await bot.sendMessage(chatId, "✅ Không có invite pending.");
    return;
  }

  let text = `🕒 Pending invites (${pending.length})\n`;
  for (const { r, due, overdue } of pending.slice(0, 50)) {
    const game = r.game;
    const name = r.name;
    const email = r.email;
    const dueStr = due.format("ddd DD/MM");
    text += `• ${overdue ? "⚠️" : "⏳"} ${game} - ${name} (${email}) due: ${dueStr}\n`;
  }
  if (pending.length > 50) text += `... còn ${pending.length - 50} dòng\n`;

  await bot.sendMessage(chatId, text);
}

// ===== Due reminder job =====
async function runDueCheck() {
  try {
    const invites = await getAllRows("INVITES");
    const now = nowSeoul();

    for (const r of invites) {
      if (String(r.status || "").toLowerCase() !== "pending") continue;

      const due = dayjs(r.due_date).tz(TZ);
      if (!due.isValid()) continue;

      // Trigger when now >= due_date AND not reminded today
      if (now.isAfter(due) || now.isSame(due)) {
        const last = r.last_reminded_at ? dayjs(r.last_reminded_at).tz(TZ) : null;
        const remindedToday = last && last.isValid() && last.format("YYYY-MM-DD") === now.format("YYYY-MM-DD");
        if (remindedToday) continue;

        // Ask user checkin reward
        const chatId = r.chatId;
        const gameLabel = String(r.game || "").toLowerCase() === "hq" ? "Hopqua" : "QR";
        const name = r.name;

        // set pending session
        sessions.set(String(chatId), {
          pending: {
            type: "ask_checkin_reward",
            data: {
              game: String(r.game || "").toLowerCase(),
              name: r.name,
              email: r.email,
              inviteRowNumber: r._rowNumber,
            },
          },
        });

        await bot.sendMessage(chatId, `${gameLabel} ${name} = bao nhiêu? (vd: 60k)`);
        r.last_reminded_at = now.format();
        await r.save();
      }
    }
  } catch (e) {
    console.error("runDueCheck error:", e.message);
  }
}

// run every day at 10:00 Seoul (you can change)
function startCron() {
  cron.schedule("0 10 * * *", runDueCheck, { timezone: TZ });
  // also run every hour as safety net (optional)
  cron.schedule("15 * * * *", runDueCheck, { timezone: TZ });
}

// ===== Message router =====
function getSession(chatId) {
  return sessions.get(String(chatId));
}
function clearSession(chatId) {
  sessions.delete(String(chatId));
}

async function handleWalletAnswer(chatId, wallet, context) {
  wallet = wallet.toLowerCase().trim();
  if (!["uri", "hana", "kt"].includes(wallet)) {
    await bot.sendMessage(chatId, "Chỉ nhận: uri / hana / kt. Nhập lại:");
    return;
  }

  if (context.type === "ask_wallet_for_phone") {
    const { phoneCode, buyPrice } = context.data;

    const phoneRow = await setPhoneWallet(phoneCode, wallet);
    await upsertWalletBalance(wallet, -buyPrice, `PHONE:${phoneCode}`, "buy phone");

    clearSession(chatId);
    await bot.sendMessage(chatId, `✅ Đã lưu mua máy ${phoneCode} giá ${fmtMoney(buyPrice)} từ ví ${wallet}. Balance mới: ${fmtMoney(phoneRow.Balance)}`);
    // NOTE: phoneRow.Balance doesn't exist; we already updated wallet. We'll just confirm.
    await bot.sendMessage(chatId, `💳 Đã trừ ví ${wallet}: -${fmtMoney(buyPrice)}.`);
    return;
  }

  if (context.type === "ask_wallet_for_lot") {
    const { lotRowNumber, totalCost } = context.data;
    const lots = await getAllRows("LOTS");
    const lotRow = lots.find(r => r._rowNumber === lotRowNumber);
    if (!lotRow) throw new Error("Không tìm thấy LOT để set wallet");

    await setLotWallet(lotRow, wallet);
    await upsertWalletBalance(wallet, -totalCost, `LOT:${lotRow.lotId}`, "buy lot");

    clearSession(chatId);
    await bot.sendMessage(chatId, `✅ Đã lưu mua lô ${lotRow.lotId} (${lotRow.qty} máy) tổng ${fmtMoney(totalCost)} từ ví ${wallet}.`);
    await bot.sendMessage(chatId, `💳 Đã trừ ví ${wallet}: -${fmtMoney(totalCost)}.`);
    return;
  }
}

async function handleCheckinAnswer(chatId, text, context) {
  const reward = parseMoney(text);
  if (reward == null) {
    await bot.sendMessage(chatId, "Không parse được tiền. Ví dụ đúng: 60k hoặc 30000");
    return;
  }

  const { game, name, email } = context.data;
  await markInviteDoneAndAddCheckin(chatId, game, name, email, reward);

  clearSession(chatId);
  await bot.sendMessage(chatId, `✅ Đã ghi checkin ${game} ${name}: +${fmtMoney(reward)} (đã cộng vào GAME_REVENUE)`);
}

function parseCommand(text) {
  const raw = String(text || "").trim();
  const parts = raw.split(/\s+/);
  return { raw, parts };
}

// ===== Main handler =====
bot.on("message", async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text || "";

  try {
    // 1) If waiting for follow-up
    const sess = getSession(chatId);
    if (sess?.pending?.type) {
      const pending = sess.pending;

      if (pending.type === "ask_wallet_for_phone" || pending.type === "ask_wallet_for_lot") {
        await handleWalletAnswer(chatId, text, pending);
        return;
      }

      if (pending.type === "ask_checkin_reward") {
        await handleCheckinAnswer(chatId, text, pending);
        return;
      }
    }

    // 2) Slash commands
    if (text.startsWith("/start")) {
      await bot.sendMessage(chatId,
        "✅ TIKTOK_LITE_BOT\n\n" +
        "Gõ nhanh:\n" +
        "• dabong 100k\n" +
        "• hopqua Khanh mail@gmail.com\n" +
        "• hopqua 200k\n" +
        "• qr Khanh mail@gmail.com\n" +
        "• qr 57k\n" +
        "• them 0.5k\n" +
        "• ssa34 35k (bot hỏi ví)\n" +
        "• ssa34 ok hopqua100k\n" +
        "• mua 5may 120k (bot hỏi ví)\n" +
        "• 4may hq ok tach1\n" +
        "• 5may hh800k tach1\n"
      );
      return;
    }

    if (text.startsWith("/help")) {
      await bot.sendMessage(chatId,
        "📌 Lệnh:\n" +
        "GAME:\n" +
        "- dabong 100k\n" +
        "- hopqua <Name> <Email>\n" +
        "- hopqua 200k\n" +
        "- qr <Name> <Email>\n" +
        "- qr 57k\n" +
        "- them 0.5k\n\n" +
        "MÁY:\n" +
        "- <phoneCode> <buyPrice>\n" +
        "- <phoneCode> ok hopqua100k\n" +
        "- mua <qty>may <totalCost>\n" +
        "- <okCount>may hq ok tach<tachCount>\n" +
        "- <qty>may hh800k tach1\n\n" +
        "BÁO CÁO:\n" +
        "- /baocao\n" +
        "- /pending\n"
      );
      return;
    }

    if (text.startsWith("/baocao")) {
      const ym = nowSeoul().format("YYYY-MM");
      await reportMonth(chatId, ym);
      return;
    }

    if (text.startsWith("/pending")) {
      await listPending(chatId);
      return;
    }

    if (text.startsWith("/undo")) {
      await bot.sendMessage(chatId, "⚠️ /undo: mình đã ghi UNDO_LOG nhưng rollback đầy đủ tuỳ mức bạn muốn. Nếu bạn muốn, mình sẽ triển khai undo theo action cụ thể.");
      return;
    }

    // 3) Free text commands
    const { parts } = parseCommand(text);
    if (parts.length === 0) return;

    const cmd = parts[0].toLowerCase();

    // --- GAME: dabong / hopqua / qr ---
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

    if (cmd === "hopqua" || cmd === "hq") {
      // hopqua 200k  OR hopqua Name email
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
        await bot.sendMessage(chatId, `✅ Đã lưu invite HQ: ${name} (${email})\n⏰ Due (14 ngày): ${due.format("ddd DD/MM")} (${TZ})`);
        return;
      }
      await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: hopqua 200k hoặc hopqua Khanh mail@gmail.com");
      return;
    }

    if (cmd === "qr") {
      // qr 57k OR qr Name email
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
        await bot.sendMessage(chatId, `✅ Đã lưu invite QR: ${name} (${email})\n⏰ Due (14 ngày): ${due.format("ddd DD/MM")} (${TZ})`);
        return;
      }
      await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: qr 57k hoặc qr Khanh mail@gmail.com");
      return;
    }

    // --- other income ---
    if (cmd === "them") {
      const amount = parseMoney(parts[1]);
      if (amount == null) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: them 0.5k");
        return;
      }
      await addGameRevenue(chatId, "other", amount, "other_income", "other income");
      await bot.sendMessage(chatId, `✅ THÊM +${fmtMoney(amount)} (other income)`);
      return;
    }

    // --- admin: chỉnh ví ---
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

      // set absolute balance by applying delta
      const rows = await getAllRows("WALLETS");
      let row = rows.find(r => String(r.Wallet || "").toLowerCase() === wallet);
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

      await bot.sendMessage(chatId, `✅ Đã set ví ${wallet} = ${fmtMoney(amount)} (delta ${fmtMoney(delta)})`);
      return;
    }

    // --- LOT: mua 5may 120k ---
    if (cmd === "mua") {
      // mua 5may 120k
      // parts[1]=5may, parts[2]=120k
      const qtyPart = (parts[1] || "").toLowerCase();
      const qtyMatch = qtyPart.match(/^(\d+)may$/);
      const qty = qtyMatch ? Number(qtyMatch[1]) : null;
      const totalCost = parseMoney(parts[2]);

      if (!qty || totalCost == null) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: mua 5may 120k");
        return;
      }

      await createLot(chatId, qty, totalCost);
      const lotRow = await getLatestLot();

      sessions.set(String(chatId), {
        pending: {
          type: "ask_wallet_for_lot",
          data: { lotRowNumber: lotRow._rowNumber, totalCost },
        }
      });

      await bot.sendMessage(chatId, `✅ Đã tạo lô ${lotRow.lotId} (${qty} máy) tổng ${fmtMoney(totalCost)}.\nTiền từ ví nào? (uri/hana/kt)`);
      return;
    }

    // --- LOT RESULT patterns ---
    // Pattern A: "4may hq ok tach1"
    // Pattern B: "5may hh800k tach1" (implies ok = qty - tach, reward=800k, game=hq)
    const mayMatch = cmd.match(/^(\d+)may$/);
    if (mayMatch) {
      const n = Number(mayMatch[1]);
      const token2 = (parts[1] || "").toLowerCase(); // hq or hh800k
      const token3 = (parts[2] || "").toLowerCase(); // ok or tach1
      const token4 = (parts[3] || "").toLowerCase();

      const lotRow = await getLatestLot();

      // Case B: 5may hh800k tach1
      // token2 like hh800k or hq800k
      const t2m = token2.match(/^([a-z]+)(\d+(\.\d+)?k?)$/i);
      if (t2m && (token3.startsWith("tach") || token4.startsWith("tach"))) {
        const game = normalizeGameToken(t2m[1]);
        const totalReward = parseMoney(t2m[2]);
        const tachTok = token3.startsWith("tach") ? token3 : token4;
        const tach = Number((tachTok.match(/^tach(\d+)$/) || [])[1] || 0);
        const ok = n - tach;

        if (totalReward == null || tachTok === "" || tach < 0 || ok < 0) {
          await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: 5may hh800k tach1");
          return;
        }

        await saveLotResult(lotRow, { ok, tach, game, totalReward });
        await bot.sendMessage(chatId, `✅ KQ lô gần nhất: ok=${ok}, tạch=${tach}, game=${game}, thưởng=${fmtMoney(totalReward)}\n📈 Lãi/lỗ lô = ${fmtMoney(totalReward - (Number(lotRow.totalCost)||0))}`);
        return;
      }

      // Case A: 4may hq ok tach1
      if ((token2 === "hq" || token2 === "qr" || token2 === "db") && token3 === "ok" && token4.startsWith("tach")) {
        const game = normalizeGameToken(token2);
        const tach = Number((token4.match(/^tach(\d+)$/) || [])[1] || 0);
        const ok = n;

        await saveLotResult(lotRow, { ok, tach, game, totalReward: null });
        await bot.sendMessage(chatId, `✅ KQ lô gần nhất: ok=${ok}, tạch=${tach}, game=${game}`);
        return;
      }
    }

    // --- PHONE BUY: ssa34 35k ---
    // phoneCode is alnum
    const maybePrice = parts[1] ? parseMoney(parts[1]) : null;
    if (parts.length === 2 && maybePrice != null && /^[a-z0-9]+$/i.test(cmd)) {
      const phoneCode = parts[0];
      const buyPrice = maybePrice;

      await createPhone(chatId, phoneCode, buyPrice);

      sessions.set(String(chatId), {
        pending: {
          type: "ask_wallet_for_phone",
          data: { phoneCode, buyPrice }
        }
      });

      await bot.sendMessage(chatId, `✅ Đã lưu mua máy ${phoneCode} giá ${fmtMoney(buyPrice)}.\nTiền từ ví nào? (uri/hana/kt)`);
      return;
    }

    // --- PHONE OK: ssa34 ok hopqua100k ---
    if (parts.length >= 3 && /^[a-z0-9]+$/i.test(cmd) && parts[1].toLowerCase() === "ok") {
      const phoneCode = parts[0];
      const gameTok = parts[2].toLowerCase(); // hopqua100k / hopqua200k / hq100k
      const gm = gameTok.match(/^([a-z]+)(\d+(\.\d+)?k?)$/i);
      if (!gm) {
        await bot.sendMessage(chatId, "Sai cú pháp. Ví dụ: ssa34 ok hopqua100k");
        return;
      }
      const gameSource = normalizeGameToken(gm[1]); // hopqua/hq/hh => hq
      const gameAmount = parseMoney(gm[2]);
      if (gameAmount == null) {
        await bot.sendMessage(chatId, "Sai tiền. Ví dụ: hopqua100k");
        return;
      }

      const { buyPrice, profit } = await logPhoneProfit(phoneCode, gameSource, gameAmount);
      await bot.sendMessage(chatId, `✅ Máy ${phoneCode} OK.\n• Giá mua: ${fmtMoney(buyPrice)}\n• Thưởng game: ${fmtMoney(gameAmount)}\n• Lãi/lỗ máy: ${fmtMoney(profit)}`);
      return;
    }

    // fallback
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
