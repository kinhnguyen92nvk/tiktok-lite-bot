/**
 * ============================================================
 * KIM BOT – SỔ KIM THU HOẠCH RONG BIỂN
 * VERSION: KIM-SO-KIM-v2.0-FINAL-2025-12-15
 *
 * ✅ FINAL REQUIREMENTS (CHỐT):
 * 1) Reply keyboard "menu box" Telegram: luôn hiện, bấm là chạy.
 * 2) Parsing:
 *    - token đầu: Bãi (A27/A14/34/...)
 *    - ...b bắt buộc, ...k bắt buộc
 *    - ...g optional:
 *        + nếu thiếu => CẮT SẠCH (progress = max)
 *        + nếu có => CẮT DỠ theo số g (delta) và CỘNG DỒN progress
 *        + nếu progress đạt max => tự thành CẮT SẠCH
 *    - ...d optional: ngày trong tháng (dd) => ghi bù ngày dd/tháng hiện tại
 *      nếu thiếu => mặc định HÔM QUA
 *    - "note:" optional => ghi cột Note
 *    - "nghỉ gió" / "làm bờ" => ghi tình hình, doanh thu = 0
 *
 * 3) Vòng (Cycle):
 *    - vòng chỉ tăng khi có CẮT SẠCH
 *    - mọi dòng trong chu kỳ hiện tại thuộc Vòng (cleanCount + 1)
 *    - "cắt dỡ" thuộc vòng hiện tại (KHÔNG nhảy vòng)
 *
 * 4) Output:
 *    --- 🌊 SỔ KIM (Vòng: X) ---
 *    Chào <Tên>, đây là kết quả của lệnh bạn gửi
 *    ... (đúng format)
 *
 * 5) Delete:
 *    - Không cần admin
 *    - Bấm nút "Xóa ..." => Bot yêu cầu nhập 2525
 *    - Nhập 2525 => thực hiện
 *
 * 6) Lịch cắt: theo lần CẮT SẠCH gần nhất + CUT_INTERVAL_DAYS
 *    - Sort từ ngày gần nhất -> xa nhất
 *
 * ============================================================
 */

import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";

/* ================== APP ================== */
const app = express();
app.use(express.json());

const VERSION = "KIM-SO-KIM-v2.0-FINAL-2025-12-15";
console.log("🚀 RUNNING:", VERSION);

/* ================== ENV ================== */
const BOT_TOKEN = process.env.BOT_TOKEN;
const TELEGRAM_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  "/etc/secrets/google-service-account.json";

const CUT_INTERVAL_DAYS = Number(process.env.CUT_INTERVAL_DAYS || 15);
const BAO_RATE = 1.7;

const CONFIRM_CODE = "2525"; // ✅ chốt mã xóa

/* ================== CONFIG (MAX DÂY CHỐT) ================== */
const MAX_DAY = {
  A14: 69,
  A27: 60,
  A22: 60,
  "34": 109, // bãi lớn
  B17: 69,
  B24: 69,
  C11: 59,
  C12: 59,
};

/* ================== BASIC ROUTES ================== */
app.get("/", (_, res) => res.send("KIM BOT OK"));
app.get("/ping", (_, res) => res.json({ ok: true, version: VERSION }));

/* ================== GOOGLE SHEETS ================== */
const auth = new google.auth.GoogleAuth({
  keyFile: GOOGLE_APPLICATION_CREDENTIALS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

async function getRows() {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: "DATA!A2:L",
  });
  return r.data.values || [];
}

async function appendRow(row12) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: "DATA!A1",
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [row12] },
  });
}

async function updateRow(rowNumber1Based, rowValues12) {
  const range = `DATA!A${rowNumber1Based}:L${rowNumber1Based}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId: GOOGLE_SHEET_ID,
    range,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [rowValues12] },
  });
}

async function clearRow(rowNumber1Based) {
  const range = `DATA!A${rowNumber1Based}:L${rowNumber1Based}`;
  await sheets.spreadsheets.values.clear({
    spreadsheetId: GOOGLE_SHEET_ID,
    range,
  });
}

async function clearAllData() {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: "DATA!A2:L",
  });
}

/* ================== TELEGRAM HELPERS ================== */
async function tg(method, payload) {
  const resp = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return resp.json().catch(() => ({}));
}

async function send(chatId, text, extra = {}) {
  await tg("sendMessage", { chat_id: chatId, text, ...extra });
}

/**
 * ✅ Reply Keyboard = “hộp menu Telegram”
 * Luôn hiển thị dưới khung chat (không cần gõ menu).
 */
function buildMainKeyboard() {
  return {
    keyboard: [
      [{ text: "📅 Thống kê tháng này" }, { text: "🔁 Thống kê theo VÒNG" }],
      [{ text: "📍 Thống kê theo BÃI" }, { text: "📆 Lịch cắt các bãi" }],
      [{ text: "📋 Danh sách lệnh đã gửi" }],
      [{ text: "✏️ Sửa dòng gần nhất" }, { text: "🗑️ Xóa dòng gần nhất" }],
      [{ text: "⚠️ XÓA SẠCH DỮ LIỆU" }],
    ],
    resize_keyboard: true,
    one_time_keyboard: false,
    is_persistent: true,
  };
}

/** Gắn keyboard cho chat (gọi mỗi lần bot trả lời cũng được) */
async function ensureKeyboard(chatId) {
  await send(chatId, "✅ Menu đã sẵn sàng.", {
    reply_markup: buildMainKeyboard(),
  });
}

/* ================== TIME (KST) ================== */
function kst(d = new Date()) {
  return new Date(d.getTime() + 9 * 3600 * 1000);
}

function fmtDayVN(d) {
  const days = [
    "Chủ Nhật",
    "Thứ Hai",
    "Thứ Ba",
    "Thứ Tư",
    "Thứ Năm",
    "Thứ Sáu",
    "Thứ Bảy",
  ];
  return `${days[d.getDay()]}, ${String(d.getDate()).padStart(2, "0")}/${String(
    d.getMonth() + 1
  ).padStart(2, "0")}`;
}

function ymd(d) {
  // d đã là KST date
  return d.toISOString().slice(0, 10);
}

function moneyToTrieu(won) {
  // 50,000,000 => 50 triệu
  return `${Math.round(Number(won || 0) / 1_000_000)} triệu`;
}

/* ================== PARSE INPUT ================== */
function parseWorkLine(text) {
  const raw = (text || "").trim();
  if (!raw) return null;

  const lower = raw.toLowerCase().trim();

  // nghỉ gió / làm bờ
  if (lower.includes("nghỉ gió") || lower.includes("làm bờ") || lower.includes("lam bo")) {
    return { type: "NO_WORK", tinhHinh: lower.includes("nghỉ gió") ? "Nghỉ gió" : "Làm bờ" };
  }

  const parts = raw.split(/\s+/);
  const bai = (parts[0] || "").toUpperCase();
  if (!bai || !MAX_DAY[bai]) return null;

  let g = null; // delta g nếu có
  let b = null;
  let k = null;
  let d = null;
  let note = "";

  // note:
  const noteIdx = parts.findIndex((p) => p.toLowerCase().startsWith("note:"));
  if (noteIdx >= 0) {
    note = parts
      .slice(noteIdx)
      .join(" ")
      .replace(/^note:\s*/i, "")
      .trim();
  }

  for (const p of parts) {
    if (/^\d+g$/i.test(p)) g = Number(p.slice(0, -1));
    if (/^\d+b$/i.test(p)) b = Number(p.slice(0, -1));
    if (/^\d+k$/i.test(p)) k = Number(p.slice(0, -1));
    if (/^\d+d$/i.test(p)) d = Number(p.slice(0, -1));
  }

  if (!b || !k) return null;

  // g thiếu => hiểu là CẮT SẠCH (progress = max)
  return { type: "WORK", bai, gDelta: g, b, k, dayInMonth: d, note };
}

function baoChuan(baoTau) {
  return Math.round(Number(baoTau || 0) * BAO_RATE);
}

/* ================== DATA MODEL (A-L) ==================
A Timestamp
B Date (YYYY-MM-DD)
C Thu (Name)
D ViTri (Bai)
E DayG (progressG sau lệnh)  ✅ QUAN TRỌNG: là TIẾN ĐỘ CỘNG DỒN, không phải delta
F MaxG
G TinhHinh ("Cắt sạch" / "Cắt dỡ" / "Nghỉ gió" / "Làm bờ")
H BaoTau
I BaoChuan
J GiaK
K Won
L Note
====================================================== */

function rowToObj(r) {
  return {
    ts: r?.[0] || "",
    date: r?.[1] || "",
    thu: r?.[2] || "",
    bai: r?.[3] || "",
    dayG: Number(r?.[4] || 0),
    maxG: Number(r?.[5] || 0),
    tinhHinh: r?.[6] || "",
    baoTau: Number(r?.[7] || 0),
    baoChuan: Number(r?.[8] || 0),
    giaK: Number(r?.[9] || 0),
    won: Number(r?.[10] || 0),
    note: r?.[11] || "",
  };
}

/* ================== HELPERS: SORT / SEARCH ================== */
function sortByDateTs(objs) {
  // stable: date then ts
  return [...objs].sort((a, b) => (a.date + a.ts).localeCompare(b.date + b.ts));
}

function isWorkRow(o) {
  return !!o.bai && o.maxG > 0;
}

function isCleanRow(o) {
  return isWorkRow(o) && Number(o.dayG) === Number(o.maxG);
}

/**
 * Lấy trạng thái bãi:
 * - cleanDone: số lần cắt sạch đã hoàn thành
 * - progress: tiến độ hiện tại trong vòng (0..max)
 * - lastCleanDate: ngày cắt sạch gần nhất
 */
function computeBaiState(allObjs, bai) {
  const max = MAX_DAY[bai] || 0;

  const sorted = sortByDateTs(allObjs).filter((o) => o.bai === bai);
  let cleanDone = 0;
  let progress = 0;
  let lastCleanDate = "";

  for (const o of sorted) {
    // chỉ tính dòng work của bãi
    if (!isWorkRow(o)) continue;

    // nếu clean => đóng vòng, reset progress
    if (Number(o.dayG) >= max && max > 0) {
      cleanDone += 1;
      progress = 0; // reset sau khi sạch
      lastCleanDate = o.date || lastCleanDate;
    } else {
      // cắt dỡ: progress là tiến độ đã lưu ở cột dayG
      progress = Math.min(Number(o.dayG || 0), max);
    }
  }

  const currentVong = Math.max(1, cleanDone + 1);

  return { bai, max, cleanDone, currentVong, progress, lastCleanDate };
}

/**
 * Gán vòng cho từng dòng (toàn bộ DATA):
 * - vòng của một dòng = cleanCountBefore + 1
 * - cleanCountBefore tăng khi gặp dòng CẮT SẠCH
 * - cắt dỡ vẫn thuộc vòng hiện tại (không nhảy vòng)
 */
function assignVongAll(objs) {
  const sorted = sortByDateTs(objs);
  const doneMap = new Map(); // bai -> cleanDone
  const out = [];

  for (const o of sorted) {
    if (!isWorkRow(o)) {
      out.push({ ...o, vong: 0 });
      continue;
    }

    const bai = o.bai;
    const max = MAX_DAY[bai] || o.maxG || 0;
    const done = doneMap.get(bai) || 0;

    const vong = Math.max(1, done + 1);

    // nếu dòng này là clean => sau dòng này tăng done
    const clean = max > 0 && Number(o.dayG) >= Number(max);

    out.push({ ...o, vong, isClean: clean });

    if (clean) doneMap.set(bai, done + 1);
  }

  return out;
}

/* ================== FORECAST ================== */
function addDaysYmd(ymdStr, days) {
  if (!ymdStr) return "";
  const d = new Date(`${ymdStr}T00:00:00`);
  const next = new Date(d.getTime() + Number(days) * 86400000);
  const dd = String(next.getDate()).padStart(2, "0");
  const mm = String(next.getMonth() + 1).padStart(2, "0");
  const yyyy = next.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function forecastForBai(state) {
  if (!state?.lastCleanDate) return ""; // chưa có sạch
  return addDaysYmd(state.lastCleanDate, CUT_INTERVAL_DAYS);
}

/* ================== OUTPUT TEMPLATE ================== */
function buildSaiCuPhapText() {
  return (
    "❌ Nhập sai rồi bạn iu ơi 😅\n" +
    "Ví dụ:\n" +
    "A27 60b 220k\n" +
    "A27 30g 40b 220k\n" +
    "A27 80b 120k 5d"
  );
}

async function sendSoKim({
  chatId,
  userName,
  vong,
  dateYmd,
  bai,
  progressG,
  maxG,
  tinhHinh,
  baoTau,
  baoChuanX,
  giaK,
  won,
  totalToNow,
  forecast,
}) {
  const dateObj = new Date(`${dateYmd}T00:00:00`);

  const text =
`--- 🌊 SỔ KIM (Vòng: ${vong}) ---
Chào ${userName}, đây là kết quả của lệnh bạn gửi

📅 Ngày: ${fmtDayVN(dateObj)}
📍 Vị trí: ${bai}
✂️ Tình hình: ${tinhHinh} (${progressG}/${maxG} dây)
📦 Sản lượng: ${baoTau} bao lớn (≈ ${baoChuanX} bao tính tiền)
💰 Giá: ${giaK}k

💵 THU HÔM NAY: ${Number(won).toLocaleString()} ₩
🏆 TỔNG THU TỚI THỜI ĐIỂM NÀY: ${moneyToTrieu(totalToNow)} ₩
----------------------------------
${forecast ? `(Dự báo nhanh: Bãi này sẽ cắt lại vào ${forecast})` : ""}`.trim();

  await send(chatId, text, { reply_markup: buildMainKeyboard() });
}

/* ================== CONFIRM DELETE STATE (2525) ================== */
const pendingConfirm = new Map();
/**
 * pendingConfirm.set(chatId, { action: "RESET"|"DEL_LAST", expiresAt })
 */
function setPending(chatId, action) {
  const expiresAt = Date.now() + 5 * 60 * 1000; // 5 phút
  pendingConfirm.set(String(chatId), { action, expiresAt });
}
function getPending(chatId) {
  const p = pendingConfirm.get(String(chatId));
  if (!p) return null;
  if (Date.now() > p.expiresAt) {
    pendingConfirm.delete(String(chatId));
    return null;
  }
  return p;
}
function clearPending(chatId) {
  pendingConfirm.delete(String(chatId));
}

/* ================== FIND / EDIT / DELETE ================== */
function findLastRowIndexAny(rows) {
  for (let i = rows.length - 1; i >= 0; i--) {
    const o = rowToObj(rows[i]);
    if (o.ts || o.date || o.thu || o.bai || o.tinhHinh) return 2 + i;
  }
  return null;
}

function findLastWorkRowIndexForUserAndBai(rows, userName, bai) {
  for (let i = rows.length - 1; i >= 0; i--) {
    const o = rowToObj(rows[i]);
    if (o.thu === userName && o.bai === bai && isWorkRow(o)) return 2 + i;
  }
  return null;
}

/* ================== MENU ACTIONS ================== */
function currentMonthKeyKST() {
  const now = kst();
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}
function rowMonthKey(o) {
  if (!o?.date || o.date.length < 7) return "";
  return o.date.slice(0, 7);
}

async function reportMonth(chatId) {
  const rows = await getRows();
  const objs = rows.map(rowToObj);
  const monthKey = currentMonthKeyKST();

  const workDays = new Set();
  const windDays = new Set();
  const shoreDays = new Set();
  let totalWon = 0;

  for (const o of objs) {
    if (rowMonthKey(o) !== monthKey) continue;

    if (o.won > 0) {
      workDays.add(o.date);
      totalWon += o.won;
    } else {
      const t = (o.tinhHinh || "").toLowerCase();
      if (t.includes("nghỉ gió")) windDays.add(o.date);
      if (t.includes("làm bờ") || t.includes("lam bo")) shoreDays.add(o.date);
    }
  }

  const text =
`📅 THỐNG KÊ THÁNG ${monthKey}
• Số ngày làm: ${workDays.size}
• Nghỉ gió: ${windDays.size} ngày
• Làm bờ: ${shoreDays.size} ngày
• Tổng doanh thu tháng: ${Number(totalWon).toLocaleString()} ₩`.trim();

  await send(chatId, text, { reply_markup: buildMainKeyboard() });
}

async function reportByBai(chatId) {
  const rows = await getRows();
  const objs = rows.map(rowToObj);

  const map = new Map(); // bai -> agg
  for (const o of objs) {
    if (!isWorkRow(o)) continue;
    const cur = map.get(o.bai) || { baoTau: 0, baoChuan: 0, won: 0, lastCleanDate: "" };
    cur.baoTau += o.baoTau || 0;
    cur.baoChuan += o.baoChuan || 0;
    cur.won += o.won || 0;
    if (isCleanRow(o)) cur.lastCleanDate = o.date || cur.lastCleanDate;
    map.set(o.bai, cur);
  }

  const items = [...map.entries()].sort((a, b) => (b[1].won || 0) - (a[1].won || 0));

  let out = "📍 THỐNG KÊ THEO BÃI (tổng từ DATA)\n";
  for (const [bai, v] of items) {
    const forecast = v.lastCleanDate ? addDaysYmd(v.lastCleanDate, CUT_INTERVAL_DAYS) : "";
    out += `\n• ${bai}: ${v.baoTau} bao | ≈ ${v.baoChuan} chuẩn | ${Number(v.won).toLocaleString()} ₩`;
    if (forecast) out += `\n  ⤷ Dự báo cắt lại: ${forecast}`;
  }

  await send(chatId, out.trim(), { reply_markup: buildMainKeyboard() });
}

/**
 * ✅ THỐNG KÊ THEO VÒNG:
 * - Vòng của mỗi dòng = cleanDoneBefore + 1
 * - Cộng tiền theo Vòng, bao gồm cả "cắt dỡ" (đúng chốt mới)
 */
async function reportByVong(chatId) {
  const rows = await getRows();
  const objs = rows.map(rowToObj);
  const withV = assignVongAll(objs);

  const sumByV = new Map(); // vong -> won
  const sumByBaiV = new Map(); // bai|vong -> won

  for (const o of withV) {
    if (!isWorkRow(o) || o.vong <= 0) continue;

    sumByV.set(o.vong, (sumByV.get(o.vong) || 0) + (o.won || 0));

    const key = `${o.bai}|${o.vong}`;
    sumByBaiV.set(key, (sumByBaiV.get(key) || 0) + (o.won || 0));
  }

  const vongs = [...sumByV.entries()].sort((a, b) => a[0] - b[0]).slice(0, 50);

  let out = "🔁 THỐNG KÊ THEO VÒNG (cộng tất cả lệnh thuộc vòng của mỗi bãi)\n";
  if (!vongs.length) out += "\n(Chưa có dữ liệu)";
  for (const [v, won] of vongs) {
    out += `\n• Vòng ${v}: ${Number(won).toLocaleString()} ₩`;
  }

  out += "\n\nTheo từng bãi:";
  const list = [...sumByBaiV.entries()]
    .map(([k, won]) => {
      const [bai, v] = k.split("|");
      return { bai, vong: Number(v), won };
    })
    .sort((a, b) => (a.bai + a.vong).localeCompare(b.bai + b.vong));

  if (!list.length) out += "\n(Chưa có dữ liệu)";
  for (const it of list) {
    out += `\n- ${it.bai}: V${it.vong}: ${Number(it.won).toLocaleString()} ₩`;
  }

  await send(chatId, out.trim(), { reply_markup: buildMainKeyboard() });
}

/**
 * 📆 LỊCH CẮT CÁC BÃI:
 * - theo lần CẮT SẠCH gần nhất + CUT_INTERVAL_DAYS
 * - sort ngày gần -> xa
 */
async function reportCutSchedule(chatId) {
  const rows = await getRows();
  const objs = rows.map(rowToObj);

  const items = [];
  for (const bai of Object.keys(MAX_DAY)) {
    const st = computeBaiState(objs, bai);
    const forecast = forecastForBai(st); // dd/mm/yyyy hoặc ""
    if (!forecast) {
      items.push({ bai, forecast: "", sortKey: Infinity });
    } else {
      // parse dd/mm/yyyy to epoch for sorting
      const [dd, mm, yyyy] = forecast.split("/");
      const t = new Date(`${yyyy}-${mm}-${dd}T00:00:00`).getTime();
      items.push({ bai, forecast, sortKey: t });
    }
  }

  items.sort((a, b) => a.sortKey - b.sortKey);

  let out = `📆 LỊCH CẮT DỰ KIẾN (tất cả bãi)\n(Theo lần CẮT SẠCH gần nhất + ${CUT_INTERVAL_DAYS} ngày)\n`;
  for (const it of items) {
    if (!it.forecast) out += `\n• ${it.bai}: (chưa có dữ liệu cắt sạch)`;
    else out += `\n• ${it.bai}: ${it.forecast}`;
  }

  await send(chatId, out.trim(), { reply_markup: buildMainKeyboard() });
}

/* ================== MAIN LOGIC: BUILD WORK ROW WITH PROGRESS ================== */
/**
 * Rule:
 * - Nếu gDelta thiếu => progress = max => Cắt sạch
 * - Nếu có gDelta => progress = prevProgress + gDelta (nếu prevProgress=0 sau clean)
 *   + nếu progress >= max => progress=max => Cắt sạch
 *   + else => Cắt dỡ
 */
function buildWorkProgress({ allObjs, bai, gDelta }) {
  const max = MAX_DAY[bai];
  const st = computeBaiState(allObjs, bai);

  // st.progress là progress hiện tại (nếu đang cắt dỡ), hoặc 0 nếu vừa sạch
  let newProgress;
  let tinhHinh;

  if (!gDelta) {
    newProgress = max;
    tinhHinh = "Cắt sạch";
  } else {
    newProgress = Math.min(max, Number(st.progress || 0) + Number(gDelta));
    tinhHinh = newProgress >= max ? "Cắt sạch" : "Cắt dỡ";
  }

  const vong = st.currentVong; // vòng hiện tại (cleanDone+1)
  // nếu lần này clean thì vẫn hiển thị vòng hiện tại (đúng yêu cầu)
  // sau đó vòng sẽ tăng cho lần tiếp theo.

  return { max, newProgress, tinhHinh, vong };
}
/* ================== 📋 DANH SÁCH LỆNH ĐÃ GỬI ================== */
async function reportCommandList(chatId) {
  const rows = await getRows();

  const objs = rows
    .map(rowToObj)
    .filter(
      (o) =>
        o.bai &&
        o.baoTau > 0 &&
        o.giaK > 0 &&
        o.won > 0
    );

  if (!objs.length) {
    await send(chatId, "📋 Chưa có lệnh WORK nào.", {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  let out = "📋 DANH SÁCH LỆNH ĐÃ CHỐT:\n\n";
  objs.forEach((o) => {
    out += `${o.bai} ${o.baoTau}b ${o.giaK}k\n`;
  });

  await send(chatId, out.trim(), { reply_markup: buildMainKeyboard() });
}

/* ================== MAIN HANDLER ================== */
async function handleTextMessage(msg) {
  const chatId = msg.chat?.id;
  if (!chatId) return;

  const userName = msg.from?.first_name || "Bạn";
  const textRaw = (msg.text || "").trim();

  // Nếu user nhập mã 2525 để xác nhận xóa
  if (textRaw === CONFIRM_CODE) {
    const p = getPending(chatId);
    if (!p) {
      await send(chatId, "⚠️ Không có yêu cầu xoá nào đang chờ xác nhận.", {
        reply_markup: buildMainKeyboard(),
      });
      return;
    }

    if (p.action === "RESET") {
      await clearAllData();
      clearPending(chatId);
      await send(chatId, "✅ Đã XOÁ SẠCH DATA (giữ header). Bạn có thể làm lại từ đầu.", {
        reply_markup: buildMainKeyboard(),
      });
      return;
    }

    if (p.action === "DEL_LAST") {
      const rows = await getRows();
      const idx = findLastRowIndexAny(rows);
      if (!idx) {
        clearPending(chatId);
        await send(chatId, "Không có dữ liệu để xoá.", { reply_markup: buildMainKeyboard() });
        return;
      }
      await clearRow(idx);
      clearPending(chatId);
      await send(chatId, "✅ Đã xoá dòng gần nhất.", { reply_markup: buildMainKeyboard() });
      return;
    }

    // fallback
    clearPending(chatId);
    await send(chatId, "⚠️ Yêu cầu xác nhận không hợp lệ.", {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  // ====== MENU BUTTONS (Reply keyboard texts) ======
  if (textRaw === "/start") {
    await send(chatId, "✅ Sổ Kim đã sẵn sàng. Bạn cứ nhập lệnh theo cú pháp.", {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  if (textRaw === "📅 Thống kê tháng này") return reportMonth(chatId);
  if (textRaw === "🔁 Thống kê theo VÒNG") return reportByVong(chatId);
  if (textRaw === "📍 Thống kê theo BÃI") return reportByBai(chatId);
  if (textRaw === "📆 Lịch cắt các bãi") return reportCutSchedule(chatId);
  if (textRaw === "📋 Danh sách lệnh đã gửi") return reportCommandList(chatId);

  if (textRaw === "✏️ Sửa dòng gần nhất") {
    await send(
      chatId,
      `✏️ SỬA DÒNG GẦN NHẤT\nBạn gõ:  sua <cú pháp mới>\nVí dụ:\nsua A27 60b 200k\nsua A27 30g 40b 220k\nsua A27 80b 120k 5d`,
      { reply_markup: buildMainKeyboard() }
    );
    return;
  }

  if (textRaw === "🗑️ Xóa dòng gần nhất") {
    setPending(chatId, "DEL_LAST");
    await send(chatId, `⚠️ Xác nhận xoá dòng gần nhất: nhập mã ${CONFIRM_CODE}`, {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  if (textRaw === "⚠️ XÓA SẠCH DỮ LIỆU") {
    setPending(chatId, "RESET");
    await send(chatId, `⚠️ Xác nhận XOÁ SẠCH dữ liệu: nhập mã ${CONFIRM_CODE}`, {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  // ====== SỬA: "sua <...>" ======
  if (textRaw.toLowerCase().startsWith("sua ")) {
    const newLine = textRaw.slice(4).trim();
    const parsed = parseWorkLine(newLine);

    if (!parsed || parsed.type !== "WORK") {
      await send(chatId, buildSaiCuPhapText(), { reply_markup: buildMainKeyboard() });
      return;
    }

    const rows = await getRows();
    const idx = findLastWorkRowIndexForUserAndBai(rows, userName, parsed.bai);

    if (!idx) {
      await send(chatId, "❌ Không tìm thấy dòng gần nhất để sửa cho bãi này.", {
        reply_markup: buildMainKeyboard(),
      });
      return;
    }

    // Lấy toàn bộ objs để tính lại progress/vòng cho dòng sửa
    const objs = rows.map(rowToObj);

    // Vì sửa dòng gần nhất của bãi, lấy "state trước dòng đó":
    // Cách đơn giản: tạm thời bỏ dòng cũ ra khỏi list rồi tính state.
    const rowIndex0 = idx - 2;
    const oldObj = rowToObj(rows[rowIndex0]);

    const objsWithoutOld = objs.filter((_, i) => i !== rowIndex0);

    // ngày làm:
    const nowKST = kst();
    const workDate = parsed.dayInMonth
      ? new Date(nowKST.getFullYear(), nowKST.getMonth(), parsed.dayInMonth)
      : new Date(nowKST.getTime() - 86400000);

    const dateYmd = ymd(workDate);
    const bc = baoChuan(parsed.b);
    const won = bc * parsed.k * 1000;

    // tính progress & vòng theo dữ liệu đã loại dòng cũ
    const { max, newProgress, tinhHinh, vong } = buildWorkProgress({
      allObjs: objsWithoutOld,
      bai: parsed.bai,
      gDelta: parsed.gDelta,
    });

    // tổng thu đến thời điểm này: cộng tất cả + dòng sửa
    const totalBefore = objsWithoutOld.reduce((s, o) => s + (o.won || 0), 0);
    const totalToNow = totalBefore + won;

    // forecast: dựa lần cắt sạch gần nhất (sau khi sửa)
    // nếu lần này sạch => dùng dateYmd làm mốc
    const stAfter = computeBaiState(
      [
        ...objsWithoutOld,
        {
          ...oldObj,
          date: dateYmd,
          bai: parsed.bai,
          dayG: newProgress,
          maxG: max,
          tinhHinh,
          baoTau: parsed.b,
          baoChuan: bc,
          giaK: parsed.k,
          won,
        },
      ],
      parsed.bai
    );

    const forecast = tinhHinh === "Cắt sạch"
      ? addDaysYmd(dateYmd, CUT_INTERVAL_DAYS)
      : forecastForBai(stAfter);

    // update row giữ timestamp cũ
    const newRow = [
      oldObj.ts || new Date().toISOString(), // A
      dateYmd,                               // B
      userName,                              // C
      parsed.bai,                            // D
      newProgress,                           // E (progress)
      max,                                   // F
      tinhHinh,                              // G
      parsed.b,                              // H
      bc,                                    // I
      parsed.k,                              // J
      won,                                   // K
      parsed.note || oldObj.note || "",      // L
    ];

    await updateRow(idx, newRow);

    // trả lại đúng format "SỔ KIM" luôn (kèm forecast mới)
    await sendSoKim({
      chatId,
      userName,
      vong,
      dateYmd,
      bai: parsed.bai,
      progressG: newProgress,
      maxG: max,
      tinhHinh,
      baoTau: parsed.b,
      baoChuanX: bc,
      giaK: parsed.k,
      won,
      totalToNow,
      forecast,
    });

    return;
  }

  // ====== NO_WORK ======
  const parsed = parseWorkLine(textRaw);

  if (!parsed) {
    await send(chatId, buildSaiCuPhapText(), { reply_markup: buildMainKeyboard() });
    return;
  }

  if (parsed.type === "NO_WORK") {
    const d = kst();
    await appendRow([
      new Date().toISOString(), // A
      ymd(d),                   // B
      userName,                 // C
      "",                       // D
      0,                        // E
      0,                        // F
      parsed.tinhHinh,          // G
      0,                        // H
      0,                        // I
      0,                        // J
      0,                        // K
      "",                       // L
    ]);
    await send(chatId, "✅ Đã ghi nhận: " + parsed.tinhHinh, {
      reply_markup: buildMainKeyboard(),
    });
    return;
  }

  // ====== WORK ======
  const nowKST = kst();
  const workDate = parsed.dayInMonth
    ? new Date(nowKST.getFullYear(), nowKST.getMonth(), parsed.dayInMonth)
    : new Date(nowKST.getTime() - 86400000);

  const dateYmd = ymd(workDate);

  const rows = await getRows();
  const objs = rows.map(rowToObj);

  const { max, newProgress, tinhHinh, vong } = buildWorkProgress({
    allObjs: objs,
    bai: parsed.bai,
    gDelta: parsed.gDelta,
  });

  const bc = baoChuan(parsed.b);
  const won = bc * parsed.k * 1000;

  const totalBefore = objs.reduce((s, o) => s + (o.won || 0), 0);
  const totalToNow = totalBefore + won;

  // forecast:
  // - nếu lần này sạch => forecast = dateYmd + interval
  // - nếu cắt dỡ => forecast dựa lastCleanDate (nếu có)
  const stBefore = computeBaiState(objs, parsed.bai);
  const forecast =
    tinhHinh === "Cắt sạch"
      ? addDaysYmd(dateYmd, CUT_INTERVAL_DAYS)
      : (stBefore.lastCleanDate ? addDaysYmd(stBefore.lastCleanDate, CUT_INTERVAL_DAYS) : "");

  // append row
  await appendRow([
    new Date().toISOString(), // A
    dateYmd,                  // B
    userName,                 // C
    parsed.bai,               // D
    newProgress,              // E (progress)
    max,                      // F
    tinhHinh,                 // G
    parsed.b,                 // H
    bc,                       // I
    parsed.k,                 // J
    won,                      // K
    parsed.note || "",        // L
  ]);

  // output
  await sendSoKim({
    chatId,
    userName,
    vong,
    dateYmd,
    bai: parsed.bai,
    progressG: newProgress,
    maxG: max,
    tinhHinh,
    baoTau: parsed.b,
    baoChuanX: bc,
    giaK: parsed.k,
    won,
    totalToNow,
    forecast,
  });
}

/* ================== CALLBACK (optional) ==================
Hiện tại ta dùng Reply Keyboard (bấm là gửi text),
nên callback_query không bắt buộc.
Nhưng vẫn để answerCallbackQuery nếu sau này bạn thêm inline buttons.
=========================================================== */
async function handleCallbackQuery(cb) {
  await tg("answerCallbackQuery", { callback_query_id: cb.id });
}

/* ================== WEBHOOK ================== */
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const body = req.body;

    if (body?.callback_query) {
      await handleCallbackQuery(body.callback_query);
      return;
    }

    if (body?.message) {
      await handleTextMessage(body.message);
      return;
    }
  } catch (e) {
    console.error("WEBHOOK ERROR:", e?.message || e);
  }
});

/* ================== START ================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log("✅ KIM BOT READY on", PORT, "|", VERSION));

/**
 * ============================================================
 * NOTES:
 * - Nếu bạn muốn menu luôn hiện ngay khi chat mở:
 *   chỉ cần /start 1 lần. Bot đã gắn keyboard vào mỗi câu trả lời.
 *
 * - Cột E (DayG) bây giờ là "progress cộng dồn" theo vòng,
 *   nên bãi 34 cắt 2 lần 55g + 54g => lần 2 sẽ thành 109/109 => CẮT SẠCH.
 *
 * - Thống kê vòng:
 *   cộng theo vòng của từng bãi (cleanCountBefore+1) và tính cả cắt dỡ.
 *
 * ============================================================
 */
