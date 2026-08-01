import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from dotenv import load_dotenv
load_dotenv()

# ===== تنظیمات =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN تنظیم نشده")

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID تنظیم نشده")

DB_PATH = os.getenv("DB_PATH", "database.db")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None
PORT = int(os.getenv("PORT", "8080"))

TITLE_MIN = 3
TITLE_MAX = 100
DESC_MIN = 10
DESC_MAX = 1000
PRICE_MAX = 50
CONTACT_MIN = 3
CONTACT_MAX = 100

DEFAULT_CATEGORIES = ["موبایل و تبلت", "لپ‌تاپ و کامپیوتر", "لوازم خانگی", "وسایل نقلیه", "املاک", "پوشاک", "سایر"]
BANNED_WORDS = ["فحش", "مست", "سکس", "شراب", "خوک"]

# ===== لاگ =====
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.FileHandler("logs/errors.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===== دیتابیس =====
def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

class Database:
    def __init__(self):
        self.conn = None

    async def connect(self):
        self.conn = await aiosqlite.connect(DB_PATH)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA foreign_keys = ON")
        await self._init_db()

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def _init_db(self):
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                full_name TEXT,
                join_date TEXT NOT NULL,
                is_banned INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('buy','sell')),
                category_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                price TEXT NOT NULL,
                contact_info TEXT NOT NULL,
                photo_file_id TEXT,
                status TEXT DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (telegram_id),
                FOREIGN KEY (category_id) REFERENCES categories (id)
            );
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (ad_id) REFERENCES ads (id)
            );
            CREATE TABLE IF NOT EXISTS banned_words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT UNIQUE NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ads_status ON ads (status);
        """)
        await self.conn.commit()
        for cat in DEFAULT_CATEGORIES:
            await self.conn.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))
        for word in BANNED_WORDS:
            await self.conn.execute("INSERT OR IGNORE INTO banned_words (word) VALUES (?)", (word,))
        await self.conn.commit()

    async def add_user(self, telegram_id, username, full_name):
        await self.conn.execute(
            "INSERT INTO users (telegram_id, username, full_name, join_date) VALUES (?,?,?,?) "
            "ON CONFLICT(telegram_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name",
            (telegram_id, username, full_name, _now())
        )
        await self.conn.commit()

    async def get_user(self, telegram_id):
        cur = await self.conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

    async def is_banned(self, telegram_id):
        user = await self.get_user(telegram_id)
        return bool(user and user.get("is_banned", 0))

    async def ban_user(self, telegram_id):
        await self.conn.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,))
        await self.conn.commit()

    async def unban_user(self, telegram_id):
        await self.conn.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (telegram_id,))
        await self.conn.commit()

    async def count_users(self):
        cur = await self.conn.execute("SELECT COUNT(*) as c FROM users")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def count_banned(self):
        cur = await self.conn.execute("SELECT COUNT(*) as c FROM users WHERE is_banned = 1")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def get_all_users(self):
        cur = await self.conn.execute("SELECT telegram_id FROM users WHERE is_banned = 0")
        return [dict(row) for row in await cur.fetchall()]

    async def get_categories(self):
        cur = await self.conn.execute("SELECT * FROM categories ORDER BY name")
        return [dict(row) for row in await cur.fetchall()]

    async def create_ad(self, user_id, ad_type, category_id, title, description, price, contact_info, photo_file_id=None):
        cur = await self.conn.execute(
            "INSERT INTO ads (user_id, type, category_id, title, description, price, contact_info, photo_file_id, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (user_id, ad_type, category_id, title, description, price, contact_info, photo_file_id, _now())
        )
        await self.conn.commit()
        return cur.lastrowid

    async def get_ad(self, ad_id):
        cur = await self.conn.execute(
            "SELECT a.*, c.name as category_name, u.username as seller_username, u.full_name as seller_name "
            "FROM ads a JOIN categories c ON c.id = a.category_id JOIN users u ON u.telegram_id = a.user_id "
            "WHERE a.id = ?",
            (ad_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_user_ads(self, user_id):
        cur = await self.conn.execute(
            "SELECT a.*, c.name as category_name FROM ads a JOIN categories c ON c.id = a.category_id "
            "WHERE a.user_id = ? ORDER BY a.created_at DESC",
            (user_id,)
        )
        return [dict(row) for row in await cur.fetchall()]

    async def get_latest_ads(self, limit=10):
        cur = await self.conn.execute(
            "SELECT a.*, c.name as category_name, u.username as seller_username, u.full_name as seller_name "
            "FROM ads a JOIN categories c ON c.id = a.category_id JOIN users u ON u.telegram_id = a.user_id "
            "WHERE a.status = 'approved' ORDER BY a.created_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in await cur.fetchall()]

    async def search_ads(self, query):
        like = f"%{query}%"
        cur = await self.conn.execute(
            "SELECT a.*, c.name as category_name, u.username as seller_username, u.full_name as seller_name "
            "FROM ads a JOIN categories c ON c.id = a.category_id JOIN users u ON u.telegram_id = a.user_id "
            "WHERE a.status = 'approved' AND (a.title LIKE ? OR a.description LIKE ?) ORDER BY a.created_at DESC",
            (like, like)
        )
        return [dict(row) for row in await cur.fetchall()]

    async def get_pending_ads(self):
        cur = await self.conn.execute(
            "SELECT a.*, c.name as category_name, u.username as seller_username, u.full_name as seller_name "
            "FROM ads a JOIN categories c ON c.id = a.category_id JOIN users u ON u.telegram_id = a.user_id "
            "WHERE a.status = 'pending' ORDER BY a.created_at ASC"
        )
        return [dict(row) for row in await cur.fetchall()]

    async def approve_ad(self, ad_id):
        await self.conn.execute("UPDATE ads SET status = 'approved' WHERE id = ?", (ad_id,))
        await self.conn.commit()

    async def reject_ad(self, ad_id):
        await self.conn.execute("UPDATE ads SET status = 'rejected' WHERE id = ?", (ad_id,))
        await self.conn.commit()

    async def delete_ad(self, ad_id, user_id):
        cur = await self.conn.execute("SELECT user_id FROM ads WHERE id = ?", (ad_id,))
        row = await cur.fetchone()
        if not row or row["user_id"] != user_id:
            return False
        await self.conn.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
        await self.conn.commit()
        return True

    async def count_ads(self):
        cur = await self.conn.execute("SELECT COUNT(*) as c FROM ads")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def count_ads_by_status(self, status):
        cur = await self.conn.execute("SELECT COUNT(*) as c FROM ads WHERE status = ?", (status,))
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def add_report(self, ad_id, reporter_id, reason):
        await self.conn.execute(
            "INSERT INTO reports (ad_id, reporter_id, reason, created_at) VALUES (?,?,?,?)",
            (ad_id, reporter_id, reason, _now())
        )
        await self.conn.commit()

    async def get_banned_words(self):
        cur = await self.conn.execute("SELECT word FROM banned_words")
        return [row["word"] for row in await cur.fetchall()]

    async def add_banned_word(self, word):
        await self.conn.execute("INSERT OR IGNORE INTO banned_words (word) VALUES (?)", (word.strip(),))
        await self.conn.commit()

db = Database()

# ===== حالت‌های FSM =====
class ListingForm(StatesGroup):
    choosing_category = State()
    entering_title = State()
    entering_description = State()
    entering_price = State()
    entering_contact = State()
    entering_photo = State()
    confirming = State()

class SearchForm(StatesGroup):
    entering_query = State()

class ReportForm(StatesGroup):
    entering_reason = State()

class AdminForm(StatesGroup):
    broadcasting = State()
    banning_user = State()
    unbanning_user = State()
    adding_word = State()

# ===== توابع کمکی =====
def contains_bad_words(text, banned_words):
    if not text or not banned_words:
        return False
    text = text.lower()
    return any(w.lower() in text for w in banned_words if w.strip())

def format_ad(ad):
    ad_type = "فروش 💰" if ad["type"] == "sell" else "خرید 🛒"
    status_emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}.get(ad.get("status"), "❓")
    text = (
        f"📌 {ad['title']}\n"
        f"🏷 نوع: {ad_type}\n"
        f"📂 دسته: {ad['category_name']}\n"
        f"📝 {ad['description']}\n"
        f"💵 قیمت: {ad['price']}\n"
        f"📞 تماس: {ad['contact_info']}\n"
        f"👤 فروشنده: {ad.get('seller_name', 'نامشخص')}\n"
        f"🕒 {ad['created_at'][:16].replace('T', ' ')}\n"
        f"🆔 {ad['id']} | {status_emoji}"
    )
    if ad.get("photo_file_id"):
        text += "\n📷 دارای عکس"
    return text

# ===== کیبوردها =====
def main_menu(is_admin=False):
    b = InlineKeyboardBuilder()
    b.button(text="💰 فروش", callback_data="menu:sell")
    b.button(text="🛒 خرید", callback_data="menu:buy")
    b.button(text="🔍 جستجو", callback_data="menu:search")
    b.button(text="🆕 آخرین آگهی‌ها", callback_data="menu:latest")
    b.button(text="📋 آگهی‌های من", callback_data="menu:my_ads")
    if is_admin:
        b.button(text="🛠 مدیریت", callback_data="menu:admin")
        b.adjust(2, 2, 1, 1)
    else:
        b.adjust(2, 2, 1, 1)
    return b.as_markup()

def categories_kb(categories):
    b = InlineKeyboardBuilder()
    for cat in categories:
        b.button(text=cat["name"], callback_data=f"cat:{cat['id']}")
    b.button(text="🔙 بازگشت", callback_data="menu:back")
    b.adjust(2)
    return b.as_markup()

def cancel_kb():
    b = InlineKeyboardBuilder()
    b.button(text="🚫 انصراف", callback_data="cancel")
    return b.as_markup()

def confirm_kb():
    b = InlineKeyboardBuilder()
    b.button(text="✅ ثبت", callback_data="ad:confirm")
    b.button(text="🚫 انصراف", callback_data="cancel")
    b.adjust(2)
    return b.as_markup()

def ad_detail_kb(ad_id, owner_id, viewer_id, seller_username=None):
    b = InlineKeyboardBuilder()
    if seller_username:
        b.button(text="📞 تماس", url=f"https://t.me/{seller_username}")
    b.button(text="🚩 گزارش", callback_data=f"report:{ad_id}")
    if viewer_id == owner_id:
        b.button(text="🗑 حذف", callback_data=f"ad:delete:{ad_id}")
    b.button(text="🔙 بازگشت", callback_data="menu:back")
    b.adjust(1)
    return b.as_markup()

def admin_panel_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📊 آمار کاربران", callback_data="admin:stats_users")
    b.button(text="📦 آمار آگهی‌ها", callback_data="admin:stats_ads")
    b.button(text="✅ در انتظار تأیید", callback_data="admin:pending")
    b.button(text="📢 پیام همگانی", callback_data="admin:broadcast")
    b.button(text="🚫 مسدود کردن", callback_data="admin:ban")
    b.button(text="♻️ رفع مسدودی", callback_data="admin:unban")
    b.button(text="✳️ افزودن کلمه", callback_data="admin:addword")
    b.button(text="🔙 بازگشت", callback_data="menu:back")
    b.adjust(2, 2, 2, 1, 1)
    return b.as_markup()

def approve_reject_kb(ad_id):
    b = InlineKeyboardBuilder()
    b.button(text="✅ تأیید", callback_data=f"admin:approve:{ad_id}")
    b.button(text="❌ رد", callback_data=f"admin:reject:{ad_id}")
    b.adjust(2)
    return b.as_markup()

# ===== هندلرها =====
router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    if not user:
        return
    if await db.is_banned(user.id):
        await message.answer("⛔️ مسدود شدید")
        return
    await db.add_user(user.id, user.username, user.full_name)
    await message.answer(
        f"👋 سلام {user.full_name}!\nبه ربات خرید و فروش خوش آمدید 🛍",
        reply_markup=main_menu(user.id == ADMIN_ID)
    )

@router.callback_query(F.data == "menu:back")
async def back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    await callback.message.edit_text("🏠 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    await callback.message.edit_text("❌ لغو شد\n🏠 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()

@router.callback_query(F.data.in_({"menu:sell", "menu:buy"}))
async def start_listing(callback: CallbackQuery, state: FSMContext):
    if await db.is_banned(callback.from_user.id):
        await callback.answer("⛔️ مسدود شدید", show_alert=True)
        return
    await state.update_data(ad_type="sell" if callback.data == "menu:sell" else "buy")
    categories = await db.get_categories()
    await callback.message.edit_text("📂 دسته‌بندی را انتخاب کنید:", reply_markup=categories_kb(categories))
    await state.set_state(ListingForm.choosing_category)
    await callback.answer()

@router.callback_query(ListingForm.choosing_category, F.data.startswith("cat:"))
async def choose_category(callback: CallbackQuery, state: FSMContext):
    cat_id = int(callback.data.split(":")[1])
    await state.update_data(category_id=cat_id)
    await callback.message.edit_text(f"📝 عنوان ({TITLE_MIN}-{TITLE_MAX} کاراکتر):", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_title)
    await callback.answer()

@router.message(ListingForm.entering_title, F.text)
async def enter_title(message: Message, state: FSMContext):
    title = message.text.strip()
    if not (TITLE_MIN <= len(title) <= TITLE_MAX):
        await message.answer(f"⚠️ عنوان باید {TITLE_MIN}-{TITLE_MAX} کاراکتر باشد:")
        return
    banned = await db.get_banned_words()
    if contains_bad_words(title, banned):
        await message.answer("⚠️ شامل کلمات ممنوعه است:")
        return
    await state.update_data(title=title)
    await message.answer(f"🖊 توضیحات ({DESC_MIN}-{DESC_MAX} کاراکتر):", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_description)

@router.message(ListingForm.entering_description, F.text)
async def enter_description(message: Message, state: FSMContext):
    desc = message.text.strip()
    if not (DESC_MIN <= len(desc) <= DESC_MAX):
        await message.answer(f"⚠️ توضیحات باید {DESC_MIN}-{DESC_MAX} کاراکتر باشد:")
        return
    banned = await db.get_banned_words()
    if contains_bad_words(desc, banned):
        await message.answer("⚠️ شامل کلمات ممنوعه است:")
        return
    await state.update_data(description=desc)
    await message.answer(f"💵 قیمت (حداکثر {PRICE_MAX} کاراکتر):", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_price)

@router.message(ListingForm.entering_price, F.text)
async def enter_price(message: Message, state: FSMContext):
    price = message.text.strip()
    if not (1 <= len(price) <= PRICE_MAX):
        await message.answer("⚠️ قیمت نامعتبر است:")
        return
    await state.update_data(price=price)
    await message.answer(f"📞 تماس ({CONTACT_MIN}-{CONTACT_MAX} کاراکتر):", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_contact)

@router.message(ListingForm.entering_contact, F.text)
async def enter_contact(message: Message, state: FSMContext):
    contact = message.text.strip()
    if not (CONTACT_MIN <= len(contact) <= CONTACT_MAX):
        await message.answer(f"⚠️ تماس باید {CONTACT_MIN}-{CONTACT_MAX} کاراکتر باشد:")
        return
    await state.update_data(contact_info=contact)
    await message.answer("📷 عکس بفرستید یا «رد شدن» بزنید:", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_photo)

@router.message(ListingForm.entering_photo, F.photo)
async def enter_photo(message: Message, state: FSMContext):
    photo = message.photo[-1]
    await state.update_data(photo_file_id=photo.file_id)
    await show_preview(message, state)

@router.message(ListingForm.entering_photo, F.text)
async def skip_photo(message: Message, state: FSMContext):
    await state.update_data(photo_file_id=None)
    await show_preview(message, state)

async def show_preview(message: Message, state: FSMContext):
    data = await state.get_data()
    ad_type = "فروش 💰" if data["ad_type"] == "sell" else "خرید 🛒"
    preview = (
        f"📋 پیش‌نمایش:\n\n"
        f"🏷 {ad_type}\n"
        f"📌 {data['title']}\n"
        f"📝 {data['description']}\n"
        f"💵 {data['price']}\n"
        f"📞 {data['contact_info']}\n"
        f"📷 {'دارد' if data.get('photo_file_id') else 'ندارد'}\n\n"
        "ثبت شود؟"
    )
    if data.get("photo_file_id"):
        await message.answer_photo(photo=data["photo_file_id"], caption=preview, reply_markup=confirm_kb())
    else:
        await message.answer(preview, reply_markup=confirm_kb())
    await state.set_state(ListingForm.confirming)

@router.callback_query(ListingForm.confirming, F.data == "ad:confirm")
async def confirm_ad(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user = callback.from_user
    ad_id = await db.create_ad(
        user.id, data["ad_type"], data["category_id"], data["title"],
        data["description"], data["price"], data["contact_info"],
        data.get("photo_file_id")
    )
    await state.clear()
    await callback.message.edit_text("✅ آگهی ثبت شد و در انتظار تأیید است.")
    await callback.message.answer("🏠 منوی اصلی:", reply_markup=main_menu(user.id == ADMIN_ID))
    await callback.answer()

    try:
        ad = await db.get_ad(ad_id)
        ad_type = "فروش 💰" if ad["type"] == "sell" else "خرید 🛒"
        text = f"🆕 آگهی جدید در انتظار تأیید:\n🆔 {ad_id}\n👤 {user.full_name}\n🏷 {ad_type}\n📌 {ad['title']}"
        await callback.bot.send_message(ADMIN_ID, text, reply_markup=approve_reject_kb(ad_id))
    except:
        pass

@router.callback_query(F.data.startswith("ad:delete:"))
async def delete_ad(callback: CallbackQuery):
    ad_id = int(callback.data.split(":")[2])
    if await db.delete_ad(ad_id, callback.from_user.id):
        await callback.answer("🗑 حذف شد", show_alert=True)
        await callback.message.delete()
    else:
        await callback.answer("⚠️ اجازه ندارید", show_alert=True)

@router.callback_query(F.data == "menu:latest")
async def latest(callback: CallbackQuery):
    ads = await db.get_latest_ads()
    if not ads:
        await callback.answer("آگهی وجود ندارد", show_alert=True)
        return
    for ad in ads:
        kb = ad_detail_kb(ad["id"], ad["user_id"], callback.from_user.id, ad.get("seller_username"))
        if ad.get("photo_file_id"):
            await callback.message.answer_photo(photo=ad["photo_file_id"], caption=format_ad(ad), reply_markup=kb)
        else:
            await callback.message.answer(format_ad(ad), reply_markup=kb)
    await callback.answer()

@router.callback_query(F.data == "menu:my_ads")
async def my_ads(callback: CallbackQuery):
    ads = await db.get_user_ads(callback.from_user.id)
    if not ads:
        await callback.answer("آگهی ندارید", show_alert=True)
        return
    for ad in ads:
        kb = ad_detail_kb(ad["id"], ad["user_id"], callback.from_user.id, None)
        if ad.get("photo_file_id"):
            await callback.message.answer_photo(photo=ad["photo_file_id"], caption=format_ad(ad), reply_markup=kb)
        else:
            await callback.message.answer(format_ad(ad), reply_markup=kb)
    await callback.answer()

@router.callback_query(F.data == "menu:search")
async def start_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔍 عبارت جستجو:", reply_markup=cancel_kb())
    await state.set_state(SearchForm.entering_query)
    await callback.answer()

@router.message(SearchForm.entering_query, F.text)
async def do_search(message: Message, state: FSMContext):
    query = message.text.strip()
    await state.clear()
    if len(query) < 2:
        await message.answer("⚠️ حداقل ۲ کاراکتر")
        return
    results = await db.search_ads(query)
    if not results:
        await message.answer("❌ نتیجه‌ای پیدا نشد")
        return
    await message.answer(f"🔎 {len(results)} نتیجه:")
    for ad in results:
        kb = ad_detail_kb(ad["id"], ad["user_id"], message.from_user.id, ad.get("seller_username"))
        if ad.get("photo_file_id"):
            await message.answer_photo(photo=ad["photo_file_id"], caption=format_ad(ad), reply_markup=kb)
        else:
            await message.answer(format_ad(ad), reply_markup=kb)

@router.callback_query(F.data.startswith("report:"))
async def start_report(callback: CallbackQuery, state: FSMContext):
    ad_id = int(callback.data.split(":")[1])
    await state.update_data(report_ad_id=ad_id)
    await callback.message.answer("🚩 دلیل گزارش:", reply_markup=cancel_kb())
    await state.set_state(ReportForm.entering_reason)
    await callback.answer()

@router.message(ReportForm.entering_reason, F.text)
async def submit_report(message: Message, state: FSMContext):
    data = await state.get_data()
    ad_id = data.get("report_ad_id")
    reason = message.text.strip()
    await state.clear()
    if not ad_id:
        await message.answer("⚠️ خطا")
        return
    await db.add_report(ad_id, message.from_user.id, reason)
    await message.answer("✅ گزارش ثبت شد")
    try:
        await message.bot.send_message(ADMIN_ID, f"🚩 گزارش جدید:\nآگهی #{ad_id}\nاز: {message.from_user.full_name}\nدلیل: {reason}")
    except:
        pass

@router.message(Command("admin"))
async def admin_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("🛠 پنل مدیریت:", reply_markup=admin_panel_kb())

@router.callback_query(F.data == "menu:admin")
async def open_admin(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⚠️ دسترسی ندارید", show_alert=True)
        return
    await callback.message.edit_text("🛠 پنل مدیریت:", reply_markup=admin_panel_kb())
    await callback.answer()

@router.callback_query(F.data == "admin:stats_users")
async def stats_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total = await db.count_users()
    banned = await db.count_banned()
    await callback.message.answer(f"👥 کاربران:\nکل: {total}\nمسدود: {banned}")
    await callback.answer()

@router.callback_query(F.data == "admin:stats_ads")
async def stats_ads(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total = await db.count_ads()
    pending = await db.count_ads_by_status("pending")
    approved = await db.count_ads_by_status("approved")
    rejected = await db.count_ads_by_status("rejected")
    await callback.message.answer(f"📦 آگهی‌ها:\nکل: {total}\n⏳ {pending}\n✅ {approved}\n❌ {rejected}")
    await callback.answer()

@router.callback_query(F.data == "admin:pending")
async def pending_ads(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    ads = await db.get_pending_ads()
    if not ads:
        await callback.message.answer("✅ آگهی در انتظار وجود ندارد")
        await callback.answer()
        return
    for ad in ads:
        if ad.get("photo_file_id"):
            await callback.message.answer_photo(photo=ad["photo_file_id"], caption=format_ad(ad), reply_markup=approve_reject_kb(ad["id"]))
        else:
            await callback.message.answer(format_ad(ad), reply_markup=approve_reject_kb(ad["id"]))
    await callback.answer()

@router.callback_query(F.data.startswith("admin:approve:"))
async def approve_ad(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    ad_id = int(callback.data.split(":")[2])
    ad = await db.get_ad(ad_id)
    await db.approve_ad(ad_id)
    await callback.answer("✅ تأیید شد")
    await callback.message.edit_caption(callback.message.caption + "\n\n✅ تأیید شد")
    if ad:
        try:
            await callback.bot.send_message(ad["user_id"], f"✅ آگهی «{ad['title']}» تأیید شد")
        except:
            pass

@router.callback_query(F.data.startswith("admin:reject:"))
async def reject_ad(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    ad_id = int(callback.data.split(":")[2])
    ad = await db.get_ad(ad_id)
    await db.reject_ad(ad_id)
    await callback.answer("❌ رد شد")
    await callback.message.edit_caption(callback.message.caption + "\n\n❌ رد شد")
    if ad:
        try:
            await callback.bot.send_message(ad["user_id"], f"❌ آگهی «{ad['title']}» رد شد")
        except:
            pass

@router.callback_query(F.data == "admin:broadcast")
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.answer("📢 متن پیام:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.broadcasting)
    await callback.answer()

@router.message(AdminForm.broadcasting)
async def do_broadcast(message: Message, state: FSMContext):
    await state.clear()
    users = await db.get_all_users()
    status_msg = await message.answer(f"⏳ ارسال به {len(users)} کاربر...")
    sent = 0
    for user in users:
        try:
            await message.copy_to(user["telegram_id"])
            sent += 1
        except:
            pass
        await asyncio.sleep(0.05)
    await status_msg.edit_text(f"✅ ارسال شد\nموفق: {sent}")

@router.callback_query(F.data == "admin:ban")
async def start_ban(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.answer("🚫 شناسه کاربر:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.banning_user)
    await callback.answer()

@router.message(AdminForm.banning_user, F.text)
async def do_ban(message: Message, state: FSMContext):
    await state.clear()
    if not message.text.isdigit():
        await message.answer("⚠️ عدد وارد کن")
        return
    await db.ban_user(int(message.text))
    await message.answer(f"🚫 کاربر {message.text} مسدود شد")

@router.callback_query(F.data == "admin:unban")
async def start_unban(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.answer("♻️ شناسه کاربر:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.unbanning_user)
    await callback.answer()

@router.message(AdminForm.unbanning_user, F.text)
async def do_unban(message: Message, state: FSMContext):
    await state.clear()
    if not message.text.isdigit():
        await message.answer("⚠️ عدد وارد کن")
        return
    await db.unban_user(int(message.text))
    await message.answer(f"♻️ کاربر {message.text} رفع مسدودی شد")

@router.callback_query(F.data == "admin:addword")
async def start_addword(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.answer("✳️ کلمه ممنوعه:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.adding_word)
    await callback.answer()

@router.message(AdminForm.adding_word, F.text)
async def do_addword(message: Message, state: FSMContext):
    await state.clear()
    word = message.text.strip()
    if not word:
        await message.answer("⚠️ کلمه وارد کن")
        return
    await db.add_banned_word(word)
    await message.answer(f"✅ «{word}» اضافه شد")

# ===== راه‌اندازی =====
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

async def on_startup():
    await db.connect()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        logger.info("Webhook: %s", WEBHOOK_URL)
    else:
        logger.info("حالت Polling")

async def on_shutdown():
    if WEBHOOK_URL:
        await bot.delete_webhook()
    await db.close()

async def health_check(request):
    return web.Response(text="OK")

def run_webhook():
    app = web.Application()
    app.router.add_get("/", health_check)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    web.run_app(app, host="0.0.0.0", port=PORT)

async def run_polling():
    await on_startup()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    if WEBHOOK_URL:
        run_webhook()
    else:
        asyncio.run(run_polling())
