import asyncio
import os
import json
import datetime
import random
import string
import hashlib
import traceback
import sys
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ChatMemberUpdated, Message, CallbackQuery, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton, MessageEntity
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import functools
import gc
from contextlib import asynccontextmanager

# ================= CONFIG =================
TOKEN = '7968143914:AAGBKqmulem7iSNRSGTLaGsB1vGTInEr8v0'
ADMIN_ID = 1417003901
BOT_USERNAME = 'FastRandom_Robot'

# PostgreSQL configuration
POSTGRES_HOST = os.environ.get('PGHOST', 'localhost')
POSTGRES_PORT = os.environ.get('PGPORT', '5432')
POSTGRES_DB = os.environ.get('PGDATABASE', 'fastrandom_bot')
POSTGRES_USER = os.environ.get('PGUSER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('PGPASSWORD', '')

# Пул потоков для CPU-тяжёлых операций
executor = ThreadPoolExecutor(max_workers=8)

def run_sync(func, *args, **kwargs):
    return func(*args, **kwargs)

# Оптимизированный бот с большими лимитами
bot = Bot(token=TOKEN, request_timeout=60)
dp = Dispatcher(storage=MemoryStorage())

# ================= АСИНХРОННАЯ БАЗА ДАННЫХ POSTGRESQL =================
class DatabasePool:
    __slots__ = ('pool', '_dsn')
    
    def __init__(self):
        self.pool = None
        self._dsn = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    async def init(self):
        self.pool = await asyncpg.create_pool(
            self._dsn,
            min_size=10,
            max_size=50,
            command_timeout=60,
            max_queries=50000,
            max_inactive_connection_lifetime=300
        )
    
    @asynccontextmanager
    async def acquire(self):
        async with self.pool.acquire() as conn:
            yield conn
    
    async def execute(self, query: str, *args):
        async with self.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args):
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)
    
    async def executemany(self, query: str, args_list: list):
        async with self.acquire() as conn:
            async with conn.transaction():
                for args in args_list:
                    await conn.execute(query, *args)
    
    async def close(self):
        if self.pool:
            await self.pool.close()

db = DatabasePool()

# ================= КЭШИРОВАНИЕ (улучшенное) =================
from functools import lru_cache
import time as time_module

_user_cache = {}
_user_cache_ttl = 60
_user_cache_time = {}

_giveaway_cache = {}
_giveaway_cache_ttl = 30
_giveaway_cache_time = {}

_temp_cache = {}
_temp_cache_ttl = 300
_temp_cache_time = {}

def _clean_cache():
    now = time_module.time()
    for uid in list(_user_cache.keys()):
        if now - _user_cache_time.get(uid, 0) > _user_cache_ttl:
            del _user_cache[uid]
            del _user_cache_time[uid]
    for gid in list(_giveaway_cache.keys()):
        if now - _giveaway_cache_time.get(gid, 0) > _giveaway_cache_ttl:
            del _giveaway_cache[gid]
            del _giveaway_cache_time[gid]
    for uid in list(_temp_cache.keys()):
        if now - _temp_cache_time.get(uid, 0) > _temp_cache_ttl:
            del _temp_cache[uid]
            del _temp_cache_time[uid]

async def init_db():
    await db.init()
    async with db.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                giveaways_created INTEGER DEFAULT 0,
                total_participants INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS user_channels (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                channel_id TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, channel_id)
            );
            CREATE TABLE IF NOT EXISTS giveaways (
                giveaway_id TEXT PRIMARY KEY,
                creator_id BIGINT NOT NULL,
                channel TEXT NOT NULL,
                chat_id TEXT,
                message_id INTEGER,
                description TEXT,
                description_entities TEXT,
                photo TEXT,
                button_text TEXT DEFAULT 'Участвовать',
                button_color TEXT DEFAULT 'default',
                winners_count INTEGER DEFAULT 1,
                participants TEXT DEFAULT '[]',
                selected_winners TEXT DEFAULT '[]',
                end_time TIMESTAMP,
                target_participants INTEGER,
                required_channel_ids TEXT DEFAULT '[]',
                required_invite_links TEXT DEFAULT '[]',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                postlot_key TEXT
            );
            CREATE TABLE IF NOT EXISTS temp_data (
                user_id BIGINT PRIMARY KEY,
                data TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_giveaways_creator ON giveaways(creator_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_giveaways_active ON giveaways(is_active)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_user_channels_user ON user_channels(user_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_giveaways_end_time ON giveaways(end_time) WHERE is_active = TRUE')

async def get_user(user_id: int) -> dict:
    now = time_module.time()
    if user_id in _user_cache and now - _user_cache_time.get(user_id, 0) < _user_cache_ttl:
        return _user_cache[user_id].copy()
    
    row = await db.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    if not row:
        await db.execute("INSERT INTO users (user_id) VALUES ($1)", user_id)
        row = {"user_id": user_id, "giveaways_created": 0, "total_participants": 0}
    else:
        row = dict(row)
    
    _user_cache[user_id] = row.copy()
    _user_cache_time[user_id] = now
    return row

async def update_user_stats(user_id: int, created: bool = False, participants: int = 0):
    if created:
        await db.execute("UPDATE users SET giveaways_created = giveaways_created + 1 WHERE user_id = $1", user_id)
    if participants > 0:
        await db.execute("UPDATE users SET total_participants = total_participants + $1 WHERE user_id = $2", participants, user_id)
    if user_id in _user_cache:
        del _user_cache[user_id]
        del _user_cache_time[user_id]

async def add_user_channel(user_id: int, channel_id: str) -> bool:
    try:
        await db.execute("INSERT INTO user_channels (user_id, channel_id) VALUES ($1, $2)", user_id, channel_id)
        return True
    except:
        return False

async def get_user_channels(user_id: int) -> list:
    rows = await db.fetch("SELECT channel_id FROM user_channels WHERE user_id = $1", user_id)
    return [row["channel_id"] for row in rows]

async def remove_user_channel(user_id: int, channel_id: str):
    await db.execute("DELETE FROM user_channels WHERE user_id = $1 AND channel_id = $2", user_id, channel_id)

async def save_giveaway(data: dict):
    await db.execute('''
        INSERT INTO giveaways 
        (giveaway_id, creator_id, channel, chat_id, message_id, description, 
         description_entities, photo, button_text, button_color, winners_count,
         participants, selected_winners, end_time, target_participants,
         required_channel_ids, required_invite_links, is_active, created_at, postlot_key)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    ''',
        data["giveaway_id"], data["creator_id"], data["channel"],
        data.get("chat_id"), data.get("message_id"), data.get("description"),
        json.dumps(data.get("description_entities")),
        data.get("photo"), data.get("button_text"), data.get("button_color"),
        data.get("winners_count", 1), json.dumps(data.get("participants", [])),
        json.dumps(data.get("selected_winners", [])),
        data.get("end_time"), data.get("target_participants"),
        json.dumps(data.get("required_channel_ids", [])),
        json.dumps(data.get("required_invite_links", [])),
        data.get("is_active", True), data.get("created_at"), data.get("postlot_key")
    )
    if data["giveaway_id"] in _giveaway_cache:
        del _giveaway_cache[data["giveaway_id"]]
        del _giveaway_cache_time[data["giveaway_id"]]

async def get_giveaway(giveaway_id: str) -> dict:
    now = time_module.time()
    if giveaway_id in _giveaway_cache and now - _giveaway_cache_time.get(giveaway_id, 0) < _giveaway_cache_ttl:
        return _giveaway_cache[giveaway_id].copy() if _giveaway_cache[giveaway_id] else None
    
    row = await db.fetchrow("SELECT * FROM giveaways WHERE giveaway_id = $1", giveaway_id)
    if row:
        row = dict(row)
        _giveaway_cache[giveaway_id] = row.copy()
        _giveaway_cache_time[giveaway_id] = now
    return row

async def get_user_giveaways(user_id: int) -> list:
    rows = await db.fetch("SELECT * FROM giveaways WHERE creator_id = $1 ORDER BY created_at DESC", user_id)
    return [dict(row) for row in rows]

async def update_giveaway(giveaway_id: str, updates: dict):
    for key in ["participants", "selected_winners", "required_channel_ids", "required_invite_links"]:
        if key in updates:
            updates[key] = json.dumps(updates[key])
    
    set_clause = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(updates.keys())])
    values = list(updates.values()) + [giveaway_id]
    await db.execute(f"UPDATE giveaways SET {set_clause} WHERE giveaway_id = ${len(values)}", *values)
    if giveaway_id in _giveaway_cache:
        del _giveaway_cache[giveaway_id]
        del _giveaway_cache_time[giveaway_id]

async def delete_giveaway(giveaway_id: str):
    await db.execute("DELETE FROM giveaways WHERE giveaway_id = $1", giveaway_id)
    if giveaway_id in _giveaway_cache:
        del _giveaway_cache[giveaway_id]
        del _giveaway_cache_time[giveaway_id]

async def save_temp(user_id: int, data: dict):
    await db.execute("INSERT INTO temp_data (user_id, data) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET data = EXCLUDED.data, updated_at = CURRENT_TIMESTAMP", user_id, json.dumps(data))
    if user_id in _temp_cache:
        del _temp_cache[user_id]
        del _temp_cache_time[user_id]

async def get_temp(user_id: int) -> dict:
    now = time_module.time()
    if user_id in _temp_cache and now - _temp_cache_time.get(user_id, 0) < _temp_cache_ttl:
        return _temp_cache[user_id].copy() if _temp_cache[user_id] else None
    
    row = await db.fetchrow("SELECT data FROM temp_data WHERE user_id = $1", user_id)
    data = json.loads(row["data"]) if row else None
    _temp_cache[user_id] = data
    _temp_cache_time[user_id] = now
    return data

async def clear_temp(user_id: int):
    await db.execute("DELETE FROM temp_data WHERE user_id = $1", user_id)
    if user_id in _temp_cache:
        del _temp_cache[user_id]
        del _temp_cache_time[user_id]

# ================= HELPERS =================
def generate_giveaway_id() -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def generate_postlot_key(giveaway_id: str) -> str:
    return hashlib.md5(f"{giveaway_id}_FastRandom_Secret_2026".encode()).hexdigest()[:32]

def get_display_name(user) -> str:
    return f"@{user.username}" if user.username else user.first_name

def create_colored_button(text: str, callback_data: str = None, url: str = None, color: str = "default"):
    if url:
        return InlineKeyboardButton(text=text, url=url)
    return InlineKeyboardButton(text=text, callback_data=callback_data)

# ================= KEYBOARDS =================
def main_menu_keyboard():
    return ReplyKeyboardMarkup(resize_keyboard=True, row_width=2, keyboard=[
        [KeyboardButton(text="Создать розыгрыш")],
        [KeyboardButton(text="Мои розыгрыши"), KeyboardButton(text="Мои каналы")]
    ])

# ================= MESSAGE DELETION SYSTEM =================
last_messages = defaultdict(list)
_last_cleanup = 0

async def delete_message(chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

async def add_to_delete(chat_id: int, message_id: int):
    last_messages[chat_id].append(message_id)
    if len(last_messages[chat_id]) > 10:
        old_id = last_messages[chat_id].pop(0)
        await delete_message(chat_id, old_id)

async def delete_previous_messages(chat_id: int):
    if chat_id in last_messages:
        for msg_id in last_messages[chat_id][:]:
            await delete_message(chat_id, msg_id)
        last_messages[chat_id] = []

async def clean_before_action(chat_id: int, user_message_id: int = None):
    if user_message_id:
        await delete_message(chat_id, user_message_id)
    if chat_id in last_messages and last_messages[chat_id]:
        for msg_id in last_messages[chat_id][-3:]:
            await delete_message(chat_id, msg_id)

# ================= GIVEAWAY LOGIC =================
async def update_participation_button(giveaway_data: dict):
    button_text = giveaway_data.get("button_text", "Участвовать")
    
    markup = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=button_text,
            url=f"https://t.me/{BOT_USERNAME}?start=giveaway_{giveaway_data['giveaway_id']}"
        )
    ]])
    
    try:
        await bot.edit_message_reply_markup(
            giveaway_data["chat_id"],
            giveaway_data["message_id"],
            reply_markup=markup
        )
    except Exception:
        pass

async def conclude_giveaway(giveaway_id: str):
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway or not giveaway["is_active"]:
        return
    
    giveaway["is_active"] = False
    participants = giveaway["participants"]
    winners_count = min(giveaway["winners_count"], len(participants))
    
    if len(participants) == 0:
        results_text = "Результаты конкурса:\n<blockquote>Победителей нет</blockquote>"
    else:
        winners = random.sample(participants, winners_count)
        giveaway["selected_winners"] = winners
        winners_text = ""
        for i, winner in enumerate(winners, 1):
            try:
                user = await bot.get_chat(winner)
                winners_text += f"{i}. {get_display_name(user)}\n"
            except:
                winners_text += f"{i}. Пользователь {winner}\n"
        results_text = f"Результаты конкурса:\n<b>Победители:</b>\n<blockquote>{winners_text}</blockquote>"
    
    await update_giveaway(giveaway_id, {
        "is_active": False,
        "participants": participants,
        "selected_winners": giveaway.get("selected_winners", [])
    })
    
    markup = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Завершён", callback_data=f"results_{giveaway_id}")
    ]])
    
    try:
        await bot.edit_message_reply_markup(
            giveaway["chat_id"],
            giveaway["message_id"],
            reply_markup=markup
        )
        
        if "message_id" in giveaway:
            await bot.send_message(
                giveaway["chat_id"],
                results_text,
                reply_to_message_id=giveaway["message_id"],
                parse_mode="HTML"
            )
        else:
            await bot.send_message(giveaway["chat_id"], results_text, parse_mode="HTML")
    except Exception:
        pass

async def schedule_giveaway_end(giveaway_id: str, delay: float):
    await asyncio.sleep(delay)
    await conclude_giveaway(giveaway_id)

# ================= HANDLERS =================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    await get_user(user_id)
    await delete_message(chat_id, message.message_id)
    
    if message.text and message.text.startswith('/start giveaway_'):
        giveaway_id = message.text.replace('/start giveaway_', '')
        giveaway = await get_giveaway(giveaway_id)
        if giveaway:
            await process_join_giveaway(message, giveaway_id)
            return
        else:
            await bot.send_message(chat_id, "❌ Розыгрыш не найден.", parse_mode="HTML")
    
    text = "👋 <b>Добро пожаловать!</b>\n\n<blockquote><i>Наш бот поможет Вам провести розыгрыш в канале или чате</i></blockquote>"
    sent = await message.answer(text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

async def process_join_giveaway(message: Message, giveaway_id: str):
    user_id = message.from_user.id
    chat_id = message.chat.id
    giveaway = await get_giveaway(giveaway_id)
    
    if not giveaway:
        await bot.send_message(chat_id, "❌ <b>Розыгрыш не найден!</b>", parse_mode="HTML")
        return
    
    if not giveaway.get("is_active"):
        await bot.send_message(chat_id, "⏰ <b>Розыгрыш уже завершён!</b>", parse_mode="HTML")
        return
    
    if user_id in giveaway.get("participants", []):
        await bot.send_message(chat_id, "✅ <b>Вы уже участвуете в розыгрыше!</b>", parse_mode="HTML")
        return
    
    required_channel_ids = giveaway.get("required_channel_ids", [])
    required_invite_links = giveaway.get("required_invite_links", [])
    
    not_subscribed = []
    not_subscribed_links = []
    need_check_button = False
    
    for i, channel_id_str in enumerate(required_channel_ids):
        try:
            channel_id = int(channel_id_str)
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append(channel_id_str)
                link = required_invite_links[i] if i < len(required_invite_links) else None
                not_subscribed_links.append(link)
                try:
                    chat = await bot.get_chat(channel_id)
                    if chat.username:
                        need_check_button = True
                except:
                    pass
        except:
            not_subscribed.append(channel_id_str)
            not_subscribed_links.append(required_invite_links[i] if i < len(required_invite_links) else None)
    
    if not not_subscribed:
        participants = giveaway["participants"]
        participants.append(user_id)
        await update_giveaway(giveaway_id, {"participants": participants})
        await update_participation_button(giveaway)
        await update_user_stats(giveaway["creator_id"], participants=1)
        
        if giveaway.get("target_participants") and len(participants) >= giveaway["target_participants"]:
            await conclude_giveaway(giveaway_id)
        
        await bot.send_message(chat_id, "🎉 <b>Вы участвуете в розыгрыше!</b>\n\nЖелаем удачи! 🍀", parse_mode="HTML")
    else:
        text = "<blockquote>😡 <b>Вы не выполнили условия конкурса‼️</b></blockquote>\n\n"
        markup = InlineKeyboardMarkup(inline_keyboard=[])
        
        for i, link in enumerate(not_subscribed_links):
            if link:
                markup.inline_keyboard.append([InlineKeyboardButton(text="Подписаться", url=link)])
                text += f"<b>Подпишитесь на канал</b>\n"
        
        if need_check_button:
            markup.inline_keyboard.append([InlineKeyboardButton(text="Я подписался", callback_data=f"check_{giveaway_id}")])
        
        markup.inline_keyboard.append([InlineKeyboardButton(text="В меню", callback_data="back_to_main_menu")])
        
        await bot.send_message(
            chat_id,
            text + "\n<b>После выполнения условий Вы станете участником конкурса!</b>",
            reply_markup=markup,
            parse_mode="HTML"
        )

@dp.message(F.text == "Создать розыгрыш")
async def menu_create(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    await clean_before_action(chat_id, message.message_id)
    
    if await get_temp(user_id):
        await clear_temp(user_id)
    
    channels = await get_user_channels(user_id)
    
    text = "🧰 <b>Создание розыгрыша</b>\n\n<blockquote>Выберите канал, где будет опубликован пост розыгрыша</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    for channel in channels:
        try:
            chat = await bot.get_chat(channel)
            channel_title = chat.title if chat.title else channel
        except:
            channel_title = channel
        markup.inline_keyboard.append([InlineKeyboardButton(text=channel_title, callback_data=f"select_channel_{channel}_{user_id}")])
    
    markup.inline_keyboard.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel_step")])
    
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("select_channel_"))
async def select_channel(call: CallbackQuery):
    parts = call.data.split("_")
    channel = parts[2]
    user_id = int(parts[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не Ваш выбор", show_alert=True)
        return
    
    try:
        chat = await bot.get_chat(channel)
        channel_name = chat.title if chat.title else channel
    except:
        channel_name = channel
    
    data = {
        "channel": channel,
        "channel_name": channel_name,
        "description": None,
        "description_entities": None,
        "photo": None,
        "button_text": "🎁 Участвовать",
        "button_color": "default",
        "winners_count": 1,
        "end_type": None,
        "end_time": None,
        "target_participants": None,
        "required_channel_ids": [],
        "created_at": datetime.datetime.now().isoformat()
    }
    await save_temp(user_id, data)
    
    await call.answer(f"✅ Выбран канал: {channel_name}")
    await step_1_post(call, user_id)

@dp.callback_query(F.data == "add_channel_step")
async def add_channel_step(call: CallbackQuery):
    user_id = call.from_user.id
    await clean_before_action(call.message.chat.id, call.message.message_id)
    await call.answer()
    await delete_previous_messages(call.message.chat.id)
    
    text = "📢 <b>Введите @username канала, где будет проходить розыгрыш.</b>\n\n<blockquote>⚠️ Бот должен быть администратором канала!</blockquote>"
    msg = await call.message.answer(text, parse_mode="HTML")
    await add_to_delete(call.message.chat.id, msg.message_id)
    
    @dp.message()
    async def process_channel_for_giveaway(message: Message):
        if message.from_user.id != user_id:
            return
        user_id_local = message.from_user.id
        chat_id = message.chat.id
        
        await delete_message(chat_id, message.message_id)
        channel = message.text.strip()
        if not channel.startswith('@'):
            channel = '@' + channel
        
        try:
            me = await bot.get_me()
            await bot.get_chat_member(channel, me.id)
        except:
            sent = await message.answer("❌ <b>Бот не является администратором канала или канал не найден.</b>", parse_mode="HTML")
            await add_to_delete(chat_id, sent.message_id)
            await message.answer("👋 <b>Добро пожаловать!</b>", reply_markup=main_menu_keyboard(), parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        await add_user_channel(user_id_local, channel)
        
        sent = await message.answer(f"✅ <b>Канал {channel} успешно добавлен!</b>\n\nТеперь создайте розыгрыш.", parse_mode="HTML")
        await add_to_delete(chat_id, sent.message_id)
        
        data = {
            "channel": channel,
            "step": 1,
            "description": None,
            "description_entities": None,
            "photo": None,
            "button_text": "🎁 Участвовать",
            "button_color": "default",
            "winners_count": 1,
            "end_type": None,
            "end_time": None,
            "target_participants": None,
            "required_channel_ids": [],
            "created_at": datetime.datetime.now().isoformat()
        }
        await save_temp(user_id_local, data)
        await step_1_post(message, user_id_local)
        dp.message.handlers.pop()

async def step_1_post(message_or_call, user_id: int):
    chat_id = message_or_call.chat.id if hasattr(message_or_call, 'chat') else message_or_call.message.chat.id
    text = "<b>💬 [1/8] Пост розыгрыша</b>\n<blockquote>Отправьте пост, который будет опубликован в канале</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="‹ Назад", callback_data=f"back_step_{user_id}_0")]
    ])
    
    if isinstance(message_or_call, CallbackQuery):
        sent = await message_or_call.message.answer(text, reply_markup=markup, parse_mode="HTML")
    else:
        sent = await message_or_call.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)
    
    @dp.message()
    async def process_post_message(message: Message):
        if message.from_user.id != user_id:
            return
        chat_id_local = message.chat.id
        
        if message.text and (message.text.startswith('/create') or message.text.startswith('/start') or message.text.startswith('/postlot')):
            await delete_message(chat_id_local, message.message_id)
            await clear_temp(user_id)
            await message.answer("👋 <b>Добро пожаловать!</b>", reply_markup=main_menu_keyboard(), parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        data = await get_temp(user_id)
        if not data:
            await message.reply("❌ <b>Данные потеряны. Начните заново через /start</b>", parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        await delete_message(chat_id_local, message.message_id)
        
        if message.text:
            data["description"] = message.text
            if message.entities:
                data["description_entities"] = [e.to_python() for e in message.entities]
            else:
                data["description_entities"] = None
            data["photo"] = None
        elif message.photo:
            data["description"] = message.caption or ""
            if message.caption_entities:
                data["description_entities"] = [e.to_python() for e in message.caption_entities]
            else:
                data["description_entities"] = None
            data["photo"] = message.photo[-1].file_id
        else:
            await message.reply("❌ <b>Отправьте текст или фото с подписью</b>", parse_mode="HTML")
            await step_1_post(message, user_id)
            return
        
        await save_temp(user_id, data)
        await step_2_button(message, user_id)
        dp.message.handlers.pop()

async def step_2_button(message: Message, user_id: int):
    chat_id = message.chat.id
    text = "<b>💬 [2/8] Кнопка к посту</b>\n\n<blockquote>Выберите готовый вариант или напишите свой текст кнопки</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Участвовать", callback_data=f"btn_{user_id}_Участвовать")],
        [InlineKeyboardButton(text="Принять участие", callback_data=f"btn_{user_id}_Принять_участие")],
        [InlineKeyboardButton(text="Я участвую!", callback_data=f"btn_{user_id}_Я_участвую!")],
        [InlineKeyboardButton(text="Свой текст", callback_data=f"btn_custom_{user_id}")],
        [InlineKeyboardButton(text="Назад", callback_data=f"back_step_{user_id}_1")]
    ])
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("btn_"))
async def btn_callback(call: CallbackQuery):
    data = call.data
    
    if data.startswith("btn_custom_"):
        user_id = int(data.split("_")[2])
        if call.from_user.id != user_id:
            await call.answer("❌ Это не ваш розыгрыш")
            return
        await call.answer()
        msg = await call.message.answer("✏️ <b>Введите ваш текст кнопки (макс 30 символов):</b>", parse_mode="HTML")
        await add_to_delete(call.message.chat.id, msg.message_id)
        
        @dp.message()
        async def process_custom_button(message: Message):
            if message.from_user.id != user_id:
                return
            text = message.text.strip()[:30]
            if not text:
                text = "Участвовать"
            data = await get_temp(user_id)
            if not data:
                await message.reply("❌ <b>Данные потеряны. Начните заново</b>", parse_mode="HTML")
                dp.message.handlers.pop()
                return
            data["button_text"] = text
            await save_temp(user_id, data)
            await step_3_color(message, user_id)
            dp.message.handlers.pop()
        return
    
    parts = data.split("_")
    user_id = int(parts[1])
    if call.from_user.id != user_id:
        await call.answer("❌ Это не ваш розыгрыш")
        return
    
    button_text = "_".join(parts[2:]).replace("_", " ")
    temp_data = await get_temp(user_id)
    if not temp_data:
        await call.answer("❌ Данные потеряны. Начните заново")
        return
    
    temp_data["button_text"] = button_text
    await save_temp(user_id, temp_data)
    await call.answer(f"✅ Текст кнопки: {button_text}")
    await step_3_color(call.message, user_id)

async def step_3_color(message: Message, user_id: int):
    chat_id = message.chat.id
    text = "<b>💬 [3/8] Цвет кнопки</b>\n\n<blockquote>Выберите цвет кнопки участия</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Обычный", callback_data=f"color_{user_id}_default"),
         InlineKeyboardButton(text="Синий", callback_data=f"color_{user_id}_primary")],
        [InlineKeyboardButton(text="Красный", callback_data=f"color_{user_id}_danger"),
         InlineKeyboardButton(text="Зелёный", callback_data=f"color_{user_id}_success")],
        [InlineKeyboardButton(text="Назад", callback_data=f"back_step_{user_id}_2")]
    ])
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("color_"))
async def color_callback(call: CallbackQuery):
    parts = call.data.split("_")
    user_id = int(parts[1])
    color = parts[2]
    
    if call.from_user.id != user_id:
        await call.answer("❌ Это не ваш розыгрыш")
        return
    
    temp_data = await get_temp(user_id)
    if not temp_data:
        await call.answer("❌ Данные потеряны. Начните заново")
        return
    
    temp_data["button_color"] = color
    await save_temp(user_id, temp_data)
    await call.answer(f"Выбран цвет: {color}")
    await step_4_winners(call.message, user_id)

async def step_4_winners(message: Message, user_id: int):
    chat_id = message.chat.id
    text = "<b>💬 [4/8] Количество победителей</b>\n\n<blockquote>Введите количество победителей (от 1 до 100)</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="‹️ Назад", callback_data=f"back_step_{user_id}_3")]
    ])
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)
    
    @dp.message()
    async def process_winners_count(message: Message):
        if message.from_user.id != user_id:
            return
        
        temp_data = await get_temp(user_id)
        if not temp_data:
            await message.reply("❌ <b>Данные потеряны. Начните заново</b>", parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        await delete_message(message.chat.id, message.message_id)
        
        try:
            cnt = int(message.text.strip())
            if cnt < 1 or cnt > 100:
                raise ValueError
        except:
            await message.reply("❌ <b>Введите число от 1 до 100</b>", parse_mode="HTML")
            await step_4_winners(message, user_id)
            return
        
        temp_data["winners_count"] = cnt
        await save_temp(user_id, temp_data)
        await step_5_end_type(message, user_id)
        dp.message.handlers.pop()

async def step_5_end_type(message: Message, user_id: int):
    chat_id = message.chat.id
    text = "<b>💬 [5/8] Как подвести итоги</b>\n\n"
    text += "<blockquote> <b>По времени</b> — итоги в заданную дату\n"
    text += " <b>По числу участников</b> — итоги, когда наберётся нужное количество</blockquote>"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏰ По времени", callback_data=f"endtype_{user_id}_time")],
        [InlineKeyboardButton(text="👥 По числу участников", callback_data=f"endtype_{user_id}_participants")],
        [InlineKeyboardButton(text="‹ Назад", callback_data=f"back_step_{user_id}_4")]
    ])
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("endtype_"))
async def endtype_callback(call: CallbackQuery):
    parts = call.data.split("_")
    user_id = int(parts[1])
    end_type = parts[2]
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш")
        return
    
    temp_data = await get_temp(user_id)
    if not temp_data:
        await call.answer("❌ Данные потеряны. Начните заново")
        return
    
    temp_data["end_type"] = end_type
    await save_temp(user_id, temp_data)
    await call.answer()
    
    if end_type == "time":
        msg = await call.message.answer("⏰ <b>Введите дату и время окончания в формате:</b>\n<code>2025-12-31 23:59</code>", parse_mode="HTML")
        await add_to_delete(call.message.chat.id, msg.message_id)
        
        @dp.message()
        async def process_end_time(message: Message):
            if message.from_user.id != user_id:
                return
            
            temp_data = await get_temp(user_id)
            if not temp_data:
                await message.reply("❌ <b>Данные потеряны. Начните заново</b>", parse_mode="HTML")
                dp.message.handlers.pop()
                return
            
            await delete_message(message.chat.id, message.message_id)
            
            try:
                dt = datetime.datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
                if dt < datetime.datetime.now():
                    raise ValueError
                temp_data["end_time"] = dt.isoformat()
                temp_data["target_participants"] = None
                await save_temp(user_id, temp_data)
                await step_6_subscription(message, user_id)
            except:
                await message.reply("❌ <b>Неверный формат. Пример: 2026-12-31 23:59</b>", parse_mode="HTML")
            dp.message.handlers.pop()
    else:
        msg = await call.message.answer("👥 <b>Введите количество участников для завершения розыгрыша:</b>", parse_mode="HTML")
        await add_to_delete(call.message.chat.id, msg.message_id)
        
        @dp.message()
        async def process_target_participants(message: Message):
            if message.from_user.id != user_id:
                return
            
            temp_data = await get_temp(user_id)
            if not temp_data:
                await message.reply("❌ <b>Данные потеряны. Начните заново</b>", parse_mode="HTML")
                dp.message.handlers.pop()
                return
            
            await delete_message(message.chat.id, message.message_id)
            
            try:
                target = int(message.text.strip())
                if target < 1 or target > 10000000:
                    raise ValueError
                temp_data["target_participants"] = target
                temp_data["end_time"] = None
                await save_temp(user_id, temp_data)
                await step_6_subscription(message, user_id)
            except:
                await message.reply("❌ <b>Введите число от 1 до 10 000 000</b>", parse_mode="HTML")
            dp.message.handlers.pop()

async def step_6_subscription(message: Message, user_id: int):
    chat_id = message.chat.id
    temp_data = await get_temp(user_id)
    if not temp_data:
        return
    
    if "required_channel_ids" not in temp_data:
        temp_data["required_channel_ids"] = []
        await save_temp(user_id, temp_data)
    
    text = "<b>💬 [6/8] Обязательная подписка</b>\n\n"
    text += "<blockquote>До 5 каналов для обязательной подписки.</blockquote>\n"
    text += "<blockquote>Если она не нужна — нажмите «Пропустить»</blockquote>\n\n"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    markup.inline_keyboard.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data=f"add_reqchan_{user_id}")])
    
    if temp_data["required_channel_ids"]:
        markup.inline_keyboard.append([InlineKeyboardButton(text="Очистить все", callback_data=f"clear_all_reqchan_{user_id}")])
        markup.inline_keyboard.append([InlineKeyboardButton(text="➡️ Далее", callback_data=f"next_confirm_{user_id}")])
    else:
        markup.inline_keyboard.append([InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"next_confirm_{user_id}")])
    
    markup.inline_keyboard.append([InlineKeyboardButton(text="‹ Назад", callback_data=f"back_step_{user_id}_5")])
    
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("add_reqchan_"))
async def add_reqchan(call: CallbackQuery):
    user_id = int(call.data.split("_")[2])
    temp_data = await get_temp(user_id)
    
    if not temp_data:
        await call.answer("❌ Данные потеряны")
        return
    
    if len(temp_data.get("required_channel_ids", [])) >= 5:
        await call.answer("❌ Максимум 5 каналов для подписки!", show_alert=True)
        return
    
    await call.answer()
    await delete_previous_messages(call.message.chat.id)
    
    text = (
        "<b>➕ Добавление канала для обязательной подписки</b>\n\n"
        "<blockquote>1️⃣ Выдайте боту права администратора в канале\n"
        "2️⃣ Отправьте @username канала\n"
        "3️⃣ ИЛИ перешлите любое сообщение из канала</blockquote>\n\n"
        f"<i>Осталось мест: {5 - len(temp_data.get('required_channel_ids', []))}</i>"
    )
    msg = await call.message.answer(text, parse_mode="HTML")
    await add_to_delete(call.message.chat.id, msg.message_id)
    
    @dp.message()
    async def process_add_channel_by_username(message: Message):
        if message.from_user.id != user_id:
            return
        
        temp_data = await get_temp(user_id)
        if not temp_data:
            await message.answer("❌ <b>Данные потеряны</b>", parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        await delete_message(message.chat.id, message.message_id)
        
        text = message.text.strip().lower() if message.text else ""
        if text in ['достаточно каналов', 'идем дальше', 'далее', 'готово', 'next', 'done', 'хватит', 'всё', 'дальше', 'продолжить', 'пропустить']:
            await step_7_confirm(message, user_id)
            dp.message.handlers.pop()
            return
        
        if len(temp_data.get("required_channel_ids", [])) >= 5:
            await message.answer("❌ <b>Максимум 5 каналов уже добавлено!</b>", parse_mode="HTML")
            await step_7_confirm(message, user_id)
            dp.message.handlers.pop()
            return
        
        channel_id = None
        display_name = None
        
        if message.forward_from_chat:
            chat = message.forward_from_chat
            channel_id = str(chat.id)
            display_name = chat.title if chat.title else str(chat.id)
            try:
                me = await bot.get_me()
                await bot.get_chat_member(chat.id, me.id)
            except:
                await message.answer("❌ <b>Бот не администратор!</b>", parse_mode="HTML")
                dp.message.handlers.pop()
                return
        elif message.text and message.text.strip().startswith('@'):
            channel_input = message.text.strip()
            try:
                chat = await bot.get_chat(channel_input)
                me = await bot.get_me()
                await bot.get_chat_member(chat.id, me.id)
                channel_id = str(chat.id)
                display_name = chat.title if chat.title else channel_input
            except:
                await message.answer(f"❌ <b>Канал {channel_input} не найден или бот не администратор!</b>", parse_mode="HTML")
                dp.message.handlers.pop()
                return
        else:
            await message.answer("❌ <b>Отправьте @username канала или перешлите сообщение</b>", parse_mode="HTML")
            msg = await message.answer("Попробуйте снова:", parse_mode="HTML")
            await add_to_delete(message.chat.id, msg.message_id)
            dp.message.handlers.pop()
            return
        
        if str(channel_id) in [str(x) for x in temp_data["required_channel_ids"]]:
            await message.answer(f"⚠️ <b>Канал {display_name} уже добавлен!</b>", parse_mode="HTML")
            msg = await message.answer("Отправьте другой канал:", parse_mode="HTML")
            await add_to_delete(message.chat.id, msg.message_id)
            dp.message.handlers.pop()
            return
        
        temp_data["required_channel_ids"].append(channel_id)
        await save_temp(user_id, temp_data)
        added_count = len(temp_data["required_channel_ids"])
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Достаточно каналов, идем дальше", callback_data=f"finish_channels_{user_id}")]
        ])
        
        await message.answer(
            f"✅ <b>Канал {display_name} добавлен! ({added_count}/5)</b>\n\n"
            f"<blockquote>Можете добавить еще или нажать кнопку</blockquote>",
            reply_markup=markup,
            parse_mode="HTML"
        )
        
        if added_count >= 5:
            await step_7_confirm(message, user_id)
            dp.message.handlers.pop()
            return

@dp.callback_query(F.data.startswith("clear_all_reqchan_"))
async def clear_all_reqchan(call: CallbackQuery):
    user_id = int(call.data.split("_")[3])
    temp_data = await get_temp(user_id)
    if not temp_data:
        await call.answer("❌ Данные потеряны")
        return
    temp_data["required_channel_ids"] = []
    await save_temp(user_id, temp_data)
    await call.answer("✅ Все каналы удалены")
    await step_6_subscription(call.message, user_id)

@dp.callback_query(F.data.startswith("finish_channels_"))
async def finish_channels(call: CallbackQuery):
    user_id = int(call.data.split("_")[2])
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваше действие", show_alert=True)
        return
    await call.answer("Я закончил")
    await step_7_confirm(call.message, user_id)

@dp.callback_query(F.data.startswith("next_confirm_"))
async def next_confirm(call: CallbackQuery):
    user_id = int(call.data.split("_")[2])
    await step_7_confirm(call.message, user_id)

async def step_7_confirm(message: Message, user_id: int):
    chat_id = message.chat.id
    data = await get_temp(user_id)
    if not data:
        return
    
    text = "‼️ <b>Внимательно перепроверьте конкурс</b>\n\n"
    
    if data.get("target_participants"):
        text += f"<blockquote>🔚 <b>Конкурс завершится, когда количество участников станет равно {data['target_participants']}</b></blockquote>\n"
    elif data.get("end_time"):
        dt = datetime.datetime.fromisoformat(data["end_time"])
        text += f"<blockquote>🔚 <b>Конкурс завершится: {dt.strftime('%d.%m.%Y %H:%M')}</b></blockquote>\n"
    
    text += f"🏆 <b>Количество победителей:</b> {data['winners_count']}\n\n"
    text += "\n<b>Всё верно? Нажмите \"✅ Подтвердить\".</b>"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{user_id}"),
         InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_create")]
    ])
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data == "cancel_create")
async def cancel_create(call: CallbackQuery):
    user_id = call.from_user.id
    await clear_temp(user_id)
    await call.answer("❌ Создание отменено")
    await call.message.edit_text("❌ <b>Создание розыгрыша отменено</b>", parse_mode="HTML")
    await call.message.answer("👋 <b>Добро пожаловать!</b>", reply_markup=main_menu_keyboard(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("confirm_"))
async def confirm_giveaway(call: CallbackQuery):
    user_id = int(call.data.split("_")[1])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Это не ваш розыгрыш!", show_alert=True)
        return
    
    data = await get_temp(user_id)
    if not data:
        await call.answer("❌ Данные потеряны, начните заново /start")
        return
    
    await call.answer("✅ Розыгрыш создаётся...")
    await call.message.edit_text("✅ <b>Пост будет опубликован в этот канал в ближайшее время!</b>", parse_mode="HTML")
    
    await publish_giveaway(user_id, data)
    await clear_temp(user_id)

async def publish_giveaway(user_id: int, data: dict):
    giveaway_id = generate_giveaway_id()
    postlot_key = generate_postlot_key(giveaway_id)
    
    button = InlineKeyboardButton(
        text=data['button_text'],
        url=f"https://t.me/{BOT_USERNAME}?start=giveaway_{giveaway_id}"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[[button]])
    
    caption_entities = None
    if data.get("description_entities"):
        caption_entities = [MessageEntity(**e) for e in data["description_entities"]]
    
    required_channel_ids = data.get("required_channel_ids", [])
    final_invite_links = []
    
    for channel_id_str in required_channel_ids:
        try:
            channel_id = int(channel_id_str)
            invite_link = await bot.create_chat_invite_link(
                chat_id=channel_id,
                name=f"giveaway_{giveaway_id}",
                member_limit=10000000000000000000000,
                creates_join_request=False
            )
            final_invite_links.append(invite_link.invite_link)
        except Exception:
            final_invite_links.append(f"Канал {channel_id_str}")
    
    try:
        if data.get("photo"):
            sent_msg = await bot.send_photo(
                data["channel"],
                data["photo"],
                caption=data["description"],
                caption_entities=caption_entities,
                reply_markup=markup
            )
        else:
            sent_msg = await bot.send_message(
                data["channel"],
                data["description"],
                entities=caption_entities,
                reply_markup=markup
            )
        
        giveaway_data = {
            "giveaway_id": giveaway_id,
            "chat_id": sent_msg.chat.id,
            "message_id": sent_msg.message_id,
            "creator_id": user_id,
            "channel": data["channel"],
            "description": data["description"],
            "description_entities": data.get("description_entities"),
            "photo": data.get("photo"),
            "button_text": data["button_text"],
            "button_color": data.get("button_color"),
            "winners_count": data["winners_count"],
            "participants": [],
            "selected_winners": [],
            "end_time": data.get("end_time"),
            "target_participants": data.get("target_participants"),
            "required_channel_ids": required_channel_ids,
            "required_invite_links": final_invite_links,
            "is_active": True,
            "created_at": datetime.datetime.now().isoformat(),
            "postlot_key": postlot_key
        }
        
        await save_giveaway(giveaway_data)
        
        if data.get("end_time"):
            dt = datetime.datetime.fromisoformat(data["end_time"])
            delay = (dt - datetime.datetime.now()).total_seconds()
            if delay > 0:
                asyncio.create_task(schedule_giveaway_end(giveaway_id, delay))
        
        await update_user_stats(user_id, created=True)
        
        await bot.send_message(
            user_id,
            f"<blockquote>✅ <b>Розыгрыш успешно создан!</b></blockquote>\n\n"
            f"<code>/postlot{postlot_key}</code>\n\n"
            f"<i>Отправьте эту команду в бота, чтобы опубликовать розыгрыш в другом канале</i>",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await bot.send_message(user_id, f"❌ <b>Ошибка при публикации:</b>\n<code>{str(e)[:200]}</code>", parse_mode="HTML")

@dp.message(F.text == "Мои розыгрыши")
async def menu_my_giveaways(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    await clean_before_action(chat_id, message.message_id)
    
    giveaways = await get_user_giveaways(user_id)
    
    if not giveaways:
        sent = await message.answer("<blockquote>📭 <b>У вас пока нет созданных розыгрышей.</b></blockquote>", parse_mode="HTML")
        await add_to_delete(chat_id, sent.message_id)
        return
    
    text = "<b>💬 Мои розыгрыши</b>\n\n<i>Выберите розыгрыш:</i>"
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    for g in giveaways[:10]:
        giveaway_id = g.get("giveaway_id", "unknown")
        short_id = giveaway_id[-4:] if len(giveaway_id) >= 4 else giveaway_id
        created_at = g.get("created_at")
        if created_at:
            if isinstance(created_at, str):
                try:
                    dt = datetime.datetime.fromisoformat(created_at)
                    created_str = dt.strftime("%d.%m %H:%M")
                except:
                    created_str = "дата неизвестна"
            else:
                created_str = "дата неизвестна"
        else:
            created_str = "дата неизвестна"
        
        status_emoji = "🟢" if g.get("is_active") else "🔴"
        button_text = f"{status_emoji} Розыгрыш #{short_id} | {created_str}"
        markup.inline_keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"view_giveaway_{giveaway_id}_{user_id}")])
    
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("view_giveaway_"))
async def view_giveaway_detail(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    g = await get_giveaway(giveaway_id)
    if not g:
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    short_id = giveaway_id[-4:] if len(giveaway_id) >= 4 else giveaway_id
    status = "🟢 Активен" if g.get("is_active") else "🔴 Завершён"
    
    created_at = g.get("created_at")
    if created_at:
        if isinstance(created_at, str):
            try:
                dt = datetime.datetime.fromisoformat(created_at)
                created_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                created_str = "дата неизвестна"
        else:
            created_str = "дата неизвестна"
    else:
        created_str = "дата неизвестна"
    
    publish_type = "список победителей" if not g.get("is_active") else "кнопка участия"
    participants = g.get("participants", [])
    selected_winners = g.get("selected_winners", [])
    can_add_more = len(selected_winners) < len(participants)
    
    text = f"<b>🎉 Розыгрыш #{short_id}</b>\n\n"
    text += f"<i>Статус:</i> <b>{status}</b>\n"
    text += f"<i>Создан:</i> <b>{created_str}</b>\n"
    text += f"<i>Победителей:</i> <b>{g.get('winners_count', 1)}</b>\n"
    text += f"<i>Участников:</i> <b>{len(participants)}</b>\n\n"
    text += f"<blockquote>Тип публикации в канале после завершения: <b>{publish_type}</b></blockquote>"
    
    if g.get("selected_winners"):
        text += "\n\n<blockquote><b>Выбранные победители:</b></blockquote>\n"
        for i, w in enumerate(g.get("selected_winners", []), 1):
            try:
                user = await bot.get_chat(w)
                text += f"{i}. {get_display_name(user)}\n"
            except:
                text += f"{i}. Пользователь {w}\n"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    if g.get("is_active"):
        markup.inline_keyboard.append([
            InlineKeyboardButton(text="🎲 Подвести итоги", callback_data=f"end_now_{giveaway_id}_{user_id}"),
            InlineKeyboardButton(text="➕ Доп. победители", callback_data=f"add_winners_{giveaway_id}_{user_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_giveaway_{giveaway_id}_{user_id}")
        ])
    else:
        if can_add_more:
            markup.inline_keyboard.append([
                InlineKeyboardButton(text="➕ Добавить победителей", callback_data=f"add_winners_{giveaway_id}_{user_id}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_giveaway_{giveaway_id}_{user_id}")
            ])
        else:
            markup.inline_keyboard.append([
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_giveaway_{giveaway_id}_{user_id}")
            ])
    
    markup.inline_keyboard.append([InlineKeyboardButton(text="‹ Назад", callback_data=f"back_to_list_{user_id}")])
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except:
        pass
    await call.answer()

@dp.callback_query(F.data.startswith("end_now_"))
async def end_now(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3]) if len(parts) > 3 else call.from_user.id
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    if not await get_giveaway(giveaway_id):
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    await call.answer("✅ Подводим итоги...")
    await conclude_giveaway(giveaway_id)
    await view_giveaway_detail(call)

@dp.callback_query(F.data.startswith("add_winners_"))
async def add_winners_callback(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3]) if len(parts) > 3 else call.from_user.id
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    participants = giveaway.get("participants", [])
    already_selected = giveaway.get("selected_winners", [])
    current_winners = len(already_selected)
    max_winners = len(participants)
    
    if current_winners >= max_winners:
        await call.answer(f"❌ Уже выбраны все {max_winners} участников!", show_alert=True)
        return
    
    winners_text = ""
    if already_selected:
        winners_text = "\n\n<b>Уже выбраны и опубликованы в канале:</b>\n"
        for i, w in enumerate(already_selected, 1):
            try:
                user = await bot.get_chat(w)
                winners_text += f"{i}. {get_display_name(user)}\n"
            except:
                winners_text += f"{i}. Пользователь {w}\n"
    
    msg = await call.message.answer(
        f"<b>Всего участников: {max_winners}</b>\n\n"
        f"<b>Сколько победителей добавить?</b>\n"
        f"<i>Победители будут опубликованы в канале!</i>{winners_text}",
        parse_mode="HTML"
    )
    await add_to_delete(call.message.chat.id, msg.message_id)
    
    @dp.message()
    async def process_add_winners_count(message: Message):
        if message.from_user.id != user_id:
            return
        
        await delete_message(message.chat.id, message.message_id)
        
        try:
            additional = int(message.text.strip())
            if additional < 1:
                raise ValueError
            new_count = current_winners + additional
            if new_count > max_winners:
                raise ValueError
        except:
            sent = await message.answer(f"❌ <b>Введите число от 1 до {max_winners - current_winners}</b>", parse_mode="HTML")
            await add_to_delete(message.chat.id, sent.message_id)
            dp.message.handlers.pop()
            return
        
        giveaway = await get_giveaway(giveaway_id)
        if not giveaway:
            await message.answer("❌ Розыгрыш не найден", parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        participants = giveaway.get("participants", [])
        already_selected = giveaway.get("selected_winners", [])
        
        available = [p for p in participants if p not in already_selected]
        
        if len(available) < additional:
            await message.answer(f"❌ Недостаточно участников для выбора {additional} победителей!", parse_mode="HTML")
            dp.message.handlers.pop()
            return
        
        new_winners = random.sample(available, additional)
        
        if "selected_winners" not in giveaway:
            giveaway["selected_winners"] = []
        giveaway["selected_winners"].extend(new_winners)
        giveaway["winners_count"] = len(giveaway["selected_winners"])
        await update_giveaway(giveaway_id, {
            "selected_winners": giveaway["selected_winners"],
            "winners_count": giveaway["winners_count"]
        })
        
        winners_text = ""
        for i, winner in enumerate(giveaway["selected_winners"], 1):
            try:
                user = await bot.get_chat(winner)
                winners_text += f"{i}. {get_display_name(user)}\n"
            except:
                winners_text += f"{i}. Пользователь {winner}\n"
        
        channel_id = giveaway.get("channel")
        message_id = giveaway.get("message_id")
        
        channel_message = (
            f"<b>Дополнительные победители!</b>\n\n"
            f"<b>Победители:</b>\n<blockquote>{winners_text}</blockquote>"
        )
        
        try:
            if message_id:
                await bot.send_message(
                    channel_id,
                    channel_message,
                    reply_to_message_id=message_id,
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(channel_id, channel_message, parse_mode="HTML")
            
            await bot.send_message(user_id, f"✅ <b>Дополнительные победители добавлены и опубликованы в канале!</b>\n\n{winners_text}", parse_mode="HTML")
        except Exception as e:
            await bot.send_message(user_id, f"❌ <b>Не удалось опубликовать победителей в канале!</b>\n\nОшибка: {str(e)[:100]}\n\nПобедители:\n{winners_text}", parse_mode="HTML")
        
        await message.answer("Готово!", reply_markup=main_menu_keyboard(), parse_mode="HTML")
        dp.message.handlers.pop()

@dp.callback_query(F.data.startswith("results_"))
async def results_callback(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3]) if len(parts) > 3 else call.from_user.id
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    if giveaway["is_active"]:
        await call.answer("⏰ Розыгрыш ещё не завершён!", show_alert=True)
        return
    
    text = (
        "📊 <b>Статистика розыгрыша</b>\n\n"
        f"👥 Участников: {len(giveaway['participants'])}\n"
        f"🏆 Победителей: {min(giveaway['winners_count'], len(giveaway['participants']))}\n\n"
        f"Результаты объявлены в канале."
    )
    
    await call.answer(text, show_alert=True)

@dp.callback_query(F.data.startswith("del_giveaway_"))
async def delete_giveaway_confirm(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3]) if len(parts) > 3 else call.from_user.id
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    if not await get_giveaway(giveaway_id):
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del_{giveaway_id}_{user_id}"),
         InlineKeyboardButton(text="❌ Отмена", callback_data=f"view_giveaway_{giveaway_id}_{user_id}")]
    ])
    
    await call.message.edit_text(
        "<b>Удалить этот розыгрыш?</b>\n\nЭто действие нельзя отменить.",
        reply_markup=markup,
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("confirm_del_"))
async def confirm_delete_giveaway(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[2]
    user_id = int(parts[3]) if len(parts) > 3 else call.from_user.id
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    if not await get_giveaway(giveaway_id):
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    await delete_giveaway(giveaway_id)
    
    await call.answer("✅ Розыгрыш удалён")
    await menu_my_giveaways(call.message)

@dp.callback_query(F.data.startswith("back_to_list_"))
async def back_to_list(call: CallbackQuery):
    user_id = int(call.data.split("_")[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш список", show_alert=True)
        return
    
    await menu_my_giveaways(call.message)
    await call.answer()

@dp.message(F.text == "Мои каналы")
async def menu_my_channels(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    await clean_before_action(chat_id, message.message_id)
    
    channels = await get_user_channels(user_id)
    
    text = "<b>Мои каналы</b>\n\n"
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    for channel in channels:
        try:
            chat = await bot.get_chat(channel)
            display_name = chat.title if chat.title else channel
        except:
            display_name = channel
        markup.inline_keyboard.append([InlineKeyboardButton(text=display_name, callback_data=f"chan_mng_{channel}_{user_id}")])
    
    markup.inline_keyboard.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="chan_add_new")])
    markup.inline_keyboard.append([InlineKeyboardButton(text="‹ Назад", callback_data="back_to_main_menu")])
    
    if not channels:
        text += "<blockquote>У вас пока нет добавленных каналов.</blockquote>\n\n"
    else:
        text += "<blockquote><b>Выберите канал или добавьте новый:</b></blockquote>"
    
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("chan_mng_"))
async def manage_channel(call: CallbackQuery):
    parts = call.data.split("_")
    channel = parts[2]
    user_id = int(parts[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Это не ваш канал", show_alert=True)
        return
    
    try:
        chat = await bot.get_chat(channel)
        display_name = chat.title if chat.title else channel
    except:
        display_name = channel
    
    text = f"<b>Выбранный канал {display_name}</b>\n\n<blockquote>Выберите действие:</blockquote>"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить канал", callback_data=f"chan_del_{channel}_{user_id}"),
         InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_channels_list")]
    ])
    
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except:
        await call.message.answer(text, reply_markup=markup, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data.startswith("chan_del_"))
async def delete_channel(call: CallbackQuery):
    parts = call.data.split("_")
    channel = parts[2]
    user_id = int(parts[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Это не ваш канал", show_alert=True)
        return
    
    await remove_user_channel(user_id, channel)
    await call.answer(f"✅ Канал {channel} удалён")
    await call.message.delete()
    await menu_my_channels(call.message)

@dp.callback_query(F.data == "back_to_channels_list")
async def back_to_channels_list(call: CallbackQuery):
    await menu_my_channels(call.message)

@dp.callback_query(F.data == "chan_add_new")
async def add_channel_from_menu(call: CallbackQuery):
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    
    await clean_before_action(chat_id, call.message.message_id)
    await call.answer()
    await delete_previous_messages(chat_id)
    
    text = "📢 <b>Введите @username канала</b>\n\n<blockquote>⚠️ Бот должен быть администратором канала!\n\nОтправьте @username канала:</blockquote>"
    msg = await call.message.answer(text, parse_mode="HTML")
    await add_to_delete(chat_id, msg.message_id)
    
    @dp.message()
    async def process_channel_from_menu(message: Message):
        if message.from_user.id != user_id:
            return
        
        user_id_local = message.from_user.id
        chat_id_local = message.chat.id
        
        await delete_message(chat_id_local, message.message_id)
        
        channel_id = None
        display_name = None
        
        if message.forward_from_chat:
            chat = message.forward_from_chat
            channel_id = str(chat.id)
            display_name = chat.title if chat.title else str(chat.id)
            try:
                me = await bot.get_me()
                await bot.get_chat_member(chat.id, me.id)
            except:
                sent = await message.answer(
                    "❌ <b>Бот не является администратором этого канала!</b>\n\n"
                    "Добавьте бота в администраторы и попробуйте снова.",
                    parse_mode="HTML"
                )
                await add_to_delete(chat_id_local, sent.message_id)
                await menu_my_channels(message)
                dp.message.handlers.pop()
                return
        elif message.text and message.text.strip().startswith('@'):
            channel_input = message.text.strip()
            try:
                chat = await bot.get_chat(channel_input)
                me = await bot.get_me()
                await bot.get_chat_member(chat.id, me.id)
                channel_id = str(chat.id)
                display_name = chat.title if chat.title else channel_input
            except:
                sent = await message.answer(f"❌ <b>Канал {channel_input} не найден или бот не администратор!</b>", parse_mode="HTML")
                await add_to_delete(chat_id_local, sent.message_id)
                await menu_my_channels(message)
                dp.message.handlers.pop()
                return
        else:
            sent = await message.answer(
                "<b>Чтобы добавить канал:</b>\n\n"
                "<blockquote><i>1. Перешлите ЛЮБОЕ сообщение из канала\n"
                "2. Или отправьте @username публичного канала</i></blockquote>",
                parse_mode="HTML"
            )
            await add_to_delete(chat_id_local, sent.message_id)
            await menu_my_channels(message)
            dp.message.handlers.pop()
            return
        
        if channel_id:
            if await add_user_channel(user_id_local, channel_id):
                sent = await message.answer(f"✅ <b>Канал {display_name} успешно добавлен!</b>", parse_mode="HTML")
            else:
                sent = await message.answer(f"⚠️ <b>Канал {display_name} уже добавлен!</b>", parse_mode="HTML")
            await add_to_delete(chat_id_local, sent.message_id)
        else:
            sent = await message.answer("❌ <b>Не удалось определить канал</b>", parse_mode="HTML")
            await add_to_delete(chat_id_local, sent.message_id)
        
        await menu_my_channels(message)
        dp.message.handlers.pop()

@dp.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu(call: CallbackQuery):
    user_id = call.from_user.id
    await delete_previous_messages(call.message.chat.id)
    
    text = "👋 <b>Добро пожаловать!</b>\n\n<blockquote><i>Наш бот поможет Вам провести розыгрыш в канале или чате</i></blockquote>"
    sent = await call.message.answer(text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await add_to_delete(call.message.chat.id, sent.message_id)
    await call.answer()

@dp.callback_query(F.data.startswith("check_"))
async def check_subscription_callback(call: CallbackQuery):
    parts = call.data.split("_")
    giveaway_id = parts[1]
    user_id = call.from_user.id
    
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await call.answer("❌ Розыгрыш не найден в базе данных!", show_alert=True)
        return
    
    if not giveaway.get("is_active"):
        await call.answer("⏰ Этот розыгрыш уже завершён!", show_alert=True)
        return
    
    if user_id in giveaway.get("participants", []):
        await call.answer("✅ Вы уже участвуете в розыгрыше!", show_alert=True)
        return
    
    required_channel_ids = giveaway.get("required_channel_ids", [])
    
    if not required_channel_ids:
        participants = giveaway["participants"]
        participants.append(user_id)
        await update_giveaway(giveaway_id, {"participants": participants})
        await update_participation_button(giveaway)
        
        if giveaway.get("target_participants") and len(participants) >= giveaway["target_participants"]:
            await conclude_giveaway(giveaway_id)
        
        await call.answer("✅ Вы успешно участвуете в розыгрыше!", show_alert=True)
        await bot.send_message(user_id, "🎉 <b>Вы участвуете в розыгрыше!</b>", parse_mode="HTML")
        await call.message.delete()
        return
    
    all_subscribed = True
    for channel_id_str in required_channel_ids:
        try:
            channel_id = int(channel_id_str)
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ['left', 'kicked']:
                all_subscribed = False
                break
        except:
            all_subscribed = False
            break
    
    if all_subscribed:
        if user_id not in giveaway["participants"]:
            participants = giveaway["participants"]
            participants.append(user_id)
            await update_giveaway(giveaway_id, {"participants": participants})
            await update_participation_button(giveaway)
            
            if giveaway.get("target_participants") and len(participants) >= giveaway["target_participants"]:
                await conclude_giveaway(giveaway_id)
            
            await call.answer("✅ Вы успешно подписаны и участвуете в розыгрыше!", show_alert=True)
            await bot.send_message(user_id, "🎉 <b>Вы участвуете в розыгрыше!</b>", parse_mode="HTML")
            await call.message.delete()
        else:
            await call.answer("✅ Вы уже участвуете в розыгрыше!", show_alert=True)
    else:
        await call.answer("❌ Вы не подписаны на все обязательные каналы!", show_alert=True)

@dp.message(Command("postlot"))
async def handle_postlot(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    await delete_message(chat_id, message.message_id)
    
    if text.startswith('/postlot '):
        key = text.replace('/postlot ', '')
    else:
        key = text.replace('/postlot', '')
    
    found_giveaway = None
    found_giveaway_id = None
    
    all_giveaways = await get_user_giveaways(user_id)
    for g in all_giveaways:
        if g.get("postlot_key") == key:
            found_giveaway = g
            found_giveaway_id = g["giveaway_id"]
            break
    
    if not found_giveaway:
        sent = await message.answer("❌ <b>Недействительный ключ или розыгрыш не найден!</b>", parse_mode="HTML")
        await add_to_delete(chat_id, sent.message_id)
        return
    
    channels = await get_user_channels(user_id)
    
    if not channels:
        sent = await message.answer("❌ <b>У вас нет добавленных каналов!</b>", parse_mode="HTML")
        await add_to_delete(chat_id, sent.message_id)
        return
    
    text = "<b>Выберите канал для публикации</b>"
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    for channel in channels:
        try:
            chat = await bot.get_chat(channel)
            channel_title = chat.title if chat.title else channel
        except:
            channel_title = channel
        markup.inline_keyboard.append([InlineKeyboardButton(text=channel_title, callback_data=f"postlot_channel_{channel}_{found_giveaway_id}_{user_id}")])
    
    markup.inline_keyboard.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_postlot")])
    
    sent = await message.answer(text, reply_markup=markup, parse_mode="HTML")
    await add_to_delete(chat_id, sent.message_id)

@dp.callback_query(F.data.startswith("postlot_channel_"))
async def postlot_channel_callback(call: CallbackQuery):
    parts = call.data.split("_")
    channel = "_".join(parts[2:-2])
    giveaway_id = parts[-2]
    user_id = int(parts[-1])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш запрос", show_alert=True)
        return
    
    giveaway = await get_giveaway(giveaway_id)
    if not giveaway:
        await call.answer("❌ Розыгрыш не найден", show_alert=True)
        return
    
    user_channels = await get_user_channels(user_id)
    if channel not in user_channels:
        await call.answer("❌ У вас нет доступа к этому каналу!", show_alert=True)
        return
    
    await call.answer("✅ Публикую...")
    
    button = InlineKeyboardButton(
        text=giveaway["button_text"],
        url=f"https://t.me/{BOT_USERNAME}?start=giveaway_{giveaway_id}"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[[button]])
    
    caption_entities = None
    if giveaway.get("description_entities"):
        caption_entities = [MessageEntity(**e) for e in giveaway["description_entities"]]
    
    try:
        if giveaway.get("photo"):
            await bot.send_photo(
                channel,
                giveaway["photo"],
                caption=giveaway["description"],
                caption_entities=caption_entities,
                reply_markup=markup
            )
        else:
            if caption_entities:
                await bot.send_message(
                    channel,
                    giveaway["description"],
                    entities=caption_entities,
                    reply_markup=markup
                )
            else:
                await bot.send_message(
                    channel,
                    giveaway["description"],
                    reply_markup=markup
                )
        
        await call.message.delete()
        await call.message.answer("✅ <b>Розыгрыш опубликован!</b>", parse_mode="HTML")
    except Exception as e:
        error_msg = str(e)
        if "chat not found" in error_msg:
            await call.message.answer(
                "❌ <b>Ошибка: Бот не является администратором канала или канал не найден!</b>\n\n"
                "Добавьте бота в канал как администратора и попробуйте снова.",
                parse_mode="HTML"
            )
        else:
            await call.message.answer(f"❌ <b>Ошибка:</b>\n<code>{error_msg[:200]}</code>", parse_mode="HTML")

@dp.callback_query(F.data == "cancel_postlot")
async def cancel_postlot(call: CallbackQuery):
    await call.answer("❌ Отменено")
    await call.message.delete()
    await call.message.answer("👋 <b>Добро пожаловать!</b>", reply_markup=main_menu_keyboard(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("back_step_"))
async def back_step(call: CallbackQuery):
    parts = call.data.split("_")
    user_id = int(parts[2])
    step = int(parts[3])
    
    if call.from_user.id != user_id:
        await call.answer("❌ Не ваш розыгрыш", show_alert=True)
        return
    
    await call.answer()
    chat_id = call.message.chat.id
    
    if not await get_temp(user_id) and step > 0:
        await call.message.answer("❌ <b>Данные потеряны. Начните заново через /start</b>", parse_mode="HTML")
        return
    
    if step == 0:
        await call.message.answer("👋 <b>Добро пожаловать!</b>", reply_markup=main_menu_keyboard(), parse_mode="HTML")
    elif step == 1:
        await step_1_post(call, user_id)
    elif step == 2:
        await step_2_button(call.message, user_id)
    elif step == 3:
        await step_3_color(call.message, user_id)
    elif step == 4:
        await step_4_winners(call.message, user_id)
    elif step == 5:
        await step_5_end_type(call.message, user_id)
    elif step == 6:
        await step_6_subscription(call.message, user_id)
    elif step == 7:
        await step_7_confirm(call.message, user_id)

@dp.chat_member()
async def handle_bot_chat_member_update(update: ChatMemberUpdated):
    if update.new_chat_member.user.id != (await bot.get_me()).id:
        return
    
    chat = update.chat
    
    if chat.type not in ['channel', 'supergroup']:
        return
    
    user_id = update.from_user.id
    channel_id = str(chat.id)
    channel_name = chat.title if chat.title else channel_id
    
    if update.new_chat_member.status == 'administrator' and update.old_chat_member.status != 'administrator':
        if await add_user_channel(user_id, channel_id):
            await bot.send_message(user_id, f"<blockquote><b>Канал {channel_name} успешно подключен!</b></blockquote>\n\n", parse_mode="HTML")
        else:
            await bot.send_message(user_id, f"<b>Канал {channel_name} уже был в вашем списке.</b>", parse_mode="HTML")
    
    elif update.old_chat_member.status == 'administrator' and update.new_chat_member.status != 'administrator':
        rows = await db.fetch("SELECT user_id FROM user_channels WHERE channel_id = $1", channel_id)
        affected_users = [row["user_id"] for row in rows]
        
        for uid in affected_users:
            await remove_user_channel(uid, channel_id)
            try:
                await bot.send_message(
                    uid,
                    f"<b>Канал {channel_name} был автоматически удалён из вашего списка!</b>\n\n"
                    f"<blockquote><i>Бот больше не является администратором этого канала.</i></blockquote>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

# ================= MAIN =================
async def main():
    await init_db()
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()

if __name__ == '__main__':
    asyncio.run(main())