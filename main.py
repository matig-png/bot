import asyncio
import logging
import re
import json
import os
import uuid
import sqlite3
import sys
from typing import Dict, Any, List, Tuple, Optional
from html import escape as html_escape
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, User, MessageEntity
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ====================== ЛОГИРОВАНИЕ ======================

sys.stdout.reconfigure(line_buffering=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler('bot.log', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ====================== КОНФИГУРАЦИЯ ======================

MAIN_BOT_TOKEN = "8216288128:AAHWCLpy-tPcFKjbpM2hUN1xt6P850mi5qE"
MAIN_ADMIN_ID = 6098677257
ADMIN_IDS = {6098677257, 8092280284, 8366347415}
DB_FILE = "bot_database.db"
CONFIG_FILE = "bot_config.json"
MAIN_ANNOUNCEMENT_CHANNEL = "-1003904052294"

BET_PATTERN = re.compile(r'(?:ставлю|ставка)\s+(\d+)', re.IGNORECASE)
PASS_PATTERN = re.compile(r'^(?:пас|лив)$', re.IGNORECASE)
MIN_BID_INCREMENT = 5

TG_LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/)?([a-zA-Z0-9_]+)',
    re.IGNORECASE
)


# ====================== БАЗА ДАННЫХ ======================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info(f"База данных подключена: {self.db_path}")

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT ''
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS balances (
            user_id INTEGER NOT NULL,
            bot_id TEXT NOT NULL,
            balance REAL NOT NULL DEFAULT 0,
            is_infinite INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, bot_id)
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS user_bot_data (
            user_id INTEGER NOT NULL,
            bot_id TEXT NOT NULL,
            quiz_passed INTEGER NOT NULL DEFAULT 0,
            show_in_top INTEGER NOT NULL DEFAULT 1,
            is_blocked INTEGER NOT NULL DEFAULT 0,
            is_frozen INTEGER NOT NULL DEFAULT 0,
            is_moderator INTEGER NOT NULL DEFAULT 0,
            is_admin INTEGER NOT NULL DEFAULT 0,
            is_owner INTEGER NOT NULL DEFAULT 0,
            activated_at TEXT NOT NULL DEFAULT '',
            last_promo_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (user_id, bot_id)
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS take_timestamps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            bot_id TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )''')
        self.conn.commit()

    def get_user(self, user_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create_or_update_user(self, user_id: int, username: str, name: str):
        now = datetime.now().isoformat()
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO users (user_id, username, name, created_at) VALUES (?, ?, ?, ?)',
            (user_id, username, name, now)
        )
        cursor.execute(
            'UPDATE users SET username = ?, name = ? WHERE user_id = ?',
            (username, name, user_id)
        )
        self.conn.commit()

    def get_balance(self, user_id: int, bot_id: str) -> float:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT balance, is_infinite FROM balances WHERE user_id = ? AND bot_id = ?',
            (user_id, bot_id)
        )
        row = cursor.fetchone()
        if not row:
            return 0
        return float('inf') if row['is_infinite'] else row['balance']

    def set_balance(self, user_id: int, bot_id: str, balance: float):
        is_inf = 1 if balance == float('inf') else 0
        val = 0 if is_inf else balance
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO balances (user_id, bot_id, balance, is_infinite) VALUES (?, ?, ?, ?)',
            (user_id, bot_id, val, is_inf)
        )
        self.conn.commit()

    def add_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        current = self.get_balance(user_id, bot_id)
        if current == float('inf'):
            return True
        self.set_balance(user_id, bot_id, current + amount)
        return True

    def deduct_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        current = self.get_balance(user_id, bot_id)
        if current == float('inf'):
            return True
        if current < amount:
            return False
        self.set_balance(user_id, bot_id, current - amount)
        return True

    def get_bot_data(self, user_id: int, bot_id: str) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM user_bot_data WHERE user_id = ? AND bot_id = ?',
            (user_id, bot_id)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {
            'user_id': user_id, 'bot_id': bot_id,
            'quiz_passed': 0, 'show_in_top': 1,
            'is_blocked': 0, 'is_frozen': 0,
            'is_moderator': 0, 'is_admin': 0, 'is_owner': 0,
            'activated_at': '', 'last_promo_at': ''
        }

    def set_bot_data(self, user_id: int, bot_id: str, **kwargs):
        existing = self.get_bot_data(user_id, bot_id)
        existing.update(kwargs)
        cursor = self.conn.cursor()
        cursor.execute('''INSERT OR REPLACE INTO user_bot_data
            (user_id, bot_id, quiz_passed, show_in_top, is_blocked, is_frozen,
             is_moderator, is_admin, is_owner, activated_at, last_promo_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (user_id, bot_id, existing['quiz_passed'], existing['show_in_top'],
             existing['is_blocked'], existing['is_frozen'], existing['is_moderator'],
             existing['is_admin'], existing['is_owner'], existing['activated_at'],
             existing['last_promo_at']))
        self.conn.commit()

    def get_last_take_time(self, user_id: int, bot_id: str) -> Optional[str]:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT timestamp FROM take_timestamps WHERE user_id = ? AND bot_id = ? ORDER BY id DESC LIMIT 1',
            (user_id, bot_id)
        )
        row = cursor.fetchone()
        return row['timestamp'] if row else None

    def add_take_timestamp(self, user_id: int, bot_id: str):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO take_timestamps (user_id, bot_id, timestamp) VALUES (?, ?, ?)',
            (user_id, bot_id, datetime.now().isoformat())
        )
        cursor.execute('''DELETE FROM take_timestamps WHERE id NOT IN (
            SELECT id FROM take_timestamps WHERE user_id = ? AND bot_id = ? ORDER BY id DESC LIMIT 20
        ) AND user_id = ? AND bot_id = ?''', (user_id, bot_id, user_id, bot_id))
        self.conn.commit()

    def get_all_users_for_bot(self, bot_id: str) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('''SELECT u.user_id, u.username, u.name,
                     COALESCE(b.balance, 0) as balance,
                     COALESCE(b.is_infinite, 0) as is_infinite,
                     COALESCE(d.show_in_top, 1) as show_in_top,
                     COALESCE(d.is_owner, 0) as is_owner
                     FROM users u
                     LEFT JOIN balances b ON u.user_id = b.user_id AND b.bot_id = ?
                     LEFT JOIN user_bot_data d ON u.user_id = d.user_id AND d.bot_id = ?
                     WHERE b.balance IS NOT NULL OR b.is_infinite = 1''', (bot_id, bot_id))
        return [dict(row) for row in cursor.fetchall()]

    def find_user_by_input(self, input_str: str) -> Optional[int]:
        input_str = input_str.strip().lstrip('@').lower()
        try:
            uid = int(input_str)
            if self.get_user(uid):
                return uid
        except ValueError:
            pass
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT user_id FROM users WHERE LOWER(username) = ? OR LOWER(name) = ?',
            (input_str, input_str)
        )
        row = cursor.fetchone()
        return row['user_id'] if row else None


db = Database(DB_FILE)


# ====================== КОНФИГУРАЦИЯ БОТОВ ======================

@dataclass
class BotConfig:
    bot_id: str
    token: str
    currency_name: str
    currency_emoji: str
    channel_url: str = ""
    takes_channel: str = ""
    shop_channel: str = ""
    announcement_channel: str = ""
    modules: List[str] = field(default_factory=lambda: ["takes", "shop"])
    take_cooldown_minutes: int = 3
    quiz_reward: int = 50
    admin_starting_balance: int = 100
    promo_price_per_hour: int = 10
    promo_pin_price_per_hour: int = 25
    owner_id: int = 0
    base_exchange_rate: float = 0.5
    censored_words: List[str] = field(default_factory=list)
    marker_words: List[str] = field(default_factory=list)
    shop_topics: List[Dict[str, str]] = field(default_factory=list)
    takes_paused: bool = False
    manual_control: bool = False


@dataclass
class ExchangeRates:
    rates: Dict[str, float] = field(default_factory=dict)
    rates_locked: bool = False


@dataclass
class PendingBotRequest:
    request_id: str
    user_id: int
    channel_url: str
    status: str = "pending"
    token: str = ""
    currency_name: str = ""
    currency_emoji: str = ""
    modules: List[str] = field(default_factory=list)
    takes_channel: str = ""
    shop_channel: str = ""
    announcement_channel: str = ""


class ConfigStorage:
    def __init__(self):
        self.bots: Dict[str, BotConfig] = {}
        self.exchange_rates: ExchangeRates = ExchangeRates()
        self.pending_requests: Dict[str, PendingBotRequest] = {}
        self.pending_takes: Dict[str, Dict] = {}
        self.paused_takes: Dict[str, List[Dict]] = {}
        self.pending_purchases: Dict[str, Dict] = {}
        self.scheduled_deletions: Dict[str, Dict] = {}
        self.active_quizzes: Dict[str, Dict] = {}
        self.active_auctions: Dict[str, Dict] = {}
        self.active_bots: Dict[str, Bot] = {}
        self.active_dispatchers: Dict[str, Dispatcher] = {}
        self.waiting_for_token: Dict[int, str] = {}
        self.auction_tasks: Dict[str, asyncio.Task] = {}

    def load(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for bot_id, bot_data in data.get('bots', {}).items():
                    if 'announcement_channel' not in bot_data:
                        bot_data['announcement_channel'] = ""
                    self.bots[bot_id] = BotConfig(**bot_data)
                if 'exchange_rates' in data:
                    self.exchange_rates = ExchangeRates(**data['exchange_rates'])
                for req_id, req_data in data.get('pending_requests', {}).items():
                    if 'announcement_channel' not in req_data:
                        req_data['announcement_channel'] = ""
                    self.pending_requests[req_id] = PendingBotRequest(**req_data)
                self.pending_takes = data.get('pending_takes', {})
                self.paused_takes = data.get('paused_takes', {})
                self.pending_purchases = data.get('pending_purchases', {})
                self.scheduled_deletions = data.get('scheduled_deletions', {})
                self.active_quizzes = data.get('active_quizzes', {})
                self.active_auctions = data.get('active_auctions', {})
                logger.info(f"Конфигурация загружена: {len(self.bots)} ботов")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")

        if "main" not in self.bots:
            self.bots["main"] = BotConfig(
                bot_id="main",
                token=MAIN_BOT_TOKEN,
                currency_name="луны",
                currency_emoji="🌗",
                channel_url="https://t.me/WINGSOFFIRECHANNEL",
                takes_channel="@WINGSOFFIRECHANNEL",
                shop_channel="@wingsoffiremagazine",
                announcement_channel=MAIN_ANNOUNCEMENT_CHANNEL,
                modules=["takes", "shop"],
                take_cooldown_minutes=3,
                owner_id=MAIN_ADMIN_ID,
                base_exchange_rate=1.0
            )
            self.exchange_rates.rates["main"] = 1.0
            logger.info(f"Главный бот создан с каналом объявлений: {MAIN_ANNOUNCEMENT_CHANNEL}")
            self.save()
        else:
            needs_save = False
            if not self.bots["main"].announcement_channel:
                self.bots["main"].announcement_channel = MAIN_ANNOUNCEMENT_CHANNEL
                logger.info(f"Обновлён канал объявлений: {MAIN_ANNOUNCEMENT_CHANNEL}")
                needs_save = True
            if needs_save:
                self.save()

    def save(self):
        try:
            data = {
                'bots': {k: asdict(v) for k, v in self.bots.items()},
                'exchange_rates': asdict(self.exchange_rates),
                'pending_requests': {k: asdict(v) for k, v in self.pending_requests.items()},
                'pending_takes': self.pending_takes,
                'paused_takes': self.paused_takes,
                'pending_purchases': self.pending_purchases,
                'scheduled_deletions': self.scheduled_deletions,
                'active_quizzes': self.active_quizzes,
                'active_auctions': self.active_auctions
            }
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации: {e}")


config = ConfigStorage()


# ====================== FSM СОСТОЯНИЯ ======================

class AdminStates(StatesGroup):
    WaitingUsernameForDeduct = State()
    WaitingAmountForDeduct = State()
    WaitingUsernameForFreeze = State()
    WaitingUsernameForUnfreeze = State()
    WaitingCensorWord = State()
    WaitingMarkerWord = State()
    WaitingRemoveCensorWord = State()
    WaitingRemoveMarkerWord = State()
    WaitingModeratorUsername = State()
    WaitingRemoveModeratorUsername = State()
    WaitingQuizQuestion = State()
    WaitingQuizReward = State()
    WaitingQuizAnswer = State()


class TransferStates(StatesGroup):
    WaitingReceiver = State()
    WaitingAmountAndMessage = State()


class TakeStates(StatesGroup):
    WaitingTake = State()


class AnnouncementStates(StatesGroup):
    WaitingAnnouncement = State()


class PromoStates(StatesGroup):
    WaitingHours = State()
    WaitingPost = State()
    WaitingConfirmation = State()


class PurchaseStates(StatesGroup):
    WaitingImage = State()
    WaitingSeller = State()
    WaitingAmount = State()


class ConvertStates(StatesGroup):
    WaitingSource = State()
    WaitingTarget = State()
    WaitingAmount = State()


class ConnectBotStates(StatesGroup):
    WaitingChannelUrl = State()
    WaitingCurrencyName = State()
    WaitingCurrencyEmoji = State()
    WaitingModules = State()
    WaitingTakesChannel = State()
    WaitingShopChannel = State()
    WaitingAnnouncementChannel = State()


class QuizStates(StatesGroup):
    Question1 = State()
    Question2 = State()
    Question3 = State()

# ====================== ЦЕНЗУРА ======================

_PFX = r'(?:за|на|по|от|об|до|у|о|вы|пере|при|рас|раз|про|недо|пре|с|ис|из)?'
_PFX_EB = r'(?:за|на|по|от|отъ|об|объ|до|у|о|вы|пере|при|рас|раз|про|недо|пре|съ|ис|из|долбо)?'
BASE_PROFANITY_PATTERNS = [
    _PFX + r'ху[йяеёюи]\w*', _PFX + r'пизд\w*', _PFX_EB + r'[её]б\w*',
    r'бля[дт]\w*', r'сук[аиуе]\w*', r'суч[каеьи]\w*',
    r'муда[кч]\w*', r'мудил\w*', r'мудозвон\w*',
    r'пидор\w*', r'пидар\w*', r'пидр\w*', r'педик\w*', r'педераст\w*',
    r'шлюх\w*', r'гандон\w*', r'залуп\w*', r'дроч\w*', r'манд[аоуеёяи]\w*',
    r'[её]бл[ао]\w*', r'[её]бну\w*', r'[её]бан\w*',
    r'хер[а-яё]*\w*', r'жоп[аеуы]\w*', r'срать?\w*', r'сран\w*',
    r'говн[оа]\w*', r'засранец\w*', r'засранк[аи]\w*',
]

def build_profanity_regex(bot_id: str) -> re.Pattern:
    bot_cfg = config.bots.get(bot_id)
    patterns = BASE_PROFANITY_PATTERNS.copy()
    if bot_cfg and bot_cfg.censored_words:
        for word in bot_cfg.censored_words:
            patterns.append(re.escape(word) + r'\w*')
    return re.compile(r'\b(?:' + '|'.join(patterns) + r')\b', re.IGNORECASE | re.UNICODE)

def censor_profanity(text: str, bot_id: str) -> Tuple[str, bool]:
    if not text:
        return text, False
    regex = build_profanity_regex(bot_id)
    matches = list(regex.finditer(text))
    if not matches:
        return text, False
    parts = []
    last = 0
    for match in matches:
        parts.append(html_escape(text[last:match.start()]))
        parts.append(f'<tg-spoiler>{html_escape(match.group())}</tg-spoiler>')
        last = match.end()
    parts.append(html_escape(text[last:]))
    return ''.join(parts), True

def contains_marker_words(text: str, bot_id: str) -> bool:
    if not text:
        return False
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg or not bot_cfg.marker_words:
        return False
    text_lower = text.lower()
    return any(word.lower() in text_lower for word in bot_cfg.marker_words)

async def check_telegram_links(text: str, bot_instance: Bot) -> Tuple[bool, str]:
    if not text:
        return False, ""
    links = TG_LINK_PATTERN.findall(text)
    if not links:
        return False, ""
    for link in links:
        if link.startswith("joinchat"):
            return True, "Инвайт-ссылка"
        try:
            chat = await bot_instance.get_chat(f"@{link}")
            if chat.type in ("group", "supergroup"):
                return True, f"Группа: @{link}"
        except Exception:
            pass
    return False, ""


# ====================== УТИЛИТЫ ======================

def register_user(user: User, bot_id: str):
    uid = user.id
    username = user.username or f"user{uid}"
    name = user.full_name
    db.create_or_update_user(uid, username, name)
    existing = db.get_bot_data(uid, bot_id)
    if not existing.get('activated_at'):
        bot_cfg = config.bots.get(bot_id)
        is_owner_flag = bot_cfg and bot_cfg.owner_id == uid
        is_admin_flag = bot_id == "main" and uid in ADMIN_IDS
        is_main_owner = bot_id == "main" and uid == MAIN_ADMIN_ID
        if is_owner_flag or is_main_owner:
            db.set_balance(uid, bot_id, float('inf'))
        elif is_admin_flag:
            db.set_balance(uid, bot_id, bot_cfg.admin_starting_balance)
        else:
            db.set_balance(uid, bot_id, 0)
        db.set_bot_data(uid, bot_id,
            quiz_passed=0, show_in_top=1, is_blocked=0, is_frozen=0, is_moderator=0,
            is_admin=1 if (is_admin_flag or is_owner_flag or is_main_owner) else 0,
            is_owner=1 if (is_owner_flag or is_main_owner) else 0,
            activated_at=datetime.now().isoformat(), last_promo_at='')

def check_admin(uid: int, bot_id: str) -> bool:
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_admin') or data.get('is_owner'))

def check_owner(uid: int, bot_id: str) -> bool:
    return bool(db.get_bot_data(uid, bot_id).get('is_owner'))

def check_moderator(uid: int, bot_id: str) -> bool:
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_moderator') or data.get('is_admin') or data.get('is_owner'))

def can_send_take(uid: int, bot_id: str) -> Tuple[bool, str]:
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return False, "Ошибка"
    last = db.get_last_take_time(uid, bot_id)
    if not last:
        return True, "Можно"
    try:
        last_dt = datetime.fromisoformat(last)
    except Exception:
        return True, "Можно"
    next_avail = last_dt + timedelta(minutes=bot_cfg.take_cooldown_minutes)
    now = datetime.now()
    if now >= next_avail:
        return True, "Можно"
    rem = next_avail - now
    return False, f"Подождите {int(rem.total_seconds()//60)}м {int(rem.total_seconds()%60)}с"

def can_use_promo(uid: int, bot_id: str) -> Tuple[bool, str]:
    data = db.get_bot_data(uid, bot_id)
    activated_at = data.get('activated_at', '')
    if activated_at:
        try:
            act_dt = datetime.fromisoformat(activated_at)
            if datetime.now() - act_dt < timedelta(days=3):
                rem = (act_dt + timedelta(days=3)) - datetime.now()
                return False, f"Пиар через {int(rem.total_seconds()//3600)}ч"
        except Exception:
            pass
    last_promo = data.get('last_promo_at', '')
    if last_promo:
        try:
            lp_dt = datetime.fromisoformat(last_promo)
            if datetime.now() - lp_dt < timedelta(hours=12):
                rem = (lp_dt + timedelta(hours=12)) - datetime.now()
                return False, f"Следующий через {int(rem.total_seconds()//3600)}ч {int((rem.total_seconds()%3600)//60)}м"
        except Exception:
            pass
    return True, "Доступно"

def do_transfer(sender_id: int, receiver_id: int, bot_id: str, amount: float) -> Tuple[bool, str]:
    sd = db.get_bot_data(sender_id, bot_id)
    rd = db.get_bot_data(receiver_id, bot_id)
    if sd.get('is_frozen'):
        return False, "Ваш счёт заморожен"
    if rd.get('is_frozen'):
        return False, "Счёт получателя заморожен"
    sb = db.get_balance(sender_id, bot_id)
    if sb != float('inf') and sb < amount:
        return False, "Недостаточно средств"
    if sb != float('inf'):
        db.set_balance(sender_id, bot_id, sb - amount)
    db.add_balance(receiver_id, bot_id, amount)
    return True, "OK"

def get_exchange_rate(bot_id: str) -> float:
    if config.exchange_rates.rates_locked:
        bot_cfg = config.bots.get(bot_id)
        return bot_cfg.base_exchange_rate if bot_cfg else 0.5
    return config.exchange_rates.rates.get(bot_id, 0.5)

def do_convert(uid: int, from_bot: str, to_bot: str, amount: float) -> Tuple[bool, float, str]:
    fb = db.get_balance(uid, from_bot)
    if fb == float('inf'):
        return False, 0, "Владельцы не могут"
    if fb < amount:
        return False, 0, "Недостаточно средств"
    fr = get_exchange_rate(from_bot)
    tr = get_exchange_rate(to_bot)
    converted = amount * fr / tr
    db.set_balance(uid, from_bot, fb - amount)
    db.add_balance(uid, to_bot, converted)
    return True, converted, "OK"

def reset_all_rates():
    config.exchange_rates.rates_locked = True
    config.exchange_rates.rates = {"main": 1.0}
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != "main":
            config.exchange_rates.rates[bot_id] = bot_cfg.base_exchange_rate
    config.save()

def serialize_entities(entities) -> Optional[List[Dict]]:
    if not entities:
        return None
    return [{'type': e.type, 'offset': e.offset, 'length': e.length,
             'url': e.url, 'language': e.language, 'custom_emoji_id': e.custom_emoji_id}
            for e in entities]

def restore_entities(data: Optional[List[Dict]]) -> Optional[List[MessageEntity]]:
    if not data:
        return None
    result = [MessageEntity(type=e['type'], offset=e['offset'], length=e['length'],
              url=e.get('url'), language=e.get('language'), custom_emoji_id=e.get('custom_emoji_id'))
              for e in data]
    return result if result else None

def get_user_display_name(user_id: int) -> str:
    user = db.get_user(user_id)
    if not user:
        return "Неизвестный"
    username = user.get('username', '')
    if username and not username.startswith('user'):
        return f"@{username}"
    return user.get('name', 'Неизвестный')

BUILT_IN_QUIZ = {
    1: {"question": "Кто должен был быть на месте Ореолы?", "answers": ["Небесный", "Ледяной", "Радужный"], "correct": 0},
    2: {"question": "Кто был отцом Мракокрада?", "answers": ["Гений", "Вдумчивый", "Арктик"], "correct": 2},
    3: {"question": "Кто убивал дочерей Коралл?", "answers": ["Мальстрём", "Орка", "Акула"], "correct": 1},
}


# ====================== КЛАВИАТУРЫ ======================

def build_main_menu(bot_id: str) -> InlineKeyboardMarkup:
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return InlineKeyboardMarkup(inline_keyboard=[])
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="💰 Заработать", callback_data="earn"),
                InlineKeyboardButton(text="💸 Перевести", callback_data="transfer"))
    builder.row(InlineKeyboardButton(text="🏆 Топ", callback_data="top"),
                InlineKeyboardButton(text="💳 Баланс", callback_data="balance"))
    if "takes" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="📝 Отправить тейк", callback_data="send_take"))
    if "shop" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="🛒 Магазин", callback_data="shop"),
                    InlineKeyboardButton(text="📢 Объявление", callback_data="post_announcement"))
    builder.row(InlineKeyboardButton(text="💱 Конвертация", callback_data="convert"),
                InlineKeyboardButton(text="📊 Курсы", callback_data="rates"))
    if bot_id == "main":
        builder.row(InlineKeyboardButton(text="🤖 Подключить бота", callback_data="connect_bot"))
    return builder.as_markup()

def build_shop_menu(bot_id: str) -> InlineKeyboardMarkup:
    bot_cfg = config.bots.get(bot_id)
    builder = InlineKeyboardBuilder()
    if bot_cfg and "takes" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="📢 Пиар (10/час)", callback_data="promo_regular"))
        builder.row(InlineKeyboardButton(text="📌 Пиар с закрепом (25/час)", callback_data="promo_pinned"))
    if bot_cfg and "shop" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="🛍 Купить товар", callback_data="buy_product"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
    return builder.as_markup()

def build_admin_menu(uid: int, bot_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    bot_cfg = config.bots.get(bot_id)
    is_own = check_owner(uid, bot_id)
    is_main = uid == MAIN_ADMIN_ID and bot_id == "main"
    if is_own or is_main:
        builder.row(InlineKeyboardButton(text="📋 Пользователи", callback_data="adm_users"),
                    InlineKeyboardButton(text="💰 Списать", callback_data="adm_deduct"))
        builder.row(InlineKeyboardButton(text="❄️ Заморозить", callback_data="adm_freeze"),
                    InlineKeyboardButton(text="🔥 Разморозить", callback_data="adm_unfreeze"))
        builder.row(InlineKeyboardButton(text="🔧 Цензура", callback_data="adm_censor"),
                    InlineKeyboardButton(text="👮 Модераторы", callback_data="adm_mods"))
        if bot_cfg and "takes" in bot_cfg.modules:
            pause_label = "▶️ Включить тейки" if bot_cfg.takes_paused else "⏸ Отключить тейки"
            builder.row(InlineKeyboardButton(text=pause_label, callback_data="adm_toggle_takes"))
            manual_label = "🔓 Авто-контроль" if bot_cfg.manual_control else "🔒 Ручной контроль"
            builder.row(InlineKeyboardButton(text=manual_label, callback_data="adm_toggle_manual"))
            builder.row(InlineKeyboardButton(text="🎯 Провести викторину", callback_data="adm_channel_quiz"))
        if is_main:
            builder.row(InlineKeyboardButton(text="📊 Сбросить курсы", callback_data="adm_reset_rates"),
                        InlineKeyboardButton(text="🔄 Сбросить топ", callback_data="adm_reset_top"))
    builder.row(InlineKeyboardButton(text="💳 Баланс", callback_data="adm_balance"),
                InlineKeyboardButton(text="👤 Пользователь", callback_data="user_mode"))
    return builder.as_markup()

def build_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])

def build_censor_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Слово", callback_data="censor_add"),
                InlineKeyboardButton(text="➖ Слово", callback_data="censor_del"))
    builder.row(InlineKeyboardButton(text="➕ Маркер", callback_data="marker_add"),
                InlineKeyboardButton(text="➖ Маркер", callback_data="marker_del"))
    builder.row(InlineKeyboardButton(text="📋 Список", callback_data="censor_list"),
                InlineKeyboardButton(text="◀️ Назад", callback_data="admin_mode"))
    return builder.as_markup()

def build_mods_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Назначить", callback_data="mod_assign"),
                InlineKeyboardButton(text="➖ Снять", callback_data="mod_remove"))
    builder.row(InlineKeyboardButton(text="📋 Список", callback_data="mod_list"),
                InlineKeyboardButton(text="◀️ Назад", callback_data="admin_mode"))
    return builder.as_markup()

def build_currency_keyboard(exclude: str = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != exclude:
            builder.row(InlineKeyboardButton(text=f"{bot_cfg.currency_name} {bot_cfg.currency_emoji}", callback_data=f"currency_{bot_id}"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()

def build_modules_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Только тейки", callback_data="module_takes"))
    builder.row(InlineKeyboardButton(text="🛒 Только магазин", callback_data="module_shop"))
    builder.row(InlineKeyboardButton(text="📝🛒 Всё", callback_data="module_all"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()

def build_take_moderation_keyboard(take_id: str, uid: int, is_blocked: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✅ Отправить", callback_data=f"take_approve_{take_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"take_reject_{take_id}"))
    if is_blocked:
        builder.row(InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"user_unblock_{uid}"))
    else:
        builder.row(InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"user_block_{uid}"))
    return builder.as_markup()

def build_promo_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Оплатить", callback_data="promo_pay"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])

def build_quiz_keyboard(question_num: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, answer in enumerate(BUILT_IN_QUIZ[question_num]["answers"]):
        builder.row(InlineKeyboardButton(text=answer, callback_data=f"quiz_{question_num}_{i}"))
    return builder.as_markup()


# ====================== ПЕРЕСЫЛКА ТЕЙКА ======================

async def forward_take_to_channel(message: types.Message, bot_id: str, bot_instance: Bot) -> Optional[types.Message]:
    try:
        bot_cfg = config.bots.get(bot_id)
        if not bot_cfg or not bot_cfg.takes_channel:
            return None
        text = message.text or message.caption or ""
        censored, has_profanity = censor_profanity(text, bot_id)
        kw = {"caption": censored if has_profanity else text, "parse_mode": "HTML" if has_profanity else None}
        if message.photo:
            return await bot_instance.send_photo(bot_cfg.takes_channel, photo=message.photo[-1].file_id, **kw)
        elif message.video:
            return await bot_instance.send_video(bot_cfg.takes_channel, video=message.video.file_id, **kw)
        elif message.animation:
            return await bot_instance.send_animation(bot_cfg.takes_channel, animation=message.animation.file_id, **kw)
        elif message.document:
            return await bot_instance.send_document(bot_cfg.takes_channel, document=message.document.file_id, **kw)
        elif message.voice:
            return await bot_instance.send_voice(bot_cfg.takes_channel, voice=message.voice.file_id, **kw)
        elif message.audio:
            return await bot_instance.send_audio(bot_cfg.takes_channel, audio=message.audio.file_id, **kw)
        elif message.sticker:
            return await bot_instance.send_sticker(bot_cfg.takes_channel, sticker=message.sticker.file_id)
        else:
            return await bot_instance.send_message(bot_cfg.takes_channel, censored if has_profanity else text, parse_mode="HTML" if has_profanity else None)
    except Exception as e:
        logger.error(f"Ошибка пересылки тейка: {e}")
        return None


# ====================== ОТЛОЖЕННОЕ УДАЛЕНИЕ ======================

async def delayed_delete_message(bot_instance: Bot, channel: str, message_id: int, hours: float, is_pinned: bool, deletion_id: str):
    try:
        await asyncio.sleep(hours * 3600)
        if is_pinned:
            try:
                await bot_instance.unpin_chat_message(channel, message_id)
            except Exception:
                pass
        try:
            await bot_instance.delete_message(channel, message_id)
        except Exception:
            pass
        if deletion_id in config.scheduled_deletions:
            del config.scheduled_deletions[deletion_id]
            config.save()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ошибка удаления: {e}")


# ====================== АУКЦИОН ======================

async def run_auction_timer(bot_instance: Bot, bot_id: str, auction_id: str):
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return
    try:
        # Ждём первой ставки
        while True:
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if auction.get('current_bidder') is not None:
                break
            await asyncio.sleep(5)

        # Основной цикл отсчёта
        while True:
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            channel_id = auction['channel']
            discussion_id = auction.get('discussion_chat_id')
            discussion_message_id = auction.get('discussion_message_id')
            message_id = auction['message_id']
            last_bid_time = datetime.fromisoformat(auction['last_bid_time'])
            snapshot_time = last_bid_time

            async def send_countdown(text: str):
                if discussion_id and discussion_message_id:
                    try:
                        await bot_instance.send_message(chat_id=discussion_id, text=text, reply_to_message_id=discussion_message_id)
                        return
                    except Exception as e:
                        logger.error(f"Ошибка reply в группу: {e}")
                if discussion_id:
                    try:
                        await bot_instance.send_message(chat_id=discussion_id, text=text)
                        return
                    except Exception as e:
                        logger.error(f"Ошибка в группу: {e}")
                try:
                    await bot_instance.send_message(chat_id=channel_id, text=text, reply_to_message_id=message_id)
                except Exception as e:
                    logger.error(f"Ошибка отсчёта {text}: {e}")

            wait_2min = last_bid_time + timedelta(minutes=2)
            now = datetime.now()
            if now < wait_2min:
                await asyncio.sleep((wait_2min - now).total_seconds())

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            discussion_message_id = auction.get('discussion_message_id')
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            await send_countdown("3")
            await asyncio.sleep(30)
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            await send_countdown("2")
            await asyncio.sleep(30)
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            await send_countdown("1")
            await asyncio.sleep(30)
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            # Победитель
            winner_id = auction.get('current_bidder')
            winner_amount = auction.get('current_bid', 0)
            auction['finished'] = True
            config.save()

            if winner_id:
                winner_display = get_user_display_name(winner_id)
                winner_balance = db.get_balance(winner_id, bot_id)
                if winner_balance != float('inf') and winner_balance < winner_amount:
                    target = discussion_id if discussion_id else channel_id
                    try:
                        await bot_instance.send_message(chat_id=target, text=f"⚠️ У {winner_display} недостаточно средств ({winner_amount} {bot_cfg.currency_emoji}). Аукцион отменён.")
                    except Exception:
                        pass
                else:
                    db.deduct_balance(winner_id, bot_id, winner_amount)
                    try:
                        await bot_instance.send_message(chat_id=bot_cfg.takes_channel, text=f"Победитель: {winner_display}")
                    except Exception as e:
                        logger.error(f"Ошибка объявления: {e}")
                    try:
                        await bot_instance.send_message(winner_id, f"🏆 Вы выиграли аукцион!\n💰 Списано: {winner_amount} {bot_cfg.currency_emoji}")
                    except Exception:
                        pass

            if auction_id in config.active_auctions:
                del config.active_auctions[auction_id]
                config.save()
            return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ошибка аукциона: {e}")

# ====================== ОБРАБОТЧИКИ БОТА ======================

def create_bot_handlers(bot_id: str, bot_instance: Bot, dp: Dispatcher):
    router = Router()
    bot_config = config.bots.get(bot_id)

    @router.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        await state.clear()
        register_user(message.from_user, bot_id)
        cfg = config.bots.get(bot_id)
        if check_admin(message.from_user.id, bot_id):
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="👤 Пользователь", callback_data="user_mode"),
                InlineKeyboardButton(text="⚙️ Админ", callback_data="admin_mode")]])
            await message.answer(f"👋 Привет, администратор!\nВалюта: {cfg.currency_name} {cfg.currency_emoji}", reply_markup=kb)
        else:
            await message.answer(f"👋 Добро пожаловать!\nВалюта: {cfg.currency_name} {cfg.currency_emoji}", reply_markup=build_main_menu(bot_id))

    @router.message(Command("cancel"))
    async def cmd_cancel(message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("Отменено.", reply_markup=build_main_menu(bot_id))

    @router.message(Command("logs"))
    async def cmd_logs(message: types.Message):
        if message.from_user.id not in ADMIN_IDS:
            return
        try:
            if os.path.exists('bot.log'):
                with open('bot.log', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                last_lines = ''.join(lines[-50:])
                if len(last_lines) > 4000:
                    last_lines = last_lines[-4000:]
                await message.answer(f"📋 Последние логи:\n\n{last_lines}")
            else:
                await message.answer("Файл логов не найден.")
        except Exception as e:
            await message.answer(f"Ошибка: {e}")

    @router.callback_query(F.data == "cancel")
    async def cb_cancel(callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Отменено.", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "user_mode")
    async def cb_user(callback: types.CallbackQuery):
        await callback.message.edit_text("Меню:", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "admin_mode")
    async def cb_admin(callback: types.CallbackQuery):
        if check_admin(callback.from_user.id, bot_id):
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
        await callback.answer()

    @router.callback_query(F.data == "back_main")
    async def cb_back(callback: types.CallbackQuery):
        await callback.message.edit_text("Меню:", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "balance")
    async def cb_balance(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        ud = db.get_bot_data(callback.from_user.id, bot_id)
        status = ""
        if ud.get('is_frozen'):
            status += "❄️ Счёт заморожен\n"
        if ud.get('is_blocked'):
            status += "🚫 Заблокирован для тейков\n"
        bal = db.get_balance(callback.from_user.id, bot_id)
        bal_str = "∞" if bal == float('inf') else f"{bal:.0f}"
        text = f"{status}💳 Балансы:\n\n▸ {cfg.currency_name} {cfg.currency_emoji}: {bal_str} (текущий)\n"
        for other_id, other_cfg in config.bots.items():
            if other_id != bot_id:
                ob = db.get_balance(callback.from_user.id, other_id)
                if ob > 0 or ob == float('inf'):
                    text += f"▸ {other_cfg.currency_name} {other_cfg.currency_emoji}: {'∞' if ob == float('inf') else f'{ob:.0f}'}\n"
        can_take, cm = can_send_take(callback.from_user.id, bot_id)
        text += f"\n📝 Тейки: {'✅' if can_take else f'⏳ {cm}'}"
        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "top")
    async def cb_top(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        ul = db.get_all_users_for_bot(bot_id)
        filtered = [(u['username'], u['balance']) for u in ul if u.get('show_in_top') and not u.get('is_owner') and not u.get('is_infinite')]
        filtered.sort(key=lambda x: x[1], reverse=True)
        text = f"🏆 Топ {cfg.currency_name}:\n\n"
        for i, (un, b) in enumerate(filtered[:10], 1):
            text += f"{i}. @{un} — {b:.0f} {cfg.currency_emoji}\n"
        if not filtered:
            text += "Пока пусто"
        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "transfer")
    async def cb_transfer(callback: types.CallbackQuery, state: FSMContext):
        ud = db.get_bot_data(callback.from_user.id, bot_id)
        if ud.get('is_frozen'):
            await callback.answer("❄️ Заморожен!", show_alert=True)
            return
        await callback.message.edit_text("Введите username или ID получателя:", reply_markup=build_cancel_keyboard())
        await state.set_state(TransferStates.WaitingReceiver)
        await callback.answer()

    @router.message(TransferStates.WaitingReceiver)
    async def transfer_recv(message: types.Message, state: FSMContext):
        rid = db.find_user_by_input(message.text)
        if not rid:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
            return
        if rid == message.from_user.id:
            await message.answer("Нельзя себе.", reply_markup=build_cancel_keyboard())
            return
        rd = db.get_bot_data(rid, bot_id)
        if rd.get('is_frozen'):
            await message.answer("❄️ Заморожен.", reply_markup=build_cancel_keyboard())
            return
        await state.update_data(receiver_id=rid)
        await message.answer("Сумма (сообщение на новой строке):", reply_markup=build_cancel_keyboard())
        await state.set_state(TransferStates.WaitingAmountAndMessage)

    @router.message(TransferStates.WaitingAmountAndMessage)
    async def transfer_amt(message: types.Message, state: FSMContext):
        lines = message.text.strip().split('\n', 1)
        try:
            amount = float(lines[0])
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Неверная сумма.", reply_markup=build_cancel_keyboard())
            return
        tmsg = lines[1].strip() if len(lines) > 1 else ""
        data = await state.get_data()
        rid = data['receiver_id']
        cfg = config.bots.get(bot_id)
        ok, err = do_transfer(message.from_user.id, rid, bot_id, amount)
        if ok:
            rv = db.get_user(rid)
            sn = db.get_user(message.from_user.id)
            await message.answer(f"✅ {amount:.0f} {cfg.currency_emoji} → @{rv['username']}", reply_markup=build_main_menu(bot_id))
            notif = f"💰 {amount:.0f} {cfg.currency_emoji} от @{sn['username']}"
            if tmsg:
                notif += f"\n💬 {tmsg}"
            try:
                await bot_instance.send_message(rid, notif)
            except Exception:
                pass
        else:
            await message.answer(f"❌ {err}", reply_markup=build_main_menu(bot_id))
        await state.clear()

    @router.callback_query(F.data == "rates")
    async def cb_rates(callback: types.CallbackQuery):
        text = "📊 Курсы:\n\n"
        for bid, bc in config.bots.items():
            text += f"{bc.currency_name} {bc.currency_emoji}: {get_exchange_rate(bid):.2f}\n"
        if config.exchange_rates.rates_locked:
            text += "\n🔒 Зафиксированы"
        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "convert")
    async def cb_conv(callback: types.CallbackQuery, state: FSMContext):
        if db.get_balance(callback.from_user.id, bot_id) == float('inf'):
            await callback.answer("Нельзя", show_alert=True)
            return
        await callback.message.edit_text("ИЗ:", reply_markup=build_currency_keyboard())
        await state.set_state(ConvertStates.WaitingSource)
        await callback.answer()

    @router.callback_query(ConvertStates.WaitingSource, F.data.startswith("currency_"))
    async def conv_src(callback: types.CallbackQuery, state: FSMContext):
        s = callback.data[9:]
        await state.update_data(source_bot=s)
        await callback.message.edit_text("В:", reply_markup=build_currency_keyboard(s))
        await state.set_state(ConvertStates.WaitingTarget)
        await callback.answer()

    @router.callback_query(ConvertStates.WaitingTarget, F.data.startswith("currency_"))
    async def conv_tgt(callback: types.CallbackQuery, state: FSMContext):
        t = callback.data[9:]
        await state.update_data(target_bot=t)
        data = await state.get_data()
        sc = config.bots.get(data['source_bot'])
        tc = config.bots.get(t)
        sr = get_exchange_rate(data['source_bot'])
        tr = get_exchange_rate(t)
        bal = db.get_balance(callback.from_user.id, data['source_bot'])
        await callback.message.edit_text(
            f"1{sc.currency_emoji}={sr/tr:.2f}{tc.currency_emoji}\nБаланс:{bal:.0f}\n\nСумма:",
            reply_markup=build_cancel_keyboard())
        await state.set_state(ConvertStates.WaitingAmount)
        await callback.answer()

    @router.message(ConvertStates.WaitingAmount)
    async def conv_amt(message: types.Message, state: FSMContext):
        try:
            a = float(message.text.strip())
            if a <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Неверно.", reply_markup=build_cancel_keyboard())
            return
        data = await state.get_data()
        ok, cv, err = do_convert(message.from_user.id, data['source_bot'], data['target_bot'], a)
        sc = config.bots.get(data['source_bot'])
        tc = config.bots.get(data['target_bot'])
        if ok:
            await message.answer(f"✅ {a:.0f}{sc.currency_emoji}→{cv:.0f}{tc.currency_emoji}", reply_markup=build_main_menu(bot_id))
        else:
            await message.answer(f"❌ {err}", reply_markup=build_main_menu(bot_id))
        await state.clear()

    @router.callback_query(F.data == "earn")
    async def cb_earn(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        builder = InlineKeyboardBuilder()
        if cfg.channel_url:
            builder.row(InlineKeyboardButton(text="📢 Канал", url=cfg.channel_url))
        builder.row(InlineKeyboardButton(text="❓ Викторина", callback_data="quiz_start"))
        builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
        await callback.message.edit_text("Заработок:", reply_markup=builder.as_markup())
        await callback.answer()

    @router.callback_query(F.data == "quiz_start")
    async def cb_quiz_start(callback: types.CallbackQuery, state: FSMContext):
        ud = db.get_bot_data(callback.from_user.id, bot_id)
        if ud.get('quiz_passed'):
            await callback.answer("Уже пройдена", show_alert=True)
            return
        await callback.message.edit_text(f"Вопрос 1:\n{BUILT_IN_QUIZ[1]['question']}", reply_markup=build_quiz_keyboard(1))
        await state.set_state(QuizStates.Question1)
        await callback.answer()

    @router.callback_query(QuizStates.Question1, F.data.startswith("quiz_1_"))
    async def quiz_q1(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[1]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            await callback.message.edit_text(f"✅ +{cfg.quiz_reward}\n\nВопрос 2:\n{BUILT_IN_QUIZ[2]['question']}", reply_markup=build_quiz_keyboard(2))
            await state.set_state(QuizStates.Question2)
        else:
            await callback.answer("❌", show_alert=True)
            await callback.message.edit_text("Неверно.", reply_markup=build_main_menu(bot_id))
            await state.clear()
        await callback.answer()

    @router.callback_query(QuizStates.Question2, F.data.startswith("quiz_2_"))
    async def quiz_q2(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[2]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            await callback.message.edit_text(f"✅ +{cfg.quiz_reward}\n\nВопрос 3:\n{BUILT_IN_QUIZ[3]['question']}", reply_markup=build_quiz_keyboard(3))
            await state.set_state(QuizStates.Question3)
        else:
            await callback.answer("❌", show_alert=True)
            await callback.message.edit_text("Неверно.", reply_markup=build_main_menu(bot_id))
            await state.clear()
        await callback.answer()

    @router.callback_query(QuizStates.Question3, F.data.startswith("quiz_3_"))
    async def quiz_q3(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[3]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            db.set_bot_data(callback.from_user.id, bot_id, quiz_passed=1)
            await callback.message.edit_text(f"🎉 +{cfg.quiz_reward*3} {cfg.currency_emoji}", reply_markup=build_main_menu(bot_id))
        else:
            await callback.answer("❌", show_alert=True)
            await callback.message.edit_text("Неверно.", reply_markup=build_main_menu(bot_id))
        await state.clear()
        await callback.answer()

    @router.callback_query(F.data == "post_announcement")
    async def cb_post_announcement(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        logger.info(f"Объявление: bot_id={bot_id}, channel='{cfg.announcement_channel if cfg else 'None'}'")
        if not cfg or not cfg.announcement_channel:
            await callback.answer("Канал для объявлений не настроен.", show_alert=True)
            return
        await callback.message.edit_text(
            "📢 Отправьте объявление.\n\nМожно текст, фото, видео и т.д.\nПремиум эмодзи сохранятся.",
            reply_markup=build_cancel_keyboard())
        await state.set_state(AnnouncementStates.WaitingAnnouncement)
        await callback.answer()

    @router.message(AnnouncementStates.WaitingAnnouncement)
    async def process_announcement(message: types.Message, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg or not cfg.announcement_channel:
            await message.answer("Канал не настроен.", reply_markup=build_main_menu(bot_id))
            await state.clear()
            return
        try:
            await bot_instance.copy_message(
                chat_id=cfg.announcement_channel,
                from_chat_id=message.chat.id,
                message_id=message.message_id)
            await message.answer("✅ Объявление опубликовано!", reply_markup=build_main_menu(bot_id))
            logger.info(f"Объявление от {message.from_user.id} в {cfg.announcement_channel}")
        except Exception as e:
            logger.error(f"Ошибка объявления: {e}")
            await message.answer(f"❌ Ошибка: {e}", reply_markup=build_main_menu(bot_id))
        await state.clear()

    if bot_config and "takes" in bot_config.modules:

        async def process_take_message(message: types.Message, bid: str, bot: Bot):
            uid = message.from_user.id
            register_user(message.from_user, bid)
            cfg = config.bots.get(bid)
            ud = db.get_bot_data(uid, bid)
            if ud.get('is_blocked'):
                await message.answer("🚫 Заблокированы.")
                return False
            can_take, cm = can_send_take(uid, bid)
            if not can_take:
                await message.answer(f"⏳ {cm}")
                return False
            text = message.text or message.caption or ""
            if cfg.takes_paused:
                take_data = {
                    'user_id': uid, 'bot_id': bid, 'text': text,
                    'photo': message.photo[-1].file_id if message.photo else None,
                    'video': message.video.file_id if message.video else None,
                    'animation': message.animation.file_id if message.animation else None,
                    'document': message.document.file_id if message.document else None,
                    'caption': message.caption, 'timestamp': datetime.now().isoformat()
                }
                if bid not in config.paused_takes:
                    config.paused_takes[bid] = []
                config.paused_takes[bid].append(take_data)
                config.save()
                db.add_take_timestamp(uid, bid)
                await message.answer("⏸ На паузе.", reply_markup=build_main_menu(bid))
                return True
            needs_mod = cfg.manual_control
            mod_reason = "Ручной контроль"
            if not needs_mod and contains_marker_words(text, bid):
                needs_mod = True
                mod_reason = "Маркерное слово"
            if not needs_mod:
                hg, lr = await check_telegram_links(text, bot)
                if hg:
                    needs_mod = True
                    mod_reason = lr
            if needs_mod:
                take_id = str(uuid.uuid4())[:8]
                config.pending_takes[take_id] = {
                    'user_id': uid, 'bot_id': bid, 'text': text,
                    'photo': message.photo[-1].file_id if message.photo else None,
                    'video': message.video.file_id if message.video else None,
                    'animation': message.animation.file_id if message.animation else None,
                    'document': message.document.file_id if message.document else None,
                    'voice': message.voice.file_id if message.voice else None,
                    'audio': message.audio.file_id if message.audio else None,
                    'sticker': message.sticker.file_id if message.sticker else None,
                    'caption': message.caption
                }
                config.save()
                all_users = db.get_all_users_for_bot(bid)
                for user in all_users:
                    mod_uid = user['user_id']
                    if check_moderator(mod_uid, bid):
                        try:
                            is_blocked = db.get_bot_data(uid, bid).get('is_blocked', 0)
                            await bot.send_message(mod_uid, f"⚠️ Модерация\nПричина: {mod_reason}\n\n{text}",
                                reply_markup=build_take_moderation_keyboard(take_id, uid, bool(is_blocked)))
                            await message.copy_to(mod_uid)
                        except Exception as e:
                            logger.error(f"Ошибка модератору {mod_uid}: {e}")
                await message.answer("📝 На модерации.", reply_markup=build_main_menu(bid))
                return True
            else:
                sent = await forward_take_to_channel(message, bid, bot)
                if sent:
                    db.add_take_timestamp(uid, bid)
                    await message.answer("✅ Отправлено!", reply_markup=build_main_menu(bid))
                    return True
                await message.answer("❌ Ошибка.", reply_markup=build_main_menu(bid))
                return False

        @router.callback_query(F.data == "send_take")
        async def cb_send_take(callback: types.CallbackQuery, state: FSMContext):
            ud = db.get_bot_data(callback.from_user.id, bot_id)
            if ud.get('is_blocked'):
                await callback.answer("🚫", show_alert=True)
                return
            can_take, cm = can_send_take(callback.from_user.id, bot_id)
            if not can_take:
                await callback.answer(f"⏳ {cm}", show_alert=True)
                return
            cfg = config.bots.get(bot_id)
            pt = " ⏸" if cfg.takes_paused else ""
            await callback.message.edit_text(
                f"📝 Тейк с #тейк{pt}\n⏱ {cfg.take_cooldown_minutes} мин",
                reply_markup=build_cancel_keyboard())
            await state.set_state(TakeStates.WaitingTake)
            await callback.answer()

        @router.message(TakeStates.WaitingTake)
        async def take_btn(message: types.Message, state: FSMContext):
            text = message.text or message.caption or ""
            if "#тейк" not in text.lower():
                await message.answer("⚠️ #тейк!", reply_markup=build_cancel_keyboard())
                return
            await process_take_message(message, bot_id, bot_instance)
            await state.clear()

        @router.message(F.text.contains("#тейк") | F.caption.contains("#тейк"))
        async def auto_take(message: types.Message, state: FSMContext):
            if message.chat.type == "channel":
                return
            cs = await state.get_state()
            if cs == TakeStates.WaitingTake:
                return
            await process_take_message(message, bot_id, bot_instance)

        @router.callback_query(F.data.startswith("take_approve_"))
        async def take_approve(callback: types.CallbackQuery):
            take_id = callback.data[13:]
            td = config.pending_takes.get(take_id)
            if not td:
                await callback.answer("Нет", show_alert=True)
                return
            cfg = config.bots.get(td['bot_id'])
            try:
                text = td.get('caption') or td.get('text', '')
                c, hp = censor_profanity(text, bot_id)
                kw = {"caption": c if hp else text, "parse_mode": "HTML" if hp else None}
                if td.get('photo'):
                    await bot_instance.send_photo(cfg.takes_channel, photo=td['photo'], **kw)
                elif td.get('video'):
                    await bot_instance.send_video(cfg.takes_channel, video=td['video'], **kw)
                elif td.get('animation'):
                    await bot_instance.send_animation(cfg.takes_channel, animation=td['animation'], **kw)
                elif td.get('document'):
                    await bot_instance.send_document(cfg.takes_channel, document=td['document'], **kw)
                elif td.get('voice'):
                    await bot_instance.send_voice(cfg.takes_channel, voice=td['voice'], **kw)
                elif td.get('audio'):
                    await bot_instance.send_audio(cfg.takes_channel, audio=td['audio'], **kw)
                elif td.get('sticker'):
                    await bot_instance.send_sticker(cfg.takes_channel, sticker=td['sticker'])
                else:
                    await bot_instance.send_message(cfg.takes_channel, c if hp else td['text'], parse_mode="HTML" if hp else None)
                db.add_take_timestamp(td['user_id'], bot_id)
                del config.pending_takes[take_id]
                config.save()
                await callback.message.edit_text("✅ Одобрен.")
                try:
                    await bot_instance.send_message(td['user_id'], "✅ Тейк одобрен!")
                except Exception:
                    pass
            except Exception as e:
                await callback.answer(f"Ошибка: {e}", show_alert=True)
            await callback.answer()

        @router.callback_query(F.data.startswith("take_reject_"))
        async def take_reject(callback: types.CallbackQuery):
            take_id = callback.data[12:]
            td = config.pending_takes.get(take_id)
            if td:
                try:
                    await bot_instance.send_message(td['user_id'], "❌ Отклонён.")
                except Exception:
                    pass
                del config.pending_takes[take_id]
                config.save()
            await callback.message.edit_text("❌ Отклонён.")
            await callback.answer()

        @router.callback_query(F.data.startswith("user_block_"))
        async def block_u(callback: types.CallbackQuery):
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет", show_alert=True)
                return
            uid = int(callback.data[11:])
            db.set_bot_data(uid, bot_id, is_blocked=1)
            try:
                await bot_instance.send_message(uid, "🚫 Заблокированы.")
            except Exception:
                pass
            await callback.answer("🚫", show_alert=True)

        @router.callback_query(F.data.startswith("user_unblock_"))
        async def unblock_u(callback: types.CallbackQuery):
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет", show_alert=True)
                return
            uid = int(callback.data[13:])
            db.set_bot_data(uid, bot_id, is_blocked=0)
            try:
                await bot_instance.send_message(uid, "✅ Разблокированы.")
            except Exception:
                pass
            await callback.answer("✅", show_alert=True)

    if bot_config and "shop" in bot_config.modules:

        @router.callback_query(F.data == "shop")
        async def cb_shop(callback: types.CallbackQuery):
            await callback.message.edit_text("🛒 Магазин:", reply_markup=build_shop_menu(bot_id))
            await callback.answer()

        @router.message(F.text.contains("#продажа") | F.caption.contains("#продажа") | F.text.contains("#обмен") | F.caption.contains("#обмен"))
        async def auto_shop(message: types.Message, state: FSMContext):
            cs = await state.get_state()
            if cs:
                return
            cfg = config.bots.get(bot_id)
            try:
                if message.photo:
                    await bot_instance.send_photo(cfg.shop_channel, photo=message.photo[-1].file_id, caption=message.caption)
                else:
                    await bot_instance.send_message(cfg.shop_channel, message.text)
                await message.answer("✅ Отправлено!")
            except Exception as e:
                logger.error(f"Автопересылка: {e}")

        @router.callback_query(F.data.startswith("promo_"))
        async def cb_promo(callback: types.CallbackQuery, state: FSMContext):
            if callback.data == "promo_pay":
                data = await state.get_data()
                tot = data.get('total_cost', 0)
                cfg = config.bots.get(bot_id)
                if not db.deduct_balance(callback.from_user.id, bot_id, tot):
                    await callback.answer("Мало!", show_alert=True)
                    return
                post = data.get('post_data', {})
                try:
                    sent = None
                    if post.get('is_forwarded'):
                        sent = await bot_instance.copy_message(
                            chat_id=cfg.takes_channel,
                            from_chat_id=post['forward_chat_id'],
                            message_id=post['forward_message_id'])
                    elif post.get('photo'):
                        sent = await bot_instance.send_photo(cfg.takes_channel, photo=post['photo'],
                            caption=post.get('caption'), caption_entities=restore_entities(post.get('caption_entities')))
                    elif post.get('text'):
                        sent = await bot_instance.send_message(cfg.takes_channel, post['text'],
                            entities=restore_entities(post.get('entities')))
                    if not sent:
                        raise Exception("Не отправлено")
                    pin = data.get('is_pinned', False)
                    if pin:
                        try:
                            await bot_instance.pin_chat_message(cfg.takes_channel, sent.message_id)
                        except Exception:
                            pass
                    h = data.get('hours', 1)
                    da = (datetime.now() + timedelta(hours=h)).isoformat()
                    did = f"{bot_id}_{sent.message_id}"
                    config.scheduled_deletions[did] = {'bot_id': bot_id, 'channel': cfg.takes_channel, 'message_id': sent.message_id, 'delete_at': da, 'is_pinned': pin}
                    config.save()
                    asyncio.create_task(delayed_delete_message(bot_instance, cfg.takes_channel, sent.message_id, h, pin, did))
                    db.set_bot_data(callback.from_user.id, bot_id, last_promo_at=datetime.now().isoformat())
                    pt = "📌 " if pin else ""
                    await callback.message.edit_text(f"✅ {pt}Пиар {h}ч!", reply_markup=build_main_menu(bot_id))
                except Exception as e:
                    db.add_balance(callback.from_user.id, bot_id, tot)
                    await callback.message.edit_text(f"❌ {e}", reply_markup=build_main_menu(bot_id))
                await state.clear()
                await callback.answer()
                return
            pin = callback.data == "promo_pinned"
            ok, pm = can_use_promo(callback.from_user.id, bot_id)
            if not ok:
                await callback.answer(pm, show_alert=True)
                return
            cfg = config.bots.get(bot_id)
            pr = cfg.promo_pin_price_per_hour if pin else cfg.promo_price_per_hour
            await state.update_data(is_pinned=pin, price_per_hour=pr)
            await callback.message.edit_text(
                f"{'📌' if pin else '📢'} {pr} {cfg.currency_emoji}/час\nЧасов?",
                reply_markup=build_cancel_keyboard())
            await state.set_state(PromoStates.WaitingHours)
            await callback.answer()

        @router.message(PromoStates.WaitingHours)
        async def promo_h(message: types.Message, state: FSMContext):
            try:
                h = int(message.text)
                if h <= 0:
                    raise ValueError
            except ValueError:
                await message.answer("Число!", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(hours=h)
            await message.answer("Пост:", reply_markup=build_cancel_keyboard())
            await state.set_state(PromoStates.WaitingPost)

        @router.message(PromoStates.WaitingPost)
        async def promo_p(message: types.Message, state: FSMContext):
            post = {
                'text': message.text, 'caption': message.caption,
                'photo': message.photo[-1].file_id if message.photo else None,
                'entities': serialize_entities(message.entities),
                'caption_entities': serialize_entities(message.caption_entities),
                'forward_chat_id': message.chat.id, 'forward_message_id': message.message_id,
                'is_forwarded': message.forward_origin is not None}
            data = await state.get_data()
            tot = data['hours'] * data['price_per_hour']
            await state.update_data(post_data=post, total_cost=tot)
            cfg = config.bots.get(bot_id)
            bal = db.get_balance(message.from_user.id, bot_id)
            bs = "∞" if bal == float('inf') else f"{bal:.0f}"
            fwd = "\n📎 Пересланное" if post['is_forwarded'] else ""
            await message.answer(
                f"{'📌' if data['is_pinned'] else '📢'} {data['hours']}ч\n💰 {tot} {cfg.currency_emoji}\n💳 {bs}{fwd}",
                reply_markup=build_promo_confirm_keyboard())
            await state.set_state(PromoStates.WaitingConfirmation)

        @router.callback_query(F.data == "buy_product")
        async def cb_buy(callback: types.CallbackQuery, state: FSMContext):
            await callback.message.edit_text("🛍 Фото:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingImage)
            await callback.answer()

        @router.message(PurchaseStates.WaitingImage)
        async def buy_i(message: types.Message, state: FSMContext):
            if not message.photo:
                await message.answer("Фото!", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(photo_id=message.photo[-1].file_id)
            await message.answer("Продавец:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingSeller)

        @router.message(PurchaseStates.WaitingSeller)
        async def buy_s(message: types.Message, state: FSMContext):
            sid = db.find_user_by_input(message.text)
            if not sid:
                await message.answer("Нет.", reply_markup=build_cancel_keyboard())
                return
            if sid == message.from_user.id:
                await message.answer("Нельзя.", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(seller_id=sid)
            await message.answer("Сумма:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingAmount)

        @router.message(PurchaseStates.WaitingAmount)
        async def buy_a(message: types.Message, state: FSMContext):
            try:
                a = float(message.text)
                if a <= 0:
                    raise ValueError
            except ValueError:
                await message.answer("Неверно.", reply_markup=build_cancel_keyboard())
                return
            data = await state.get_data()
            pid = str(uuid.uuid4())[:8]
            buyer = db.get_user(message.from_user.id)
            config.pending_purchases[pid] = {'buyer_id': message.from_user.id, 'seller_id': data['seller_id'], 'amount': a, 'photo_id': data['photo_id'], 'bot_id': bot_id}
            config.save()
            cfg = config.bots.get(bot_id)
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅", callback_data=f"purchase_ok_{pid}"),
                InlineKeyboardButton(text="❌", callback_data=f"purchase_no_{pid}")]])
            try:
                await bot_instance.send_photo(data['seller_id'], photo=data['photo_id'],
                    caption=f"🛒 @{buyer['username']}\n{a} {cfg.currency_emoji}", reply_markup=kb)
                await message.answer("✅ Запрос!", reply_markup=build_main_menu(bot_id))
            except Exception:
                del config.pending_purchases[pid]
                config.save()
                await message.answer("❌", reply_markup=build_main_menu(bot_id))
            await state.clear()

        @router.callback_query(F.data.startswith("purchase_ok_"))
        async def pur_ok(callback: types.CallbackQuery):
            pid = callback.data[12:]
            p = config.pending_purchases.get(pid)
            if not p:
                await callback.answer("Нет", show_alert=True)
                return
            if callback.from_user.id != p['seller_id']:
                await callback.answer("Не ваше", show_alert=True)
                return
            cfg = config.bots.get(p['bot_id'])
            ok, err = do_transfer(p['buyer_id'], p['seller_id'], p['bot_id'], p['amount'])
            if ok:
                await callback.message.edit_caption(caption=f"✅ +{p['amount']} {cfg.currency_emoji}")
                try:
                    await bot_instance.send_message(p['buyer_id'], f"✅ -{p['amount']} {cfg.currency_emoji}")
                except Exception:
                    pass
            else:
                await callback.answer(err, show_alert=True)
            del config.pending_purchases[pid]
            config.save()
            await callback.answer()

        @router.callback_query(F.data.startswith("purchase_no_"))
        async def pur_no(callback: types.CallbackQuery):
            pid = callback.data[12:]
            p = config.pending_purchases.get(pid)
            if p:
                try:
                    await bot_instance.send_message(p['buyer_id'], "❌ Отклонено.")
                except Exception:
                    pass
                del config.pending_purchases[pid]
                config.save()
            await callback.message.edit_caption(caption="❌")
            await callback.answer()

    dp.include_router(router)

def create_admin_and_channel_handlers(bot_id: str, bot_instance: Bot, dp: Dispatcher):
    router = Router()
    bot_config = config.bots.get(bot_id)

    # =================== АДМИН-ПАНЕЛЬ ===================

    @router.callback_query(F.data == "adm_users")
    async def adm_users(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        ul = db.get_all_users_for_bot(bot_id)
        text = "👥 Пользователи:\n\n"
        for u in ul[:20]:
            bs = "∞" if u.get('is_infinite') else f"{u['balance']:.0f}"
            ud = db.get_bot_data(u['user_id'], bot_id)
            fl = ""
            if ud.get('is_frozen'):
                fl += "❄️"
            if ud.get('is_blocked'):
                fl += "🚫"
            if ud.get('is_moderator'):
                fl += "👮"
            text += f"@{u['username']}: {bs} {cfg.currency_emoji} {fl}\n"
        await callback.message.edit_text(text, reply_markup=build_admin_menu(callback.from_user.id, bot_id))
        await callback.answer()

    @router.callback_query(F.data == "adm_deduct")
    async def adm_deduct(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Username для списания:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForDeduct)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForDeduct)
    async def deduct_user(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if not uid:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
            return
        await state.update_data(target_uid=uid)
        cfg = config.bots.get(bot_id)
        bal = db.get_balance(uid, bot_id)
        await message.answer(f"Баланс: {bal:.0f} {cfg.currency_emoji}\nСколько?", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingAmountForDeduct)

    @router.message(AdminStates.WaitingAmountForDeduct)
    async def deduct_amount(message: types.Message, state: FSMContext):
        try:
            amt = float(message.text)
        except ValueError:
            await message.answer("Число!", reply_markup=build_cancel_keyboard())
            return
        data = await state.get_data()
        cfg = config.bots.get(bot_id)
        if db.deduct_balance(data['target_uid'], bot_id, amt):
            await message.answer(f"✅ -{amt} {cfg.currency_emoji}", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("❌ Ошибка.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        await state.clear()

    @router.callback_query(F.data == "adm_freeze")
    async def adm_freeze(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Username для заморозки:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForFreeze)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForFreeze)
    async def freeze_user(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_frozen=1)
            await message.answer("❄️ Заморожен.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "adm_unfreeze")
    async def adm_unfreeze(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Username для разморозки:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForUnfreeze)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForUnfreeze)
    async def unfreeze_user(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_frozen=0)
            await message.answer("🔥 Разморожен.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "adm_toggle_takes")
    async def toggle_takes(callback: types.CallbackQuery):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        if cfg.takes_paused:
            cfg.takes_paused = False
            paused = config.paused_takes.get(bot_id, [])
            sent = 0
            for t in paused:
                text = t.get('text', '')
                if contains_marker_words(text, bot_id) or cfg.manual_control:
                    tid = str(uuid.uuid4())[:8]
                    config.pending_takes[tid] = t
                else:
                    try:
                        c, hp = censor_profanity(text, bot_id)
                        kw = {"caption": c if hp else text, "parse_mode": "HTML" if hp else None}
                        if t.get('photo'):
                            await bot_instance.send_photo(cfg.takes_channel, photo=t['photo'], **kw)
                        elif t.get('video'):
                            await bot_instance.send_video(cfg.takes_channel, video=t['video'], **kw)
                        else:
                            await bot_instance.send_message(cfg.takes_channel, c if hp else text, parse_mode="HTML" if hp else None)
                        sent += 1
                    except Exception as e:
                        logger.error(f"Ошибка из очереди: {e}")
            config.paused_takes[bot_id] = []
            config.save()
            await callback.answer(f"▶️ Включены! Отправлено: {sent}", show_alert=True)
        else:
            cfg.takes_paused = True
            config.save()
            await callback.answer("⏸ На паузе", show_alert=True)
        await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    @router.callback_query(F.data == "adm_toggle_manual")
    async def toggle_manual(callback: types.CallbackQuery):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        cfg.manual_control = not cfg.manual_control
        config.save()
        st = "🔒 Ручной ВКЛ" if cfg.manual_control else "🔓 Авто ВКЛ"
        await callback.answer(st, show_alert=True)
        await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    @router.callback_query(F.data == "adm_channel_quiz")
    async def adm_quiz(callback: types.CallbackQuery, state: FSMContext):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("🎯 Отправьте вопрос:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingQuizQuestion)
        await callback.answer()

    @router.message(AdminStates.WaitingQuizQuestion)
    async def quiz_q(message: types.Message, state: FSMContext):
        qd = {'text': message.text or message.caption or '',
              'photo': message.photo[-1].file_id if message.photo else None,
              'video': message.video.file_id if message.video else None}
        await state.update_data(quiz_data=qd)
        await message.answer("Награда (число):", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingQuizReward)

    @router.message(AdminStates.WaitingQuizReward)
    async def quiz_r(message: types.Message, state: FSMContext):
        try:
            rw = int(message.text)
            if rw <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Число!", reply_markup=build_cancel_keyboard())
            return
        await state.update_data(quiz_reward=rw)
        await message.answer("Правильный ответ:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingQuizAnswer)

    @router.message(AdminStates.WaitingQuizAnswer)
    async def quiz_a(message: types.Message, state: FSMContext):
        data = await state.get_data()
        cfg = config.bots.get(bot_id)
        qd = data['quiz_data']
        ans = message.text.strip().lower()
        try:
            if qd.get('photo'):
                sent = await bot_instance.send_photo(cfg.takes_channel, photo=qd['photo'], caption=qd['text'])
            elif qd.get('video'):
                sent = await bot_instance.send_video(cfg.takes_channel, video=qd['video'], caption=qd['text'])
            else:
                sent = await bot_instance.send_message(cfg.takes_channel, qd['text'])
            qid = str(sent.message_id)
            config.active_quizzes[qid] = {'bot_id': bot_id, 'message_id': sent.message_id, 'answer': ans, 'reward': data['quiz_reward'], 'channel': cfg.takes_channel, 'solved': False}
            config.save()
            await message.answer(f"✅ Опубликовано!\nОтвет: {ans}\nНаграда: {data['quiz_reward']} {cfg.currency_emoji}", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        except Exception as e:
            await message.answer(f"❌ {e}", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        await state.clear()

    @router.callback_query(F.data == "adm_censor")
    async def adm_censor(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("🔧 Цензура\nБазовые корни всегда активны.", reply_markup=build_censor_menu())
        await callback.answer()

    @router.callback_query(F.data == "censor_add")
    async def censor_add(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Слово/корень:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingCensorWord)
        await callback.answer()

    @router.message(AdminStates.WaitingCensorWord)
    async def censor_add_p(message: types.Message, state: FSMContext):
        w = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if w not in cfg.censored_words:
            cfg.censored_words.append(w)
            config.save()
        await message.answer(f"✅ '{w}' добавлено.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "censor_del")
    async def censor_del(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg.censored_words:
            await callback.answer("Пусто.", show_alert=True)
            return
        await callback.message.edit_text(f"Слова: {', '.join(cfg.censored_words)}\n\nУдалить:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingRemoveCensorWord)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveCensorWord)
    async def censor_del_p(message: types.Message, state: FSMContext):
        w = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if w in cfg.censored_words:
            cfg.censored_words.remove(w)
            config.save()
            await message.answer(f"✅ '{w}' удалено.", reply_markup=build_censor_menu())
        else:
            await message.answer("Не найдено.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "marker_add")
    async def marker_add(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Маркер:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingMarkerWord)
        await callback.answer()

    @router.message(AdminStates.WaitingMarkerWord)
    async def marker_add_p(message: types.Message, state: FSMContext):
        w = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if w not in cfg.marker_words:
            cfg.marker_words.append(w)
            config.save()
        await message.answer(f"✅ '{w}' добавлен.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "marker_del")
    async def marker_del(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg.marker_words:
            await callback.answer("Пусто.", show_alert=True)
            return
        await callback.message.edit_text(f"Маркеры: {', '.join(cfg.marker_words)}\n\nУдалить:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingRemoveMarkerWord)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveMarkerWord)
    async def marker_del_p(message: types.Message, state: FSMContext):
        w = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if w in cfg.marker_words:
            cfg.marker_words.remove(w)
            config.save()
            await message.answer(f"✅ '{w}' удалён.", reply_markup=build_censor_menu())
        else:
            await message.answer("Не найден.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "censor_list")
    async def censor_list(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        cw = cfg.censored_words or ["(нет)"]
        mw = cfg.marker_words or ["(нет)"]
        await callback.message.edit_text(
            f"🔧 Базовые: всегда\n📋 Доп: {', '.join(cw)}\n🏷 Маркеры: {', '.join(mw)}",
            reply_markup=build_censor_menu())
        await callback.answer()

    @router.callback_query(F.data == "adm_mods")
    async def adm_mods(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("👮 Модераторы:", reply_markup=build_mods_menu())
        await callback.answer()

    @router.callback_query(F.data == "mod_assign")
    async def mod_assign(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Username:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingModeratorUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingModeratorUsername)
    async def mod_assign_p(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_moderator=1)
            await message.answer("✅ Назначен.", reply_markup=build_mods_menu())
        else:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "mod_remove")
    async def mod_remove(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Username:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingRemoveModeratorUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveModeratorUsername)
    async def mod_remove_p(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_moderator=0)
            await message.answer("✅ Снят.", reply_markup=build_mods_menu())
        else:
            await message.answer("Не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "mod_list")
    async def mod_list(callback: types.CallbackQuery):
        ul = db.get_all_users_for_bot(bot_id)
        mods = []
        for u in ul:
            ud = db.get_bot_data(u['user_id'], bot_id)
            if ud.get('is_moderator'):
                ui = db.get_user(u['user_id'])
                if ui:
                    mods.append(f"@{ui['username']}")
        text = ", ".join(mods) if mods else "(нет)"
        await callback.message.edit_text(f"👮 Модераторы: {text}", reply_markup=build_mods_menu())
        await callback.answer()

    if bot_id == "main":
        @router.callback_query(F.data == "adm_reset_rates")
        async def reset_rates(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            reset_all_rates()
            await callback.answer("✅ Сброшены.", show_alert=True)
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

        @router.callback_query(F.data == "adm_reset_top")
        async def reset_top(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            ul = db.get_all_users_for_bot(bot_id)
            for u in ul:
                db.set_bot_data(u['user_id'], bot_id, show_in_top=0)
            await callback.answer("✅ Сброшен.", show_alert=True)
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    @router.callback_query(F.data == "adm_balance")
    async def adm_bal(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        bal = db.get_balance(callback.from_user.id, bot_id)
        bs = "∞" if bal == float('inf') else f"{bal:.0f}"
        await callback.message.edit_text(f"💳 {bs} {cfg.currency_emoji}", reply_markup=build_admin_menu(callback.from_user.id, bot_id))
        await callback.answer()

    # =================== КАНАЛ — АУКЦИОН И ВИКТОРИНА ===================

    @router.channel_post()
    async def handle_channel_post(message: types.Message):
        """Отслеживание постов канала. Создаёт аукцион по #аукцион."""
        if not message.text:
            return
        cfg = config.bots.get(bot_id)
        if not cfg:
            return
        if "#аукцион" in message.text.lower():
            auction_id = str(message.message_id)
            discussion_chat_id = None
            try:
                channel_info = await bot_instance.get_chat(message.chat.id)
                if hasattr(channel_info, 'linked_chat_id') and channel_info.linked_chat_id:
                    discussion_chat_id = channel_info.linked_chat_id
                    logger.info(f"Группа комментариев: {discussion_chat_id}")
            except Exception as e:
                logger.error(f"Ошибка получения группы: {e}")
            config.active_auctions[auction_id] = {
                'bot_id': bot_id,
                'channel': message.chat.id,
                'discussion_chat_id': discussion_chat_id,
                'discussion_message_id': None,
                'message_id': message.message_id,
                'current_bidder': None,
                'current_bid': 0,
                'last_bid_time': datetime.now().isoformat(),
                'bid_history': [],
                'finished': False
            }
            config.save()
            logger.info(f"Аукцион создан: {auction_id}")
            task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, auction_id))
            config.auction_tasks[auction_id] = task

    @router.message(F.forward_from_chat)
    async def handle_forwarded_post(message: types.Message):
        """
        Ловим пересланные посты из канала в группу комментариев.
        Telegram автоматически пересылает каждый пост канала в группу.
        Это даёт нам ID поста В ГРУППЕ для reply при отсчёте аукциона.
        """
        if not message.forward_from_chat:
            return
        fwd_id = message.forward_from_message_id
        if not fwd_id:
            return
        auction_id = str(fwd_id)
        auction = config.active_auctions.get(auction_id)
        if auction and not auction.get('discussion_message_id'):
            auction['discussion_message_id'] = message.message_id
            auction['discussion_chat_id'] = message.chat.id
            config.save()
            logger.info(f"Аукцион {auction_id}: ID в группе = {message.message_id}")

    @router.message(F.reply_to_message)
    async def handle_comment_reply(message: types.Message):
        """
        Обработка комментариев под постами канала.
        Викторина: правильный ответ — награда.
        Аукцион: ставки, пас/лив, правила повышения.
        """
        if not message.text:
            return
        if not message.reply_to_message:
            return
        cfg = config.bots.get(bot_id)
        if not cfg:
            return

        original_msg_id = None
        if message.reply_to_message.forward_from_message_id:
            original_msg_id = str(message.reply_to_message.forward_from_message_id)
        elif message.reply_to_message.message_id:
            original_msg_id = str(message.reply_to_message.message_id)
        if not original_msg_id:
            return

        user_id = message.from_user.id
        user_name = message.from_user.full_name
        comment_text = message.text.strip()
        comment_lower = comment_text.lower()

        # ===== ВИКТОРИНА =====
        quiz = config.active_quizzes.get(original_msg_id)
        if quiz and not quiz.get('solved') and quiz.get('bot_id') == bot_id:
            if comment_lower == quiz.get('answer', '').lower():
                reward = quiz.get('reward', 0)
                quiz['solved'] = True
                config.save()
                register_user(message.from_user, bot_id)
                db.add_balance(user_id, bot_id, reward)
                try:
                    await message.reply(f"✅ Правильный ответ, {user_name}!\n+{reward} {cfg.currency_emoji}")
                except Exception as e:
                    logger.error(f"Ошибка ответа викторины: {e}")
                logger.info(f"Викторина {original_msg_id} решена {user_id}")
                if original_msg_id in config.active_quizzes:
                    del config.active_quizzes[original_msg_id]
                    config.save()

        # ===== АУКЦИОН =====
        auction = config.active_auctions.get(original_msg_id)
        if not auction or auction.get('bot_id') != bot_id or auction.get('finished'):
            return

        # Пас / лив — откат к предыдущей ставке
        if PASS_PATTERN.match(comment_text.strip()):
            bid_history = auction.get('bid_history', [])
            if len(bid_history) < 2:
                try:
                    await message.reply("Нет предыдущей ставки для отката.")
                except Exception:
                    pass
                return
            bid_history.pop()
            prev_bid = bid_history[-1]
            auction['current_bidder'] = prev_bid['bidder']
            auction['current_bid'] = prev_bid['amount']
            auction['last_bid_time'] = datetime.now().isoformat()
            auction['bid_history'] = bid_history
            config.save()
            prev_display = prev_bid.get('display', 'Неизвестный')
            try:
                await message.reply(f"↩️ Ставка отменена.\nАктуальная ставка: {prev_bid['amount']} {cfg.currency_emoji} ({prev_display})")
            except Exception as e:
                logger.error(f"Ошибка пас/лив: {e}")
            old_task = config.auction_tasks.get(original_msg_id)
            if old_task and not old_task.done():
                old_task.cancel()
            task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, original_msg_id))
            config.auction_tasks[original_msg_id] = task
            logger.info(f"Аукцион {original_msg_id}: откат к {prev_bid['amount']}")
            return

        # Проверка ставки
        bet_match = BET_PATTERN.search(comment_text)
        if not bet_match:
            return

        bet_amount = int(bet_match.group(1))
        register_user(message.from_user, bot_id)
        user_balance = db.get_balance(user_id, bot_id)
        current_bid = auction.get('current_bid', 0)

        # Ставка должна быть строго больше предыдущей
        if bet_amount <= current_bid:
            try:
                await message.reply(f"Ошибка: ставка равна или меньше предыдущей ({current_bid} {cfg.currency_emoji})")
            except Exception:
                pass
            return

        # Минимальное повышение
        if bet_amount - current_bid < MIN_BID_INCREMENT:
            try:
                await message.reply(f"Ошибка: недостаточное повышение (минимум +{MIN_BID_INCREMENT} {cfg.currency_emoji})")
            except Exception:
                pass
            return

        # Проверка баланса
        if user_balance != float('inf') and user_balance < bet_amount:
            try:
                await message.reply(f"У вас недостаточно средств.\nБаланс: {user_balance:.0f} {cfg.currency_emoji}")
            except Exception as e:
                logger.error(f"Ошибка баланса: {e}")
            return

        # Отображаемое имя (username в приоритете)
        user_info = db.get_user(user_id)
        if user_info and user_info.get('username') and not user_info['username'].startswith('user'):
            display_name = f"@{user_info['username']}"
        else:
            display_name = user_name

        # Принимаем ставку
        if 'bid_history' not in auction:
            auction['bid_history'] = []
        auction['bid_history'].append({'bidder': user_id, 'amount': bet_amount, 'display': display_name, 'time': datetime.now().isoformat()})
        auction['current_bidder'] = user_id
        auction['current_bid'] = bet_amount
        auction['last_bid_time'] = datetime.now().isoformat()
        config.save()

        try:
            await message.reply("Ставка принята")
        except Exception as e:
            logger.error(f"Ошибка подтверждения: {e}")

        logger.info(f"Аукцион {original_msg_id}: ставка {bet_amount} от {display_name}")

        old_task = config.auction_tasks.get(original_msg_id)
        if old_task and not old_task.done():
            old_task.cancel()
        task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, original_msg_id))
        config.auction_tasks[original_msg_id] = task

    dp.include_router(router)

# ====================== ПОДКЛЮЧЕНИЕ БОТОВ ======================

def create_connection_handlers(bot_instance: Bot, dp: Dispatcher):
    router = Router()

    @router.callback_query(F.data == "connect_bot")
    async def connect_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("🤖 Ссылка на канал:", reply_markup=build_cancel_keyboard())
        await state.set_state(ConnectBotStates.WaitingChannelUrl)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingChannelUrl)
    async def connect_channel(message: types.Message, state: FSMContext):
        request_id = str(uuid.uuid4())[:8]
        request = PendingBotRequest(request_id=request_id, user_id=message.from_user.id, channel_url=message.text.strip())
        config.pending_requests[request_id] = request
        config.save()
        user = db.get_user(message.from_user.id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"request_approve_{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"request_reject_{request_id}")]])
        try:
            await bot_instance.send_message(MAIN_ADMIN_ID,
                f"📝 Заявка\nОт: @{user['username'] if user else '?'}\nКанал: {message.text}",
                reply_markup=kb)
        except Exception:
            pass
        await message.answer("✅ Отправлено!", reply_markup=build_main_menu("main"))
        await state.clear()

    @router.callback_query(F.data.startswith("request_approve_"))
    async def req_approve(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[16:]
        req = config.pending_requests.get(rid)
        if not req:
            await callback.answer("Не найдено", show_alert=True)
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Да", callback_data=f"request_confirm_{rid}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"request_back_{rid}")]])
        await callback.message.edit_text("Уверены?", reply_markup=kb)
        await callback.answer()

    @router.callback_query(F.data.startswith("request_back_"))
    async def req_back(callback: types.CallbackQuery):
        rid = callback.data[13:]
        req = config.pending_requests.get(rid)
        if req:
            user = db.get_user(req.user_id)
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"request_approve_{rid}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"request_reject_{rid}")]])
            await callback.message.edit_text(
                f"📝 Заявка\nОт: @{user['username'] if user else '?'}\nКанал: {req.channel_url}",
                reply_markup=kb)
        await callback.answer()

    @router.callback_query(F.data.startswith("request_confirm_"))
    async def req_confirm(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[16:]
        req = config.pending_requests.get(rid)
        if not req:
            await callback.answer("Не найдено", show_alert=True)
            return
        req.status = "token_wait"
        config.waiting_for_token[req.user_id] = rid
        config.save()
        try:
            await bot_instance.send_message(req.user_id, "✅ Одобрено!\nТокен от @BotFather:")
            await callback.message.edit_text("⏳ Ждём токен...")
        except Exception as e:
            await callback.message.edit_text(f"❌ {e}")
        await callback.answer()

    @router.callback_query(F.data.startswith("request_reject_"))
    async def req_reject(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[15:]
        req = config.pending_requests.get(rid)
        if req:
            try:
                await bot_instance.send_message(req.user_id, "❌ Отклонено.")
            except Exception:
                pass
            del config.pending_requests[rid]
            config.save()
        await callback.message.edit_text("❌ Отклонено.")
        await callback.answer()

    @router.message(F.text.regexp(r'^\d{8,10}:[A-Za-z0-9_-]{35,}$'))
    async def receive_token(message: types.Message, state: FSMContext):
        uid = message.from_user.id
        if uid not in config.waiting_for_token:
            return
        rid = config.waiting_for_token[uid]
        req = config.pending_requests.get(rid)
        if not req or req.status != "token_wait":
            del config.waiting_for_token[uid]
            return
        token = message.text.strip()
        try:
            test_bot = Bot(token=token)
            info = await test_bot.get_me()
            await test_bot.session.close()
            req.token = token
            req.status = "configuring"
            del config.waiting_for_token[uid]
            config.save()
            await state.update_data(request_id=rid)
            await message.answer(f"✅ @{info.username}\n\nНазвание валюты:")
            await state.set_state(ConnectBotStates.WaitingCurrencyName)
            try:
                await bot_instance.send_message(MAIN_ADMIN_ID, f"✅ Токен: @{info.username}")
            except Exception:
                pass
        except Exception as e:
            await message.answer(f"❌ Неверный токен: {e}\nЕщё раз:")

    @router.message(ConnectBotStates.WaitingCurrencyName)
    async def connect_curr_name(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if req:
            req.currency_name = message.text.strip()
            config.save()
        await message.answer("Эмодзи валюты (💎, 🪙, ⭐):")
        await state.set_state(ConnectBotStates.WaitingCurrencyEmoji)

    @router.message(ConnectBotStates.WaitingCurrencyEmoji)
    async def connect_curr_emoji(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if req:
            req.currency_emoji = message.text.strip()
            config.save()
        await message.answer("Выберите функции:", reply_markup=build_modules_keyboard())
        await state.set_state(ConnectBotStates.WaitingModules)

    @router.callback_query(ConnectBotStates.WaitingModules, F.data.startswith("module_"))
    async def connect_modules(callback: types.CallbackQuery, state: FSMContext):
        mod = callback.data[7:]
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            await callback.answer("Ошибка", show_alert=True)
            return

        if mod == "takes":
            req.modules = ["takes"]
            config.save()
            await callback.message.edit_text(
                "📝 Выбраны тейки\n\n"
                "Ссылка или ID канала для тейков\n"
                "(закрытый канал — числовой ID, например: -1001234567890):")
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        elif mod == "shop":
            req.modules = ["shop"]
            config.save()
            await callback.message.edit_text(
                "🛒 Выбран магазин\n\n"
                "Ссылка или ID канала для объявлений\n"
                "(закрытый канал — числовой ID, например: -1001234567890):")
            await state.set_state(ConnectBotStates.WaitingAnnouncementChannel)
        else:
            req.modules = ["takes", "shop"]
            config.save()
            await callback.message.edit_text(
                "📝🛒 Тейки + магазин\n\n"
                "Сначала ссылка или ID канала для ТЕЙКОВ\n"
                "(закрытый канал — числовой ID, например: -1001234567890):")
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingTakesChannel)
    async def connect_takes_ch(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            return
        channel = message.text.strip()
        if channel.startswith("https://t.me/"):
            channel = "@" + channel.replace("https://t.me/", "")
        elif not channel.startswith("@") and not channel.startswith("-"):
            channel = "@" + channel
        req.takes_channel = channel
        config.save()
        if "shop" in req.modules:
            await message.answer(
                f"✅ Канал тейков: {channel}\n\n"
                f"Теперь ссылка или ID канала для ОБЪЯВЛЕНИЙ\n"
                f"(закрытый канал — числовой ID, например: -1001234567890):")
            await state.set_state(ConnectBotStates.WaitingAnnouncementChannel)
        else:
            await finalize_setup(message, state, req, bot_instance)

    @router.message(ConnectBotStates.WaitingAnnouncementChannel)
    async def connect_announcement_ch(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            return
        channel = message.text.strip()
        if channel.startswith("https://t.me/"):
            channel = "@" + channel.replace("https://t.me/", "")
        elif not channel.startswith("@") and not channel.startswith("-"):
            channel = "@" + channel
        req.announcement_channel = channel
        req.shop_channel = channel
        config.save()
        await finalize_setup(message, state, req, bot_instance)

    async def finalize_setup(message, state, req, main_bot):
        """Завершение настройки и запуск подключённого бота."""
        await message.answer("⏳ Запуск...")
        try:
            new_id = f"bot_{req.user_id}_{int(datetime.now().timestamp())}"
            new_cfg = BotConfig(
                bot_id=new_id, token=req.token,
                currency_name=req.currency_name, currency_emoji=req.currency_emoji,
                channel_url=req.channel_url, takes_channel=req.takes_channel,
                shop_channel=req.shop_channel, announcement_channel=req.announcement_channel,
                modules=req.modules, owner_id=req.user_id,
                base_exchange_rate=0.5, take_cooldown_minutes=3
            )
            config.bots[new_id] = new_cfg

            register_user(message.from_user, new_id)
            db.set_balance(req.user_id, new_id, float('inf'))
            db.set_bot_data(req.user_id, new_id, is_owner=1, is_admin=1, activated_at=datetime.now().isoformat())

            config.exchange_rates.rates[new_id] = 0.5
            if req.request_id in config.pending_requests:
                del config.pending_requests[req.request_id]
            config.save()

            new_bot = Bot(token=req.token)
            new_dp = Dispatcher(storage=MemoryStorage())
            create_bot_handlers(new_id, new_bot, new_dp)
            create_admin_and_channel_handlers(new_id, new_bot, new_dp)
            config.active_bots[new_id] = new_bot
            config.active_dispatchers[new_id] = new_dp
            asyncio.create_task(new_dp.start_polling(new_bot))

            channels_info = ""
            if req.takes_channel:
                channels_info += f"📝 Тейки: {req.takes_channel}\n"
            if req.announcement_channel:
                channels_info += f"📢 Объявления: {req.announcement_channel}\n"

            await message.answer(
                f"✅ Бот подключён!\n\n"
                f"💰 Валюта: {req.currency_name} {req.currency_emoji}\n"
                f"📦 Модули: {', '.join(req.modules)}\n"
                f"{channels_info}\n"
                f"Вы — владелец с ∞ балансом.\n/start в боте",
                reply_markup=build_main_menu("main"))
            try:
                await main_bot.send_message(MAIN_ADMIN_ID, f"✅ Бот {new_id} запущен!")
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Ошибка запуска: {e}")
            await message.answer(f"❌ {e}", reply_markup=build_main_menu("main"))
        await state.clear()

    dp.include_router(router)


# ====================== ГЛАВНАЯ ФУНКЦИЯ ======================

async def main():
    logger.info("Подключение к БД...")
    db.connect()
    logger.info("Загрузка конфигурации...")
    config.load()

    main_cfg = config.bots.get("main")
    if main_cfg:
        logger.info(f"Канал объявлений: '{main_cfg.announcement_channel}'")

    main_bot = Bot(token=MAIN_BOT_TOKEN)
    main_dp = Dispatcher(storage=MemoryStorage())
    create_bot_handlers("main", main_bot, main_dp)
    create_admin_and_channel_handlers("main", main_bot, main_dp)
    create_connection_handlers(main_bot, main_dp)
    config.active_bots["main"] = main_bot
    config.active_dispatchers["main"] = main_dp

    # Запуск подключённых ботов
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != "main":
            try:
                cb = Bot(token=bot_cfg.token)
                cd = Dispatcher(storage=MemoryStorage())
                create_bot_handlers(bot_id, cb, cd)
                create_admin_and_channel_handlers(bot_id, cb, cd)
                config.active_bots[bot_id] = cb
                config.active_dispatchers[bot_id] = cd
                asyncio.create_task(cd.start_polling(cb))
                logger.info(f"Запущен: {bot_id}")
            except Exception as e:
                logger.error(f"Ошибка {bot_id}: {e}")

    # Восстановление задач удаления пиара
    now = datetime.now()
    to_remove = []
    for did, info in config.scheduled_deletions.items():
        try:
            da = datetime.fromisoformat(info['delete_at'])
            tb = config.active_bots.get(info['bot_id'])
            if not tb:
                to_remove.append(did)
                continue
            if da <= now:
                logger.info(f"Просроченное удаление: {did}")
                try:
                    if info.get('is_pinned'):
                        await tb.unpin_chat_message(info['channel'], info['message_id'])
                    await tb.delete_message(info['channel'], info['message_id'])
                except Exception as e:
                    logger.error(f"Ошибка удаления: {e}")
                to_remove.append(did)
            else:
                h = (da - now).total_seconds() / 3600
                logger.info(f"Восстановлено: {did}, {h:.1f}ч")
                asyncio.create_task(delayed_delete_message(tb, info['channel'], info['message_id'], h, info.get('is_pinned', False), did))
        except Exception as e:
            logger.error(f"Ошибка восстановления {did}: {e}")
            to_remove.append(did)

    for did in to_remove:
        if did in config.scheduled_deletions:
            del config.scheduled_deletions[did]
    if to_remove:
        config.save()

    logger.info(f"Задач удаления: {len(config.scheduled_deletions)}")
    logger.info("Запуск главного бота...")
    await main_dp.start_polling(main_bot)


if __name__ == "__main__":
    print("🚀 Запуск бота...", flush=True)
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ ОШИБКА: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
