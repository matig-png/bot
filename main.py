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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ====================== КОНФИГУРАЦИЯ ======================

MAIN_BOT_TOKEN = "8216288128:AAHWCLpy-tPcFKjbpM2hUN1xt6P850mi5qE"
MAIN_ADMIN_ID = 6098677257
ADMIN_IDS = {6098677257, 8092280284, 8366347415}
DB_FILE = "bot_database.db"
CONFIG_FILE = "bot_config.json"

TG_LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/)?([a-zA-Z0-9_]+)',
    re.IGNORECASE
)
BET_PATTERN = re.compile(r'ставлю\s+(\d+)', re.IGNORECASE)


# ====================== БАЗА ДАННЫХ ======================

class Database:
    """SQLite база данных. Данные пользователей сохраняются между обновлениями бота."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Подключение и создание таблиц."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info(f"База данных подключена: {self.db_path}")

    def _create_tables(self):
        """Создание всех таблиц если не существуют."""
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

    # --- Пользователи ---

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получить данные пользователя."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create_or_update_user(self, user_id: int, username: str, name: str):
        """Создать или обновить пользователя."""
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

    # --- Балансы ---

    def get_balance(self, user_id: int, bot_id: str) -> float:
        """Получить баланс пользователя в конкретном боте."""
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
        """Установить баланс."""
        is_inf = 1 if balance == float('inf') else 0
        val = 0 if is_inf else balance
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO balances (user_id, bot_id, balance, is_infinite) VALUES (?, ?, ?, ?)',
            (user_id, bot_id, val, is_inf)
        )
        self.conn.commit()

    def add_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        """Добавить к балансу. Возвращает True если успешно."""
        current = self.get_balance(user_id, bot_id)
        if current == float('inf'):
            return True
        self.set_balance(user_id, bot_id, current + amount)
        return True

    def deduct_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        """Списать с баланса. Возвращает True если хватило средств."""
        current = self.get_balance(user_id, bot_id)
        if current == float('inf'):
            return True
        if current < amount:
            return False
        self.set_balance(user_id, bot_id, current - amount)
        return True

    # --- Данные пользователя для бота ---

    def get_bot_data(self, user_id: int, bot_id: str) -> Dict:
        """Получить данные пользователя для конкретного бота."""
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
        """Обновить данные пользователя для бота."""
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

    # --- Тейки ---

    def get_last_take_time(self, user_id: int, bot_id: str) -> Optional[str]:
        """Время последнего тейка."""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT timestamp FROM take_timestamps WHERE user_id = ? AND bot_id = ? ORDER BY id DESC LIMIT 1',
            (user_id, bot_id)
        )
        row = cursor.fetchone()
        return row['timestamp'] if row else None

    def add_take_timestamp(self, user_id: int, bot_id: str):
        """Записать время отправки тейка."""
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO take_timestamps (user_id, bot_id, timestamp) VALUES (?, ?, ?)',
            (user_id, bot_id, datetime.now().isoformat())
        )
        # Оставляем только последние 20 записей
        cursor.execute('''DELETE FROM take_timestamps WHERE id NOT IN (
            SELECT id FROM take_timestamps WHERE user_id = ? AND bot_id = ? ORDER BY id DESC LIMIT 20
        ) AND user_id = ? AND bot_id = ?''', (user_id, bot_id, user_id, bot_id))
        self.conn.commit()

    # --- Списки ---

    def get_all_users_for_bot(self, bot_id: str) -> List[Dict]:
        """Все пользователи с балансами в конкретном боте."""
        cursor = self.conn.cursor()
        cursor.execute('''SELECT u.user_id, u.username, u.name,
                     COALESCE(b.balance, 0) as balance, COALESCE(b.is_infinite, 0) as is_infinite,
                     COALESCE(d.show_in_top, 1) as show_in_top, COALESCE(d.is_owner, 0) as is_owner
                     FROM users u
                     LEFT JOIN balances b ON u.user_id = b.user_id AND b.bot_id = ?
                     LEFT JOIN user_bot_data d ON u.user_id = d.user_id AND d.bot_id = ?
                     WHERE b.balance IS NOT NULL OR b.is_infinite = 1''', (bot_id, bot_id))
        return [dict(row) for row in cursor.fetchall()]

    def find_user_by_input(self, input_str: str) -> Optional[int]:
        """Найти пользователя по username, имени или ID."""
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


# ====================== КОНФИГУРАЦИЯ БОТОВ (JSON) ======================

@dataclass
class BotConfig:
    bot_id: str
    token: str
    currency_name: str
    currency_emoji: str
    channel_url: str = ""
    takes_channel: str = ""
    shop_channel: str = ""
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


class ConfigStorage:
    """Хранит конфигурации ботов и временные данные в JSON."""

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
        """Загрузка конфигурации из JSON файла."""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for bot_id, bot_data in data.get('bots', {}).items():
                    self.bots[bot_id] = BotConfig(**bot_data)

                if 'exchange_rates' in data:
                    self.exchange_rates = ExchangeRates(**data['exchange_rates'])

                for req_id, req_data in data.get('pending_requests', {}).items():
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

        # Создаём главного бота если нет
        if "main" not in self.bots:
            self.bots["main"] = BotConfig(
                bot_id="main",
                token=MAIN_BOT_TOKEN,
                currency_name="луны",
                currency_emoji="🌗",
                channel_url="https://t.me/WINGSOFFIRECHANNEL",
                takes_channel="@WINGSOFFIRECHANNEL",
                shop_channel="@wingsoffiremagazine",
                modules=["takes", "shop"],
                take_cooldown_minutes=3,
                owner_id=MAIN_ADMIN_ID,
                base_exchange_rate=1.0
            )
            self.exchange_rates.rates["main"] = 1.0
            self.save()

    def save(self):
        """Сохранение конфигурации в JSON файл."""
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


class QuizStates(StatesGroup):
    Question1 = State()
    Question2 = State()
    Question3 = State()

# ====================== ЦЕНЗУРА ======================

_PFX = r'(?:за|на|по|от|об|до|у|о|вы|пере|при|рас|раз|про|недо|пре|с|ис|из)?'
_PFX_EB = r'(?:за|на|по|от|отъ|об|объ|до|у|о|вы|пере|при|рас|раз|про|недо|пре|съ|ис|из|долбо)?'

BASE_PROFANITY_PATTERNS = [
    _PFX + r'ху[йяеёюи]\w*',
    _PFX + r'пизд\w*',
    _PFX_EB + r'[её]б\w*',
    r'бля[дт]\w*',
    r'сук[аиуе]\w*',
    r'суч[каеьи]\w*',
    r'муда[кч]\w*',
    r'мудил\w*',
    r'мудозвон\w*',
    r'пидор\w*',
    r'пидар\w*',
    r'пидр\w*',
    r'педик\w*',
    r'педераст\w*',
    r'шлюх\w*',
    r'гандон\w*',
    r'залуп\w*',
    r'дроч\w*',
    r'манд[аоуеёяи]\w*',
    r'[её]бл[ао]\w*',
    r'[её]бну\w*',
    r'[её]бан\w*',
    r'хер[а-яё]*\w*',
    r'жоп[аеуы]\w*',
    r'срать?\w*',
    r'сран\w*',
    r'говн[оа]\w*',
    r'засранец\w*',
    r'засранк[аи]\w*',
]


def build_profanity_regex(bot_id: str) -> re.Pattern:
    """Строит регулярное выражение для цензуры с базовыми корнями + пользовательские слова."""
    bot_config = config.bots.get(bot_id)
    patterns = BASE_PROFANITY_PATTERNS.copy()
    if bot_config and bot_config.censored_words:
        for word in bot_config.censored_words:
            patterns.append(re.escape(word) + r'\w*')
    return re.compile(r'\b(?:' + '|'.join(patterns) + r')\b', re.IGNORECASE | re.UNICODE)


def censor_profanity(text: str, bot_id: str) -> Tuple[str, bool]:
    """Заменяет мат на спойлеры. Возвращает (текст, найден_мат)."""
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
    """Проверяет наличие маркерных слов для модерации."""
    if not text:
        return False
    bot_config = config.bots.get(bot_id)
    if not bot_config or not bot_config.marker_words:
        return False
    text_lower = text.lower()
    return any(word.lower() in text_lower for word in bot_config.marker_words)


async def check_telegram_links(text: str, bot_instance: Bot) -> Tuple[bool, str]:
    """Проверяет ссылки — если группа, отправлять на модерацию."""
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
    """Регистрация пользователя в БД при первом обращении к боту."""
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
            quiz_passed=0, show_in_top=1, is_blocked=0, is_frozen=0,
            is_moderator=0,
            is_admin=1 if (is_admin_flag or is_owner_flag or is_main_owner) else 0,
            is_owner=1 if (is_owner_flag or is_main_owner) else 0,
            activated_at=datetime.now().isoformat(),
            last_promo_at=''
        )


def check_admin(uid: int, bot_id: str) -> bool:
    """Является ли пользователь админом."""
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_admin') or data.get('is_owner'))


def check_owner(uid: int, bot_id: str) -> bool:
    """Является ли пользователь владельцем."""
    return bool(db.get_bot_data(uid, bot_id).get('is_owner'))


def check_moderator(uid: int, bot_id: str) -> bool:
    """Является ли пользователь модератором (или админом/владельцем)."""
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_moderator') or data.get('is_admin') or data.get('is_owner'))


def can_send_take(uid: int, bot_id: str) -> Tuple[bool, str]:
    """Проверка кулдауна тейков (1 в N минут)."""
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return False, "Ошибка конфигурации"

    last = db.get_last_take_time(uid, bot_id)
    if not last:
        return True, "Можно отправить"

    try:
        last_dt = datetime.fromisoformat(last)
    except Exception:
        return True, "Можно отправить"

    next_available = last_dt + timedelta(minutes=bot_cfg.take_cooldown_minutes)
    now = datetime.now()

    if now >= next_available:
        return True, "Можно отправить"

    remaining = next_available - now
    minutes = int(remaining.total_seconds() // 60)
    seconds = int(remaining.total_seconds() % 60)
    return False, f"Подождите {minutes}м {seconds}с"


def can_use_promo(uid: int, bot_id: str) -> Tuple[bool, str]:
    """Проверка: 3 дня с активации + 12ч с последнего пиара."""
    data = db.get_bot_data(uid, bot_id)

    # Проверка 3 дня с активации
    activated_at = data.get('activated_at', '')
    if activated_at:
        try:
            act_dt = datetime.fromisoformat(activated_at)
            if datetime.now() - act_dt < timedelta(days=3):
                remaining = (act_dt + timedelta(days=3)) - datetime.now()
                hours_left = int(remaining.total_seconds() // 3600)
                return False, f"Пиар доступен через {hours_left}ч (3 дня с активации)"
        except Exception:
            pass

    # Проверка 12 часов с последнего пиара
    last_promo = data.get('last_promo_at', '')
    if last_promo:
        try:
            last_dt = datetime.fromisoformat(last_promo)
            if datetime.now() - last_dt < timedelta(hours=12):
                remaining = (last_dt + timedelta(hours=12)) - datetime.now()
                hours_left = int(remaining.total_seconds() // 3600)
                mins_left = int((remaining.total_seconds() % 3600) // 60)
                return False, f"Следующий пиар через {hours_left}ч {mins_left}м"
        except Exception:
            pass

    return True, "Доступно"


def do_transfer(sender_id: int, receiver_id: int, bot_id: str, amount: float) -> Tuple[bool, str]:
    """Перевод валюты между пользователями."""
    sender_data = db.get_bot_data(sender_id, bot_id)
    receiver_data = db.get_bot_data(receiver_id, bot_id)

    if sender_data.get('is_frozen'):
        return False, "Ваш счёт заморожен"
    if receiver_data.get('is_frozen'):
        return False, "Счёт получателя заморожен"

    sender_bal = db.get_balance(sender_id, bot_id)
    if sender_bal != float('inf') and sender_bal < amount:
        return False, "Недостаточно средств"

    if sender_bal != float('inf'):
        db.set_balance(sender_id, bot_id, sender_bal - amount)
    db.add_balance(receiver_id, bot_id, amount)
    return True, "OK"


def get_exchange_rate(bot_id: str) -> float:
    """Получить текущий курс валюты."""
    if config.exchange_rates.rates_locked:
        bot_cfg = config.bots.get(bot_id)
        return bot_cfg.base_exchange_rate if bot_cfg else 0.5
    return config.exchange_rates.rates.get(bot_id, 0.5)


def do_convert(uid: int, from_bot: str, to_bot: str, amount: float) -> Tuple[bool, float, str]:
    """Конвертация валюты между ботами."""
    from_bal = db.get_balance(uid, from_bot)
    if from_bal == float('inf'):
        return False, 0, "Владельцы не могут конвертировать"
    if from_bal < amount:
        return False, 0, "Недостаточно средств"

    from_rate = get_exchange_rate(from_bot)
    to_rate = get_exchange_rate(to_bot)
    converted = amount * from_rate / to_rate

    db.set_balance(uid, from_bot, from_bal - amount)
    db.add_balance(uid, to_bot, converted)
    return True, converted, "OK"


def reset_all_rates():
    """Сброс и блокировка курсов."""
    config.exchange_rates.rates_locked = True
    config.exchange_rates.rates = {"main": 1.0}
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != "main":
            config.exchange_rates.rates[bot_id] = bot_cfg.base_exchange_rate
    config.save()


def serialize_entities(entities) -> Optional[List[Dict]]:
    """Сериализация entities для сохранения."""
    if not entities:
        return None
    return [
        {
            'type': e.type, 'offset': e.offset, 'length': e.length,
            'url': e.url, 'language': e.language, 'custom_emoji_id': e.custom_emoji_id
        }
        for e in entities
    ]


def restore_entities(data: Optional[List[Dict]]) -> Optional[List[MessageEntity]]:
    """Восстановление entities из сохранённых данных."""
    if not data:
        return None
    result = [
        MessageEntity(
            type=e['type'], offset=e['offset'], length=e['length'],
            url=e.get('url'), language=e.get('language'),
            custom_emoji_id=e.get('custom_emoji_id')
        )
        for e in data
    ]
    return result if result else None


# ====================== ВСТРОЕННАЯ ВИКТОРИНА ======================

BUILT_IN_QUIZ = {
    1: {
        "question": "Кто должен был быть на месте Ореолы?",
        "answers": ["Небесный", "Ледяной", "Радужный"],
        "correct": 0
    },
    2: {
        "question": "Кто был отцом Мракокрада?",
        "answers": ["Гений", "Вдумчивый", "Арктик"],
        "correct": 2
    },
    3: {
        "question": "Кто убивал дочерей Коралл?",
        "answers": ["Мальстрём", "Орка", "Акула"],
        "correct": 1
    },
}


# ====================== КЛАВИАТУРЫ ======================

def build_main_menu(bot_id: str) -> InlineKeyboardMarkup:
    """Главное меню бота."""
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return InlineKeyboardMarkup(inline_keyboard=[])

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💰 Заработать", callback_data="earn"),
        InlineKeyboardButton(text="💸 Перевести", callback_data="transfer")
    )
    builder.row(
        InlineKeyboardButton(text="🏆 Топ", callback_data="top"),
        InlineKeyboardButton(text="💳 Баланс", callback_data="balance")
    )
    if "takes" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="📝 Отправить тейк", callback_data="send_take"))
    if "shop" in bot_cfg.modules:
        builder.row(InlineKeyboardButton(text="🛒 Магазин", callback_data="shop"))
    builder.row(
        InlineKeyboardButton(text="💱 Конвертация", callback_data="convert"),
        InlineKeyboardButton(text="📊 Курсы", callback_data="rates")
    )
    if bot_id == "main":
        builder.row(InlineKeyboardButton(text="🤖 Подключить бота", callback_data="connect_bot"))
    return builder.as_markup()


def build_shop_menu(bot_id: str) -> InlineKeyboardMarkup:
    """Меню магазина."""
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
    """Админ-панель."""
    builder = InlineKeyboardBuilder()
    bot_cfg = config.bots.get(bot_id)
    is_owner_user = check_owner(uid, bot_id)
    is_main = uid == MAIN_ADMIN_ID and bot_id == "main"

    if is_owner_user or is_main:
        builder.row(
            InlineKeyboardButton(text="📋 Пользователи", callback_data="adm_users"),
            InlineKeyboardButton(text="💰 Списать", callback_data="adm_deduct")
        )
        builder.row(
            InlineKeyboardButton(text="❄️ Заморозить", callback_data="adm_freeze"),
            InlineKeyboardButton(text="🔥 Разморозить", callback_data="adm_unfreeze")
        )
        builder.row(
            InlineKeyboardButton(text="🔧 Цензура", callback_data="adm_censor"),
            InlineKeyboardButton(text="👮 Модераторы", callback_data="adm_mods")
        )

        if bot_cfg and "takes" in bot_cfg.modules:
            pause_label = "▶️ Включить тейки" if bot_cfg.takes_paused else "⏸ Отключить тейки"
            builder.row(InlineKeyboardButton(text=pause_label, callback_data="adm_toggle_takes"))

            manual_label = "🔓 Авто-контроль" if bot_cfg.manual_control else "🔒 Ручной контроль"
            builder.row(InlineKeyboardButton(text=manual_label, callback_data="adm_toggle_manual"))

            builder.row(InlineKeyboardButton(text="🎯 Провести викторину", callback_data="adm_channel_quiz"))

        if is_main:
            builder.row(
                InlineKeyboardButton(text="📊 Сбросить курсы", callback_data="adm_reset_rates"),
                InlineKeyboardButton(text="🔄 Сбросить топ", callback_data="adm_reset_top")
            )

    builder.row(
        InlineKeyboardButton(text="💳 Баланс", callback_data="adm_balance"),
        InlineKeyboardButton(text="👤 Пользователь", callback_data="user_mode")
    )
    return builder.as_markup()


def build_cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопка отмены."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


def build_censor_menu() -> InlineKeyboardMarkup:
    """Меню цензуры."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Слово", callback_data="censor_add"),
        InlineKeyboardButton(text="➖ Слово", callback_data="censor_del")
    )
    builder.row(
        InlineKeyboardButton(text="➕ Маркер", callback_data="marker_add"),
        InlineKeyboardButton(text="➖ Маркер", callback_data="marker_del")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список", callback_data="censor_list"),
        InlineKeyboardButton(text="◀️ Назад", callback_data="admin_mode")
    )
    return builder.as_markup()


def build_mods_menu() -> InlineKeyboardMarkup:
    """Меню модераторов."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Назначить", callback_data="mod_assign"),
        InlineKeyboardButton(text="➖ Снять", callback_data="mod_remove")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список", callback_data="mod_list"),
        InlineKeyboardButton(text="◀️ Назад", callback_data="admin_mode")
    )
    return builder.as_markup()


def build_currency_keyboard(exclude: str = None) -> InlineKeyboardMarkup:
    """Выбор валюты для конвертации."""
    builder = InlineKeyboardBuilder()
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != exclude:
            builder.row(InlineKeyboardButton(
                text=f"{bot_cfg.currency_name} {bot_cfg.currency_emoji}",
                callback_data=f"currency_{bot_id}"
            ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()


def build_modules_keyboard() -> InlineKeyboardMarkup:
    """Выбор модулей при подключении бота."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Только тейки", callback_data="module_takes"))
    builder.row(InlineKeyboardButton(text="🛒 Только магазин", callback_data="module_shop"))
    builder.row(InlineKeyboardButton(text="📝🛒 Всё", callback_data="module_all"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))
    return builder.as_markup()


def build_take_moderation_keyboard(take_id: str, uid: int, is_blocked: bool) -> InlineKeyboardMarkup:
    """Клавиатура модерации тейка."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Отправить", callback_data=f"take_approve_{take_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"take_reject_{take_id}")
    )
    if is_blocked:
        builder.row(InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"user_unblock_{uid}"))
    else:
        builder.row(InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"user_block_{uid}"))
    return builder.as_markup()


def build_promo_confirm_keyboard() -> InlineKeyboardMarkup:
    """Подтверждение пиара."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Оплатить", callback_data="promo_pay"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    ]])


def build_quiz_keyboard(question_num: int) -> InlineKeyboardMarkup:
    """Варианты ответов на вопрос викторины."""
    builder = InlineKeyboardBuilder()
    for i, answer in enumerate(BUILT_IN_QUIZ[question_num]["answers"]):
        builder.row(InlineKeyboardButton(text=answer, callback_data=f"quiz_{question_num}_{i}"))
    return builder.as_markup()


# ====================== ПЕРЕСЫЛКА ТЕЙКА ======================

async def forward_take_to_channel(message: types.Message, bot_id: str, bot_instance: Bot) -> Optional[types.Message]:
    """Пересылает тейк в канал с цензурой мата."""
    try:
        bot_cfg = config.bots.get(bot_id)
        if not bot_cfg or not bot_cfg.takes_channel:
            return None

        text = message.text or message.caption or ""
        censored, has_profanity = censor_profanity(text, bot_id)
        send_kwargs = {
            "caption": censored if has_profanity else text,
            "parse_mode": "HTML" if has_profanity else None
        }

        if message.photo:
            return await bot_instance.send_photo(bot_cfg.takes_channel, photo=message.photo[-1].file_id, **send_kwargs)
        elif message.video:
            return await bot_instance.send_video(bot_cfg.takes_channel, video=message.video.file_id, **send_kwargs)
        elif message.animation:
            return await bot_instance.send_animation(bot_cfg.takes_channel, animation=message.animation.file_id, **send_kwargs)
        elif message.document:
            return await bot_instance.send_document(bot_cfg.takes_channel, document=message.document.file_id, **send_kwargs)
        elif message.voice:
            return await bot_instance.send_voice(bot_cfg.takes_channel, voice=message.voice.file_id, **send_kwargs)
        elif message.audio:
            return await bot_instance.send_audio(bot_cfg.takes_channel, audio=message.audio.file_id, **send_kwargs)
        elif message.sticker:
            return await bot_instance.send_sticker(bot_cfg.takes_channel, sticker=message.sticker.file_id)
        else:
            return await bot_instance.send_message(
                bot_cfg.takes_channel,
                censored if has_profanity else text,
                parse_mode="HTML" if has_profanity else None
            )
    except Exception as e:
        logger.error(f"Ошибка пересылки тейка: {e}")
        return None


# ====================== ОТЛОЖЕННОЕ УДАЛЕНИЕ ПИАРА ======================

async def delayed_delete_message(bot_instance: Bot, channel: str, message_id: int,
                                  hours: float, is_pinned: bool, deletion_id: str):
    """Удаляет сообщение пиара через указанное время. Данные задачи удаляются после."""
    try:
        await asyncio.sleep(hours * 3600)

        if is_pinned:
            try:
                await bot_instance.unpin_chat_message(channel, message_id)
                logger.info(f"Откреплено сообщение {message_id}")
            except Exception as e:
                logger.error(f"Ошибка открепления: {e}")

        try:
            await bot_instance.delete_message(channel, message_id)
            logger.info(f"Удалено сообщение пиара {message_id}")
        except Exception as e:
            logger.error(f"Ошибка удаления: {e}")

        if deletion_id in config.scheduled_deletions:
            del config.scheduled_deletions[deletion_id]
            config.save()
            logger.info(f"Задача {deletion_id} удалена из хранилища")

    except asyncio.CancelledError:
        logger.info(f"Задача удаления {deletion_id} отменена")
    except Exception as e:
        logger.error(f"Ошибка отложенного удаления: {e}")


# ====================== АУКЦИОН ======================

async def run_auction_timer(bot_instance: Bot, bot_id: str, auction_id: str):
    """
    Таймер аукциона:
    - Ждёт 4 минуты после последней ставки
    - Через 2 минуты начинает обратный отсчёт: 3... 2... 1...
    - Если новая ставка — сброс таймера
    - Без новых ставок — объявляет победителя
    """
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return

    try:
        while True:
            auction = config.active_auctions.get(auction_id)
            if not auction:
                return

            channel = auction['channel']
            message_id = auction['message_id']
            last_bid_time = datetime.fromisoformat(auction['last_bid_time'])

            # Ждём 4 минуты с последней ставки
            wait_until = last_bid_time + timedelta(minutes=4)
            now = datetime.now()
            if now < wait_until:
                await asyncio.sleep((wait_until - now).total_seconds())

            # Проверяем новую ставку
            auction = config.active_auctions.get(auction_id)
            if not auction:
                return
            new_last = datetime.fromisoformat(auction['last_bid_time'])
            if new_last > last_bid_time:
                continue  # Новая ставка — начинаем заново

            # Обратный отсчёт: 3, 2, 1
            countdown_broken = False
            for count in [3, 2, 1]:
                auction = config.active_auctions.get(auction_id)
                if not auction:
                    return

                try:
                    await bot_instance.send_message(
                        channel, str(count),
                        reply_to_message_id=message_id
                    )
                except Exception:
                    pass

                await asyncio.sleep(30)

                # Проверяем новую ставку во время отсчёта
                auction = config.active_auctions.get(auction_id)
                if not auction:
                    return
                check_last = datetime.fromisoformat(auction['last_bid_time'])
                if check_last > new_last:
                    countdown_broken = True
                    break  # Новая ставка — сброс

            if countdown_broken:
                continue  # Начинаем заново

            # Объявляем победителя
            auction = config.active_auctions.get(auction_id)
            if not auction:
                return

            winner_id = auction.get('current_bidder')
            winner_amount = auction.get('current_bid', 0)

            if winner_id:
                winner = db.get_user(winner_id)
                winner_name = winner['name'] if winner else "Неизвестный"

                try:
                    await bot_instance.send_message(
                        channel,
                        f"🏆 {winner_name}, вы выиграли аукцион!\n"
                        f"💰 Ставка: {winner_amount} {bot_cfg.currency_emoji}",
                        reply_to_message_id=message_id
                    )
                except Exception:
                    pass

                # Списываем со счёта победителя
                db.deduct_balance(winner_id, bot_id, winner_amount)

            # Удаляем аукцион
            if auction_id in config.active_auctions:
                del config.active_auctions[auction_id]
                config.save()
            return

    except asyncio.CancelledError:
        logger.info(f"Аукцион {auction_id} отменён")
    except Exception as e:
        logger.error(f"Ошибка аукциона: {e}")

# ====================== ОБРАБОТЧИКИ БОТА ======================

def create_bot_handlers(bot_id: str, bot_instance: Bot, dp: Dispatcher):
    """Создаёт все обработчики для конкретного бота."""
    router = Router()
    bot_config = config.bots.get(bot_id)

    # =================== КОМАНДЫ ===================

    @router.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        await state.clear()
        register_user(message.from_user, bot_id)
        cfg = config.bots.get(bot_id)

        if check_admin(message.from_user.id, bot_id):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="👤 Пользователь", callback_data="user_mode"),
                InlineKeyboardButton(text="⚙️ Админ", callback_data="admin_mode")
            ]])
            await message.answer(
                f"👋 Привет, администратор!\nВалюта: {cfg.currency_name} {cfg.currency_emoji}",
                reply_markup=keyboard
            )
        else:
            await message.answer(
                f"👋 Добро пожаловать!\nВалюта: {cfg.currency_name} {cfg.currency_emoji}",
                reply_markup=build_main_menu(bot_id)
            )

    @router.message(Command("cancel"))
    async def cmd_cancel(message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("Действие отменено.", reply_markup=build_main_menu(bot_id))

    # =================== ОБЩИЕ CALLBACK ===================

    @router.callback_query(F.data == "cancel")
    async def callback_cancel(callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Действие отменено.", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "user_mode")
    async def callback_user_mode(callback: types.CallbackQuery):
        await callback.message.edit_text("Выберите действие:", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    @router.callback_query(F.data == "admin_mode")
    async def callback_admin_mode(callback: types.CallbackQuery):
        if check_admin(callback.from_user.id, bot_id):
            await callback.message.edit_text(
                "Админ-панель:",
                reply_markup=build_admin_menu(callback.from_user.id, bot_id)
            )
        else:
            await callback.answer("Нет доступа", show_alert=True)
        await callback.answer()

    @router.callback_query(F.data == "back_main")
    async def callback_back_main(callback: types.CallbackQuery):
        await callback.message.edit_text("Выберите действие:", reply_markup=build_main_menu(bot_id))
        await callback.answer()

    # =================== БАЛАНС (ВСЕ ВАЛЮТЫ) ===================

    @router.callback_query(F.data == "balance")
    async def callback_balance(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        user_data = db.get_bot_data(callback.from_user.id, bot_id)

        status_text = ""
        if user_data.get('is_frozen'):
            status_text += "❄️ Счёт заморожен\n"
        if user_data.get('is_blocked'):
            status_text += "🚫 Заблокирован для тейков\n"

        main_balance = db.get_balance(callback.from_user.id, bot_id)
        main_bal_str = "∞" if main_balance == float('inf') else f"{main_balance:.0f}"

        text = f"{status_text}💳 Балансы:\n\n"
        text += f"▸ {cfg.currency_name} {cfg.currency_emoji}: {main_bal_str} (текущий)\n"

        for other_bot_id, other_cfg in config.bots.items():
            if other_bot_id != bot_id:
                other_balance = db.get_balance(callback.from_user.id, other_bot_id)
                if other_balance > 0 or other_balance == float('inf'):
                    other_str = "∞" if other_balance == float('inf') else f"{other_balance:.0f}"
                    text += f"▸ {other_cfg.currency_name} {other_cfg.currency_emoji}: {other_str}\n"

        can_take, cooldown_msg = can_send_take(callback.from_user.id, bot_id)
        text += f"\n📝 Тейки: {'✅ доступно' if can_take else f'⏳ {cooldown_msg}'}"

        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    # =================== ТОП ===================

    @router.callback_query(F.data == "top")
    async def callback_top(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        users_list = db.get_all_users_for_bot(bot_id)

        filtered = [
            (u['username'], u['balance'])
            for u in users_list
            if u.get('show_in_top') and not u.get('is_owner') and not u.get('is_infinite')
        ]
        filtered.sort(key=lambda x: x[1], reverse=True)

        text = f"🏆 Топ {cfg.currency_name}:\n\n"
        for i, (username, balance) in enumerate(filtered[:10], 1):
            text += f"{i}. @{username} — {balance:.0f} {cfg.currency_emoji}\n"
        if not filtered:
            text += "Пока пусто"

        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    # =================== ПЕРЕВОДЫ ===================

    @router.callback_query(F.data == "transfer")
    async def callback_transfer(callback: types.CallbackQuery, state: FSMContext):
        user_data = db.get_bot_data(callback.from_user.id, bot_id)
        if user_data.get('is_frozen'):
            await callback.answer("❄️ Ваш счёт заморожен!", show_alert=True)
            return
        await callback.message.edit_text(
            "Введите username или ID получателя:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(TransferStates.WaitingReceiver)
        await callback.answer()

    @router.message(TransferStates.WaitingReceiver)
    async def transfer_receiver(message: types.Message, state: FSMContext):
        receiver_id = db.find_user_by_input(message.text)
        if not receiver_id:
            await message.answer("Пользователь не найден. Попробуйте ещё раз:", reply_markup=build_cancel_keyboard())
            return
        if receiver_id == message.from_user.id:
            await message.answer("Нельзя перевести самому себе.", reply_markup=build_cancel_keyboard())
            return
        receiver_data = db.get_bot_data(receiver_id, bot_id)
        if receiver_data.get('is_frozen'):
            await message.answer("❄️ Счёт получателя заморожен.", reply_markup=build_cancel_keyboard())
            return

        await state.update_data(receiver_id=receiver_id)
        await message.answer(
            "Введите сумму перевода.\n"
            "Можете добавить сообщение на новой строке:\n\n"
            "Пример:\n100\nСпасибо!",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(TransferStates.WaitingAmountAndMessage)

    @router.message(TransferStates.WaitingAmountAndMessage)
    async def transfer_amount(message: types.Message, state: FSMContext):
        lines = message.text.strip().split('\n', 1)
        try:
            amount = float(lines[0])
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введите корректную сумму.", reply_markup=build_cancel_keyboard())
            return

        transfer_msg = lines[1].strip() if len(lines) > 1 else ""
        data = await state.get_data()
        receiver_id = data['receiver_id']
        cfg = config.bots.get(bot_id)

        success, error = do_transfer(message.from_user.id, receiver_id, bot_id, amount)
        if success:
            receiver = db.get_user(receiver_id)
            sender = db.get_user(message.from_user.id)
            await message.answer(
                f"✅ Переведено {amount:.0f} {cfg.currency_emoji} пользователю @{receiver['username']}",
                reply_markup=build_main_menu(bot_id)
            )

            notification = f"💰 Получено {amount:.0f} {cfg.currency_emoji} от @{sender['username']}"
            if transfer_msg:
                notification += f"\n💬 Сообщение: {transfer_msg}"
            try:
                await bot_instance.send_message(receiver_id, notification)
            except Exception:
                pass
        else:
            await message.answer(f"❌ {error}", reply_markup=build_main_menu(bot_id))

        await state.clear()

    # =================== КУРСЫ ВАЛЮТ ===================

    @router.callback_query(F.data == "rates")
    async def callback_rates(callback: types.CallbackQuery):
        text = "📊 Курсы валют:\n\n"
        for bid, cfg in config.bots.items():
            rate = get_exchange_rate(bid)
            text += f"{cfg.currency_name} {cfg.currency_emoji}: {rate:.2f}\n"
        if config.exchange_rates.rates_locked:
            text += "\n🔒 Курсы зафиксированы"
        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

    # =================== КОНВЕРТАЦИЯ ===================

    @router.callback_query(F.data == "convert")
    async def callback_convert(callback: types.CallbackQuery, state: FSMContext):
        balance = db.get_balance(callback.from_user.id, bot_id)
        if balance == float('inf'):
            await callback.answer("Владельцы не могут конвертировать", show_alert=True)
            return
        await callback.message.edit_text(
            "Выберите валюту, ИЗ которой конвертировать:",
            reply_markup=build_currency_keyboard()
        )
        await state.set_state(ConvertStates.WaitingSource)
        await callback.answer()

    @router.callback_query(ConvertStates.WaitingSource, F.data.startswith("currency_"))
    async def convert_source(callback: types.CallbackQuery, state: FSMContext):
        source_bot = callback.data[9:]
        await state.update_data(source_bot=source_bot)
        await callback.message.edit_text(
            "Выберите валюту, В которую конвертировать:",
            reply_markup=build_currency_keyboard(source_bot)
        )
        await state.set_state(ConvertStates.WaitingTarget)
        await callback.answer()

    @router.callback_query(ConvertStates.WaitingTarget, F.data.startswith("currency_"))
    async def convert_target(callback: types.CallbackQuery, state: FSMContext):
        target_bot = callback.data[9:]
        await state.update_data(target_bot=target_bot)
        data = await state.get_data()
        source_cfg = config.bots.get(data['source_bot'])
        target_cfg = config.bots.get(target_bot)
        source_rate = get_exchange_rate(data['source_bot'])
        target_rate = get_exchange_rate(target_bot)
        balance = db.get_balance(callback.from_user.id, data['source_bot'])

        await callback.message.edit_text(
            f"Курс: 1 {source_cfg.currency_emoji} = {source_rate/target_rate:.2f} {target_cfg.currency_emoji}\n"
            f"Ваш баланс: {balance:.0f} {source_cfg.currency_emoji}\n\n"
            f"Введите сумму для конвертации:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(ConvertStates.WaitingAmount)
        await callback.answer()

    @router.message(ConvertStates.WaitingAmount)
    async def convert_amount(message: types.Message, state: FSMContext):
        try:
            amount = float(message.text.strip())
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введите корректную сумму.", reply_markup=build_cancel_keyboard())
            return

        data = await state.get_data()
        success, converted, error = do_convert(
            message.from_user.id, data['source_bot'], data['target_bot'], amount
        )
        source_cfg = config.bots.get(data['source_bot'])
        target_cfg = config.bots.get(data['target_bot'])

        if success:
            await message.answer(
                f"✅ Конвертировано:\n"
                f"{amount:.0f} {source_cfg.currency_emoji} → {converted:.0f} {target_cfg.currency_emoji}",
                reply_markup=build_main_menu(bot_id)
            )
        else:
            await message.answer(f"❌ {error}", reply_markup=build_main_menu(bot_id))

        await state.clear()

    # =================== ЗАРАБОТОК / ВСТРОЕННАЯ ВИКТОРИНА ===================

    @router.callback_query(F.data == "earn")
    async def callback_earn(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        builder = InlineKeyboardBuilder()
        if cfg.channel_url:
            builder.row(InlineKeyboardButton(text="📢 Перейти в канал", url=cfg.channel_url))
        builder.row(InlineKeyboardButton(text="❓ Викторина", callback_data="quiz_start"))
        builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
        await callback.message.edit_text("Способы заработка:", reply_markup=builder.as_markup())
        await callback.answer()

    @router.callback_query(F.data == "quiz_start")
    async def callback_quiz_start(callback: types.CallbackQuery, state: FSMContext):
        user_data = db.get_bot_data(callback.from_user.id, bot_id)
        if user_data.get('quiz_passed'):
            await callback.answer("Вы уже прошли викторину", show_alert=True)
            return
        await callback.message.edit_text(
            f"Вопрос 1:\n{BUILT_IN_QUIZ[1]['question']}",
            reply_markup=build_quiz_keyboard(1)
        )
        await state.set_state(QuizStates.Question1)
        await callback.answer()

    @router.callback_query(QuizStates.Question1, F.data.startswith("quiz_1_"))
    async def quiz_answer_1(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[1]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            await callback.message.edit_text(
                f"✅ Правильно! +{cfg.quiz_reward} {cfg.currency_emoji}\n\n"
                f"Вопрос 2:\n{BUILT_IN_QUIZ[2]['question']}",
                reply_markup=build_quiz_keyboard(2)
            )
            await state.set_state(QuizStates.Question2)
        else:
            await callback.answer("❌ Неправильно!", show_alert=True)
            await callback.message.edit_text("Неправильный ответ.", reply_markup=build_main_menu(bot_id))
            await state.clear()
        await callback.answer()

    @router.callback_query(QuizStates.Question2, F.data.startswith("quiz_2_"))
    async def quiz_answer_2(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[2]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            await callback.message.edit_text(
                f"✅ Правильно! +{cfg.quiz_reward} {cfg.currency_emoji}\n\n"
                f"Вопрос 3:\n{BUILT_IN_QUIZ[3]['question']}",
                reply_markup=build_quiz_keyboard(3)
            )
            await state.set_state(QuizStates.Question3)
        else:
            await callback.answer("❌ Неправильно!", show_alert=True)
            await callback.message.edit_text("Неправильный ответ.", reply_markup=build_main_menu(bot_id))
            await state.clear()
        await callback.answer()

    @router.callback_query(QuizStates.Question3, F.data.startswith("quiz_3_"))
    async def quiz_answer_3(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if int(callback.data.split("_")[2]) == BUILT_IN_QUIZ[3]["correct"]:
            db.add_balance(callback.from_user.id, bot_id, cfg.quiz_reward)
            db.set_bot_data(callback.from_user.id, bot_id, quiz_passed=1)
            total_reward = cfg.quiz_reward * 3
            await callback.message.edit_text(
                f"🎉 Викторина пройдена!\n+{total_reward} {cfg.currency_emoji}",
                reply_markup=build_main_menu(bot_id)
            )
        else:
            await callback.answer("❌ Неправильно!", show_alert=True)
            await callback.message.edit_text("Неправильный ответ.", reply_markup=build_main_menu(bot_id))
        await state.clear()
        await callback.answer()

    # =================== ТЕЙКИ ===================

    if bot_config and "takes" in bot_config.modules:

        async def process_take_message(message: types.Message, bid: str, bot: Bot):
            """Общая логика обработки тейка."""
            uid = message.from_user.id
            register_user(message.from_user, bid)
            cfg = config.bots.get(bid)
            user_data = db.get_bot_data(uid, bid)

            if user_data.get('is_blocked'):
                await message.answer("🚫 Вы заблокированы для отправки тейков.")
                return False

            can_take, cooldown_msg = can_send_take(uid, bid)
            if not can_take:
                await message.answer(f"⏳ {cooldown_msg}")
                return False

            text = message.text or message.caption or ""

            # Тейки на паузе
            if cfg.takes_paused:
                take_data = {
                    'user_id': uid, 'bot_id': bid, 'text': text,
                    'photo': message.photo[-1].file_id if message.photo else None,
                    'video': message.video.file_id if message.video else None,
                    'animation': message.animation.file_id if message.animation else None,
                    'document': message.document.file_id if message.document else None,
                    'caption': message.caption,
                    'timestamp': datetime.now().isoformat()
                }
                if bid not in config.paused_takes:
                    config.paused_takes[bid] = []
                config.paused_takes[bid].append(take_data)
                config.save()
                db.add_take_timestamp(uid, bid)
                await message.answer(
                    "⏸ Тейки сейчас на паузе. Ваш тейк будет отправлен когда тейки включат.",
                    reply_markup=build_main_menu(bid)
                )
                return True

            # Модерация
            needs_moderation = cfg.manual_control
            moderation_reason = "Ручной контроль"

            if not needs_moderation:
                if contains_marker_words(text, bid):
                    needs_moderation = True
                    moderation_reason = "Маркерное слово"

            if not needs_moderation:
                has_group_link, link_reason = await check_telegram_links(text, bot)
                if has_group_link:
                    needs_moderation = True
                    moderation_reason = link_reason

            if needs_moderation:
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
                            await bot.send_message(
                                mod_uid,
                                f"⚠️ Тейк на модерации\nПричина: {moderation_reason}\n\n{text}",
                                reply_markup=build_take_moderation_keyboard(take_id, uid, bool(is_blocked))
                            )
                            await message.copy_to(mod_uid)
                        except Exception as e:
                            logger.error(f"Ошибка отправки модератору {mod_uid}: {e}")

                await message.answer("📝 Тейк отправлен на модерацию.", reply_markup=build_main_menu(bid))
                return True
            else:
                sent = await forward_take_to_channel(message, bid, bot)
                if sent:
                    db.add_take_timestamp(uid, bid)
                    await message.answer("✅ Тейк отправлен в канал!", reply_markup=build_main_menu(bid))
                    return True
                else:
                    await message.answer("❌ Ошибка при отправке тейка.", reply_markup=build_main_menu(bid))
                    return False

        @router.callback_query(F.data == "send_take")
        async def callback_send_take(callback: types.CallbackQuery, state: FSMContext):
            user_data = db.get_bot_data(callback.from_user.id, bot_id)
            if user_data.get('is_blocked'):
                await callback.answer("🚫 Вы заблокированы", show_alert=True)
                return

            can_take, cooldown_msg = can_send_take(callback.from_user.id, bot_id)
            if not can_take:
                await callback.answer(f"⏳ {cooldown_msg}", show_alert=True)
                return

            cfg = config.bots.get(bot_id)
            pause_text = " ⏸ (на паузе — будет отправлен позже)" if cfg.takes_paused else ""
            await callback.message.edit_text(
                f"📝 Отправьте тейк с хештегом #тейк{pause_text}\n"
                f"⏱ Кулдаун: {cfg.take_cooldown_minutes} мин",
                reply_markup=build_cancel_keyboard()
            )
            await state.set_state(TakeStates.WaitingTake)
            await callback.answer()

        @router.message(TakeStates.WaitingTake)
        async def process_take_from_button(message: types.Message, state: FSMContext):
            text = message.text or message.caption or ""
            if "#тейк" not in text.lower():
                await message.answer("⚠️ Добавьте #тейк в сообщение!", reply_markup=build_cancel_keyboard())
                return
            await process_take_message(message, bot_id, bot_instance)
            await state.clear()

        @router.message(F.text.contains("#тейк") | F.caption.contains("#тейк"))
        async def auto_forward_take(message: types.Message, state: FSMContext):
            current_state = await state.get_state()
            if current_state == TakeStates.WaitingTake:
                return
            await process_take_message(message, bot_id, bot_instance)

        # Модерация тейков
        @router.callback_query(F.data.startswith("take_approve_"))
        async def take_approve(callback: types.CallbackQuery):
            take_id = callback.data[13:]
            take_data = config.pending_takes.get(take_id)
            if not take_data:
                await callback.answer("Тейк не найден", show_alert=True)
                return

            cfg = config.bots.get(take_data['bot_id'])
            try:
                text = take_data.get('caption') or take_data.get('text', '')
                censored, has_profanity = censor_profanity(text, bot_id)
                send_kwargs = {
                    "caption": censored if has_profanity else text,
                    "parse_mode": "HTML" if has_profanity else None
                }

                if take_data.get('photo'):
                    await bot_instance.send_photo(cfg.takes_channel, photo=take_data['photo'], **send_kwargs)
                elif take_data.get('video'):
                    await bot_instance.send_video(cfg.takes_channel, video=take_data['video'], **send_kwargs)
                elif take_data.get('animation'):
                    await bot_instance.send_animation(cfg.takes_channel, animation=take_data['animation'], **send_kwargs)
                elif take_data.get('document'):
                    await bot_instance.send_document(cfg.takes_channel, document=take_data['document'], **send_kwargs)
                elif take_data.get('voice'):
                    await bot_instance.send_voice(cfg.takes_channel, voice=take_data['voice'], **send_kwargs)
                elif take_data.get('audio'):
                    await bot_instance.send_audio(cfg.takes_channel, audio=take_data['audio'], **send_kwargs)
                elif take_data.get('sticker'):
                    await bot_instance.send_sticker(cfg.takes_channel, sticker=take_data['sticker'])
                else:
                    await bot_instance.send_message(
                        cfg.takes_channel,
                        censored if has_profanity else take_data['text'],
                        parse_mode="HTML" if has_profanity else None
                    )

                db.add_take_timestamp(take_data['user_id'], bot_id)
                del config.pending_takes[take_id]
                config.save()

                await callback.message.edit_text("✅ Тейк одобрен и отправлен в канал.")
                try:
                    await bot_instance.send_message(take_data['user_id'], "✅ Ваш тейк одобрен!")
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"Ошибка одобрения тейка: {e}")
                await callback.answer(f"Ошибка: {e}", show_alert=True)
            await callback.answer()

        @router.callback_query(F.data.startswith("take_reject_"))
        async def take_reject(callback: types.CallbackQuery):
            take_id = callback.data[12:]
            take_data = config.pending_takes.get(take_id)
            if take_data:
                try:
                    await bot_instance.send_message(take_data['user_id'], "❌ Ваш тейк отклонён модератором.")
                except Exception:
                    pass
                del config.pending_takes[take_id]
                config.save()
            await callback.message.edit_text("❌ Тейк отклонён.")
            await callback.answer()

        @router.callback_query(F.data.startswith("user_block_"))
        async def block_user_callback(callback: types.CallbackQuery):
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            uid = int(callback.data[11:])
            db.set_bot_data(uid, bot_id, is_blocked=1)
            try:
                await bot_instance.send_message(uid, "🚫 Вы заблокированы для тейков.")
            except Exception:
                pass
            await callback.answer("🚫 Пользователь заблокирован", show_alert=True)

        @router.callback_query(F.data.startswith("user_unblock_"))
        async def unblock_user_callback(callback: types.CallbackQuery):
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            uid = int(callback.data[13:])
            db.set_bot_data(uid, bot_id, is_blocked=0)
            try:
                await bot_instance.send_message(uid, "✅ Вы разблокированы.")
            except Exception:
                pass
            await callback.answer("✅ Разблокирован", show_alert=True)

    # =================== МАГАЗИН / ПИАР ===================

    if bot_config and "shop" in bot_config.modules:

        @router.callback_query(F.data == "shop")
        async def callback_shop(callback: types.CallbackQuery):
            await callback.message.edit_text("🛒 Магазин:", reply_markup=build_shop_menu(bot_id))
            await callback.answer()

        # Автопересылка объявлений
        @router.message(
            F.text.contains("#продажа") | F.caption.contains("#продажа") |
            F.text.contains("#обмен") | F.caption.contains("#обмен")
        )
        async def auto_forward_shop(message: types.Message, state: FSMContext):
            current_state = await state.get_state()
            if current_state:
                return
            cfg = config.bots.get(bot_id)
            try:
                if message.photo:
                    await bot_instance.send_photo(
                        cfg.shop_channel, photo=message.photo[-1].file_id, caption=message.caption
                    )
                else:
                    await bot_instance.send_message(cfg.shop_channel, message.text)
                await message.answer("✅ Объявление отправлено!")
            except Exception as e:
                logger.error(f"Ошибка автопересылки: {e}")

        # Пиар
        @router.callback_query(F.data.startswith("promo_"))
        async def callback_promo(callback: types.CallbackQuery, state: FSMContext):
            if callback.data == "promo_pay":
                data = await state.get_data()
                total_cost = data.get('total_cost', 0)
                cfg = config.bots.get(bot_id)

                if not db.deduct_balance(callback.from_user.id, bot_id, total_cost):
                    await callback.answer("Недостаточно средств!", show_alert=True)
                    return

                post_data = data.get('post_data', {})
                try:
                    sent_message = None
                    if post_data.get('is_forwarded'):
                        sent_message = await bot_instance.forward_message(
                            chat_id=cfg.takes_channel,
                            from_chat_id=post_data['forward_chat_id'],
                            message_id=post_data['forward_message_id']
                        )
                    elif post_data.get('photo'):
                        sent_message = await bot_instance.send_photo(
                            cfg.takes_channel, photo=post_data['photo'],
                            caption=post_data.get('caption'),
                            caption_entities=restore_entities(post_data.get('caption_entities'))
                        )
                    elif post_data.get('text'):
                        sent_message = await bot_instance.send_message(
                            cfg.takes_channel, post_data['text'],
                            entities=restore_entities(post_data.get('entities'))
                        )

                    if not sent_message:
                        raise Exception("Не удалось отправить")

                    is_pinned = data.get('is_pinned', False)
                    if is_pinned:
                        try:
                            await bot_instance.pin_chat_message(cfg.takes_channel, sent_message.message_id)
                        except Exception as e:
                            logger.error(f"Ошибка закрепления: {e}")

                    hours = data.get('hours', 1)
                    delete_at = (datetime.now() + timedelta(hours=hours)).isoformat()
                    deletion_id = f"{bot_id}_{sent_message.message_id}"

                    config.scheduled_deletions[deletion_id] = {
                        'bot_id': bot_id, 'channel': cfg.takes_channel,
                        'message_id': sent_message.message_id,
                        'delete_at': delete_at, 'is_pinned': is_pinned
                    }
                    config.save()

                    asyncio.create_task(delayed_delete_message(
                        bot_instance, cfg.takes_channel, sent_message.message_id,
                        hours, is_pinned, deletion_id
                    ))

                    db.set_bot_data(callback.from_user.id, bot_id, last_promo_at=datetime.now().isoformat())

                    pin_text = "📌 Закреплён и " if is_pinned else ""
                    await callback.message.edit_text(
                        f"✅ Пиар размещён на {hours}ч!\n{pin_text}будет удалён автоматически.",
                        reply_markup=build_main_menu(bot_id)
                    )

                except Exception as e:
                    db.add_balance(callback.from_user.id, bot_id, total_cost)
                    await callback.message.edit_text(
                        f"❌ Ошибка: {e}\nЛуны возвращены.",
                        reply_markup=build_main_menu(bot_id)
                    )

                await state.clear()
                await callback.answer()
                return

            # Начало оформления пиара
            is_pinned = callback.data == "promo_pinned"
            can_promo, promo_msg = can_use_promo(callback.from_user.id, bot_id)
            if not can_promo:
                await callback.answer(promo_msg, show_alert=True)
                return

            cfg = config.bots.get(bot_id)
            price_per_hour = cfg.promo_pin_price_per_hour if is_pinned else cfg.promo_price_per_hour

            await state.update_data(is_pinned=is_pinned, price_per_hour=price_per_hour)
            await callback.message.edit_text(
                f"{'📌 Пиар с закрепом' if is_pinned else '📢 Пиар'}\n"
                f"Стоимость: {price_per_hour} {cfg.currency_emoji}/час\n\n"
                f"Сколько часов?",
                reply_markup=build_cancel_keyboard()
            )
            await state.set_state(PromoStates.WaitingHours)
            await callback.answer()

        @router.message(PromoStates.WaitingHours)
        async def promo_hours(message: types.Message, state: FSMContext):
            try:
                hours = int(message.text)
                if hours <= 0:
                    raise ValueError
            except ValueError:
                await message.answer("Введите положительное число.", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(hours=hours)
            await message.answer(
                "Отправьте ваш рекламный пост:\n\n"
                "💡 Ссылки и форматирование сохранятся.\n"
                "📎 Пересланные сообщения сохранят 'Переслано из'.",
                reply_markup=build_cancel_keyboard()
            )
            await state.set_state(PromoStates.WaitingPost)

        @router.message(PromoStates.WaitingPost)
        async def promo_post(message: types.Message, state: FSMContext):
            post_data = {
                'text': message.text, 'caption': message.caption,
                'photo': message.photo[-1].file_id if message.photo else None,
                'entities': serialize_entities(message.entities),
                'caption_entities': serialize_entities(message.caption_entities),
                'forward_chat_id': message.chat.id,
                'forward_message_id': message.message_id,
                'is_forwarded': message.forward_origin is not None,
            }
            data = await state.get_data()
            total_cost = data['hours'] * data['price_per_hour']
            await state.update_data(post_data=post_data, total_cost=total_cost)

            cfg = config.bots.get(bot_id)
            balance = db.get_balance(message.from_user.id, bot_id)
            balance_str = "∞" if balance == float('inf') else f"{balance:.0f}"

            forward_note = "\n📎 Пересланное — сохранит 'Переслано из'" if post_data['is_forwarded'] else ""

            await message.answer(
                f"{'📌 Пиар с закрепом' if data['is_pinned'] else '📢 Пиар'}\n"
                f"⏱ Длительность: {data['hours']} ч.\n"
                f"💰 К оплате: {total_cost} {cfg.currency_emoji}\n"
                f"💳 Ваш баланс: {balance_str} {cfg.currency_emoji}"
                f"{forward_note}",
                reply_markup=build_promo_confirm_keyboard()
            )
            await state.set_state(PromoStates.WaitingConfirmation)

        # Покупка товара
        @router.callback_query(F.data == "buy_product")
        async def callback_buy_product(callback: types.CallbackQuery, state: FSMContext):
            await callback.message.edit_text("🛍 Отправьте фото товара:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingImage)
            await callback.answer()

        @router.message(PurchaseStates.WaitingImage)
        async def buy_image(message: types.Message, state: FSMContext):
            if not message.photo:
                await message.answer("Нужно отправить фото.", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(photo_id=message.photo[-1].file_id)
            await message.answer("Введите username или ID продавца:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingSeller)

        @router.message(PurchaseStates.WaitingSeller)
        async def buy_seller(message: types.Message, state: FSMContext):
            seller_id = db.find_user_by_input(message.text)
            if not seller_id:
                await message.answer("Продавец не найден.", reply_markup=build_cancel_keyboard())
                return
            if seller_id == message.from_user.id:
                await message.answer("Нельзя купить у себя.", reply_markup=build_cancel_keyboard())
                return
            await state.update_data(seller_id=seller_id)
            await message.answer("Введите сумму оплаты:", reply_markup=build_cancel_keyboard())
            await state.set_state(PurchaseStates.WaitingAmount)

        @router.message(PurchaseStates.WaitingAmount)
        async def buy_amount(message: types.Message, state: FSMContext):
            try:
                amount = float(message.text)
                if amount <= 0:
                    raise ValueError
            except ValueError:
                await message.answer("Введите корректную сумму.", reply_markup=build_cancel_keyboard())
                return

            data = await state.get_data()
            purchase_id = str(uuid.uuid4())[:8]
            buyer = db.get_user(message.from_user.id)

            config.pending_purchases[purchase_id] = {
                'buyer_id': message.from_user.id, 'seller_id': data['seller_id'],
                'amount': amount, 'photo_id': data['photo_id'], 'bot_id': bot_id
            }
            config.save()

            cfg = config.bots.get(bot_id)
            confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"purchase_ok_{purchase_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"purchase_no_{purchase_id}")
            ]])

            try:
                await bot_instance.send_photo(
                    data['seller_id'], photo=data['photo_id'],
                    caption=f"🛒 Запрос покупки!\nОт: @{buyer['username']}\nСумма: {amount} {cfg.currency_emoji}",
                    reply_markup=confirm_kb
                )
                await message.answer("✅ Запрос отправлен продавцу!", reply_markup=build_main_menu(bot_id))
            except Exception:
                del config.pending_purchases[purchase_id]
                config.save()
                await message.answer("❌ Не удалось связаться с продавцом.", reply_markup=build_main_menu(bot_id))

            await state.clear()

        @router.callback_query(F.data.startswith("purchase_ok_"))
        async def purchase_confirm(callback: types.CallbackQuery):
            purchase_id = callback.data[12:]
            purchase = config.pending_purchases.get(purchase_id)
            if not purchase:
                await callback.answer("Покупка не найдена", show_alert=True)
                return
            if callback.from_user.id != purchase['seller_id']:
                await callback.answer("Это не ваша продажа", show_alert=True)
                return

            cfg = config.bots.get(purchase['bot_id'])
            success, error = do_transfer(purchase['buyer_id'], purchase['seller_id'], purchase['bot_id'], purchase['amount'])
            if success:
                await callback.message.edit_caption(caption=f"✅ Продано! +{purchase['amount']} {cfg.currency_emoji}")
                try:
                    await bot_instance.send_message(
                        purchase['buyer_id'],
                        f"✅ Покупка подтверждена! -{purchase['amount']} {cfg.currency_emoji}"
                    )
                except Exception:
                    pass
            else:
                await callback.answer(error, show_alert=True)

            del config.pending_purchases[purchase_id]
            config.save()
            await callback.answer()

        @router.callback_query(F.data.startswith("purchase_no_"))
        async def purchase_reject(callback: types.CallbackQuery):
            purchase_id = callback.data[12:]
            purchase = config.pending_purchases.get(purchase_id)
            if purchase:
                try:
                    await bot_instance.send_message(purchase['buyer_id'], "❌ Покупка отклонена продавцом.")
                except Exception:
                    pass
                del config.pending_purchases[purchase_id]
                config.save()
            await callback.message.edit_caption(caption="❌ Продажа отклонена")
            await callback.answer()

    # =================== АДМИН-ПАНЕЛЬ ===================

    @router.callback_query(F.data == "adm_users")
    async def admin_users_list(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        users_list = db.get_all_users_for_bot(bot_id)
        text = "👥 Пользователи:\n\n"
        for user in users_list[:20]:
            balance_str = "∞" if user.get('is_infinite') else f"{user['balance']:.0f}"
            user_bot_data = db.get_bot_data(user['user_id'], bot_id)
            flags = ""
            if user_bot_data.get('is_frozen'): flags += "❄️"
            if user_bot_data.get('is_blocked'): flags += "🚫"
            if user_bot_data.get('is_moderator'): flags += "👮"
            text += f"@{user['username']}: {balance_str} {cfg.currency_emoji} {flags}\n"
        await callback.message.edit_text(text, reply_markup=build_admin_menu(callback.from_user.id, bot_id))
        await callback.answer()

    @router.callback_query(F.data == "adm_deduct")
    async def admin_deduct_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Введите username пользователя для списания:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForDeduct)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForDeduct)
    async def admin_deduct_username(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if not uid:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
            return
        await state.update_data(target_uid=uid)
        cfg = config.bots.get(bot_id)
        balance = db.get_balance(uid, bot_id)
        await message.answer(
            f"Баланс: {balance:.0f} {cfg.currency_emoji}\nСколько списать?",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AdminStates.WaitingAmountForDeduct)

    @router.message(AdminStates.WaitingAmountForDeduct)
    async def admin_deduct_amount(message: types.Message, state: FSMContext):
        try:
            amount = float(message.text)
        except ValueError:
            await message.answer("Введите число.", reply_markup=build_cancel_keyboard())
            return
        data = await state.get_data()
        cfg = config.bots.get(bot_id)
        if db.deduct_balance(data['target_uid'], bot_id, amount):
            await message.answer(
                f"✅ Списано {amount} {cfg.currency_emoji}",
                reply_markup=build_admin_menu(message.from_user.id, bot_id)
            )
        else:
            await message.answer("❌ Ошибка списания.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        await state.clear()

    @router.callback_query(F.data == "adm_freeze")
    async def admin_freeze_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Введите username для заморозки:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForFreeze)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForFreeze)
    async def admin_freeze_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_frozen=1)
            await message.answer("❄️ Счёт заморожен.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "adm_unfreeze")
    async def admin_unfreeze_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Введите username для разморозки:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingUsernameForUnfreeze)
        await callback.answer()

    @router.message(AdminStates.WaitingUsernameForUnfreeze)
    async def admin_unfreeze_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_frozen=0)
            await message.answer("🔥 Счёт разморожен.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    # Переключение тейков (пауза)
    @router.callback_query(F.data == "adm_toggle_takes")
    async def admin_toggle_takes(callback: types.CallbackQuery):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        cfg = config.bots.get(bot_id)
        if cfg.takes_paused:
            cfg.takes_paused = False
            paused_list = config.paused_takes.get(bot_id, [])
            sent_count = 0
            for take in paused_list:
                text = take.get('text', '')
                if contains_marker_words(text, bot_id) or cfg.manual_control:
                    take_id = str(uuid.uuid4())[:8]
                    config.pending_takes[take_id] = take
                else:
                    try:
                        censored, has_prof = censor_profanity(text, bot_id)
                        send_kwargs = {
                            "caption": censored if has_prof else text,
                            "parse_mode": "HTML" if has_prof else None
                        }
                        if take.get('photo'):
                            await bot_instance.send_photo(cfg.takes_channel, photo=take['photo'], **send_kwargs)
                        elif take.get('video'):
                            await bot_instance.send_video(cfg.takes_channel, video=take['video'], **send_kwargs)
                        else:
                            await bot_instance.send_message(
                                cfg.takes_channel,
                                censored if has_prof else text,
                                parse_mode="HTML" if has_prof else None
                            )
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Ошибка отправки из очереди: {e}")

            config.paused_takes[bot_id] = []
            config.save()
            await callback.answer(f"▶️ Тейки включены! Отправлено: {sent_count}", show_alert=True)
        else:
            cfg.takes_paused = True
            config.save()
            await callback.answer("⏸ Тейки поставлены на паузу", show_alert=True)

        await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    # Ручной контроль
    @router.callback_query(F.data == "adm_toggle_manual")
    async def admin_toggle_manual(callback: types.CallbackQuery):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        cfg = config.bots.get(bot_id)
        cfg.manual_control = not cfg.manual_control
        config.save()

        status = "🔒 Ручной контроль включён — все тейки на модерацию" if cfg.manual_control else "🔓 Авто-контроль включён"
        await callback.answer(status, show_alert=True)
        await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    # Викторина в канале
    @router.callback_query(F.data == "adm_channel_quiz")
    async def admin_channel_quiz_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text(
            "🎯 Провести викторину в канале\n\n"
            "Отправьте вопрос (текст, фото или видео с подписью):",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AdminStates.WaitingQuizQuestion)
        await callback.answer()

    @router.message(AdminStates.WaitingQuizQuestion)
    async def admin_quiz_question(message: types.Message, state: FSMContext):
        quiz_data = {
            'text': message.text or message.caption or '',
            'photo': message.photo[-1].file_id if message.photo else None,
            'video': message.video.file_id if message.video else None,
        }
        await state.update_data(quiz_data=quiz_data)
        await message.answer("Укажите награду (число валюты):", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingQuizReward)

    @router.message(AdminStates.WaitingQuizReward)
    async def admin_quiz_reward(message: types.Message, state: FSMContext):
        try:
            reward = int(message.text)
            if reward <= 0:
                raise ValueError
        except ValueError:
            await message.answer("Введите положительное число.", reply_markup=build_cancel_keyboard())
            return
        await state.update_data(quiz_reward=reward)
        await message.answer("Введите правильный ответ:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingQuizAnswer)

    @router.message(AdminStates.WaitingQuizAnswer)
    async def admin_quiz_answer(message: types.Message, state: FSMContext):
        data = await state.get_data()
        cfg = config.bots.get(bot_id)
        quiz_data = data['quiz_data']
        correct_answer = message.text.strip().lower()

        try:
            if quiz_data.get('photo'):
                sent = await bot_instance.send_photo(
                    cfg.takes_channel, photo=quiz_data['photo'], caption=quiz_data['text']
                )
            elif quiz_data.get('video'):
                sent = await bot_instance.send_video(
                    cfg.takes_channel, video=quiz_data['video'], caption=quiz_data['text']
                )
            else:
                sent = await bot_instance.send_message(cfg.takes_channel, quiz_data['text'])

            quiz_id = str(sent.message_id)
            config.active_quizzes[quiz_id] = {
                'bot_id': bot_id,
                'message_id': sent.message_id,
                'answer': correct_answer,
                'reward': data['quiz_reward'],
                'channel': cfg.takes_channel,
                'solved': False
            }
            config.save()

            await message.answer(
                f"✅ Викторина опубликована в канале!\n"
                f"Правильный ответ: {correct_answer}\n"
                f"Награда: {data['quiz_reward']} {cfg.currency_emoji}\n\n"
                f"Бот отслеживает комментарии под постом.",
                reply_markup=build_admin_menu(message.from_user.id, bot_id)
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}", reply_markup=build_admin_menu(message.from_user.id, bot_id))

        await state.clear()

    # Цензура
    @router.callback_query(F.data == "adm_censor")
    async def admin_censor_menu(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text(
            "🔧 Управление цензурой\nБазовые корни (мат) работают всегда.\nДобавляйте дополнительные слова.",
            reply_markup=build_censor_menu()
        )
        await callback.answer()

    @router.callback_query(F.data == "censor_add")
    async def censor_add_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите слово/корень для цензуры:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingCensorWord)
        await callback.answer()

    @router.message(AdminStates.WaitingCensorWord)
    async def censor_add_process(message: types.Message, state: FSMContext):
        word = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if word not in cfg.censored_words:
            cfg.censored_words.append(word)
            config.save()
        await message.answer(f"✅ Слово '{word}' добавлено в цензуру.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "censor_del")
    async def censor_del_start(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg.censored_words:
            await callback.answer("Список пуст.", show_alert=True)
            return
        await callback.message.edit_text(
            f"Дополнительные слова: {', '.join(cfg.censored_words)}\n\nВведите слово для удаления:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AdminStates.WaitingRemoveCensorWord)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveCensorWord)
    async def censor_del_process(message: types.Message, state: FSMContext):
        word = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if word in cfg.censored_words:
            cfg.censored_words.remove(word)
            config.save()
            await message.answer(f"✅ Слово '{word}' удалено.", reply_markup=build_censor_menu())
        else:
            await message.answer("Слово не найдено.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "marker_add")
    async def marker_add_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите маркер (тейки с ним → модерация):", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingMarkerWord)
        await callback.answer()

    @router.message(AdminStates.WaitingMarkerWord)
    async def marker_add_process(message: types.Message, state: FSMContext):
        word = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if word not in cfg.marker_words:
            cfg.marker_words.append(word)
            config.save()
        await message.answer(f"✅ Маркер '{word}' добавлен.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "marker_del")
    async def marker_del_start(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg.marker_words:
            await callback.answer("Список маркеров пуст.", show_alert=True)
            return
        await callback.message.edit_text(
            f"Маркеры: {', '.join(cfg.marker_words)}\n\nВведите маркер для удаления:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AdminStates.WaitingRemoveMarkerWord)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveMarkerWord)
    async def marker_del_process(message: types.Message, state: FSMContext):
        word = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if word in cfg.marker_words:
            cfg.marker_words.remove(word)
            config.save()
            await message.answer(f"✅ Маркер '{word}' удалён.", reply_markup=build_censor_menu())
        else:
            await message.answer("Маркер не найден.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "censor_list")
    async def censor_list_show(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        words = cfg.censored_words or ["(нет)"]
        markers = cfg.marker_words or ["(нет)"]
        await callback.message.edit_text(
            f"🔧 Цензура\n\n📋 Базовые корни: всегда активны\n"
            f"📋 Дополнительные: {', '.join(words)}\n"
            f"🏷 Маркеры: {', '.join(markers)}",
            reply_markup=build_censor_menu()
        )
        await callback.answer()

    # Модераторы
    @router.callback_query(F.data == "adm_mods")
    async def admin_mods_menu(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("👮 Управление модераторами:", reply_markup=build_mods_menu())
        await callback.answer()

    @router.callback_query(F.data == "mod_assign")
    async def mod_assign_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите username модератора:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingModeratorUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingModeratorUsername)
    async def mod_assign_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_moderator=1)
            await message.answer("✅ Назначен модератором.", reply_markup=build_mods_menu())
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "mod_remove")
    async def mod_remove_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите username для снятия:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingRemoveModeratorUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveModeratorUsername)
    async def mod_remove_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_moderator=0)
            await message.answer("✅ Снят с модераторов.", reply_markup=build_mods_menu())
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "mod_list")
    async def mod_list_show(callback: types.CallbackQuery):
        users_list = db.get_all_users_for_bot(bot_id)
        moderators = []
        for user in users_list:
            user_bot_data = db.get_bot_data(user['user_id'], bot_id)
            if user_bot_data.get('is_moderator'):
                user_info = db.get_user(user['user_id'])
                if user_info:
                    moderators.append(f"@{user_info['username']}")
        text = ", ".join(moderators) if moderators else "(нет модераторов)"
        await callback.message.edit_text(f"👮 Модераторы: {text}", reply_markup=build_mods_menu())
        await callback.answer()

    # Спецкнопки главного админа
    if bot_id == "main":
        @router.callback_query(F.data == "adm_reset_rates")
        async def admin_reset_rates(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            reset_all_rates()
            await callback.answer("✅ Курсы сброшены и заблокированы.", show_alert=True)
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

        @router.callback_query(F.data == "adm_reset_top")
        async def admin_reset_top(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            users_list = db.get_all_users_for_bot(bot_id)
            for user in users_list:
                db.set_bot_data(user['user_id'], bot_id, show_in_top=0)
            await callback.answer("✅ Топ сброшен.", show_alert=True)
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    @router.callback_query(F.data == "adm_balance")
    async def admin_show_balance(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        balance = db.get_balance(callback.from_user.id, bot_id)
        balance_str = "∞" if balance == float('inf') else f"{balance:.0f}"
        await callback.message.edit_text(
            f"💳 Ваш баланс: {balance_str} {cfg.currency_emoji}",
            reply_markup=build_admin_menu(callback.from_user.id, bot_id)
        )
        await callback.answer()

    # =================== ОТСЛЕЖИВАНИЕ КОММЕНТАРИЕВ В КАНАЛЕ ===================

    @router.channel_post()
    async def handle_channel_post(message: types.Message):
        """Отслеживание новых постов в канале — создание аукционов по #аукцион."""
        if not message.text:
            return

        cfg = config.bots.get(bot_id)
        if not cfg:
            return

        if "#аукцион" in message.text.lower():
            auction_id = str(message.message_id)
            config.active_auctions[auction_id] = {
                'bot_id': bot_id,
                'channel': message.chat.id,
                'message_id': message.message_id,
                'current_bidder': None,
                'current_bid': 0,
                'last_bid_time': datetime.now().isoformat(),
                'started': True
            }
            config.save()
            logger.info(f"Аукцион создан: {auction_id} в канале {message.chat.id}")

            task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, auction_id))
            config.auction_tasks[auction_id] = task

    @router.message(F.reply_to_message)
    async def handle_comment_reply(message: types.Message):
        """Обработка комментариев под постами канала — викторины и аукционы."""
        if not message.text:
            return
        if not message.reply_to_message:
            return

        cfg = config.bots.get(bot_id)
        if not cfg:
            return

        # Получаем ID оригинального поста в канале
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
        comment_text_lower = comment_text.lower()

        # ===== ПРОВЕРКА ВИКТОРИНЫ =====
        quiz = config.active_quizzes.get(original_msg_id)
        if quiz and not quiz.get('solved') and quiz.get('bot_id') == bot_id:
            correct_answer = quiz.get('answer', '').lower()

            if comment_text_lower == correct_answer:
                reward = quiz.get('reward', 0)
                quiz['solved'] = True
                config.save()

                register_user(message.from_user, bot_id)
                db.add_balance(user_id, bot_id, reward)

                try:
                    await message.reply(
                        f"✅ Правильный ответ, {user_name}!\n"
                        f"+{reward} {cfg.currency_emoji}"
                    )
                except Exception as e:
                    logger.error(f"Ошибка ответа на викторину: {e}")

                logger.info(f"Викторина {original_msg_id} решена пользователем {user_id}, награда: {reward}")

                if original_msg_id in config.active_quizzes:
                    del config.active_quizzes[original_msg_id]
                    config.save()

        # ===== ПРОВЕРКА АУКЦИОНА =====
        auction = config.active_auctions.get(original_msg_id)
        if auction and auction.get('bot_id') == bot_id:
            bet_match = BET_PATTERN.search(message.text)

            if bet_match:
                bet_amount = int(bet_match.group(1))

                register_user(message.from_user, bot_id)
                user_balance = db.get_balance(user_id, bot_id)

                if user_balance != float('inf') and user_balance < bet_amount:
                    try:
                        await message.reply(
                            f"{user_name}, у вас недостаточно средств.\n"
                            f"Ваш баланс: {user_balance:.0f} {cfg.currency_emoji}"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка ответа аукциона: {e}")
                    return

                current_bid = auction.get('current_bid', 0)
                if bet_amount <= current_bid:
                    try:
                        await message.reply(
                            f"{user_name}, ставка должна быть больше текущей: "
                            f"{current_bid} {cfg.currency_emoji}"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка ответа аукциона: {e}")
                    return

                auction['current_bidder'] = user_id
                auction['current_bid'] = bet_amount
                auction['last_bid_time'] = datetime.now().isoformat()
                config.save()

                try:
                    await message.reply(
                        f"✅ {user_name} ставит {bet_amount} {cfg.currency_emoji}!"
                    )
                except Exception as e:
                    logger.error(f"Ошибка подтверждения ставки: {e}")

                logger.info(f"Аукцион {original_msg_id}: ставка {bet_amount} от {user_id}")

                old_task = config.auction_tasks.get(original_msg_id)
                if old_task is None or old_task.done():
                    task = asyncio.create_task(
                        run_auction_timer(bot_instance, bot_id, original_msg_id)
                    )
                    config.auction_tasks[original_msg_id] = task

    dp.include_router(router)


# ====================== ПОДКЛЮЧЕНИЕ БОТОВ ======================

def create_connection_handlers(bot_instance: Bot, dp: Dispatcher):
    """Обработчики подключения новых ботов (только главный бот)."""
    router = Router()

    @router.callback_query(F.data == "connect_bot")
    async def connect_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("🤖 Подключение бота\n\nОтправьте ссылку на ваш канал:", reply_markup=build_cancel_keyboard())
        await state.set_state(ConnectBotStates.WaitingChannelUrl)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingChannelUrl)
    async def connect_channel(message: types.Message, state: FSMContext):
        request_id = str(uuid.uuid4())[:8]
        request = PendingBotRequest(request_id=request_id, user_id=message.from_user.id, channel_url=message.text.strip())
        config.pending_requests[request_id] = request
        config.save()

        user = db.get_user(message.from_user.id)
        approve_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"request_approve_{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"request_reject_{request_id}")
        ]])
        try:
            await bot_instance.send_message(
                MAIN_ADMIN_ID,
                f"📝 Заявка на подключение бота\nОт: @{user['username'] if user else '?'}\nКанал: {message.text}",
                reply_markup=approve_kb
            )
        except Exception:
            pass
        await message.answer("✅ Заявка отправлена!", reply_markup=build_main_menu("main"))
        await state.clear()

    @router.callback_query(F.data.startswith("request_approve_"))
    async def request_approve(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[16:]
        req = config.pending_requests.get(rid)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Да, уверен", callback_data=f"request_confirm_{rid}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"request_back_{rid}")
        ]])
        await callback.message.edit_text("Вы уверены?", reply_markup=confirm_kb)
        await callback.answer()

    @router.callback_query(F.data.startswith("request_back_"))
    async def request_back(callback: types.CallbackQuery):
        rid = callback.data[13:]
        req = config.pending_requests.get(rid)
        if req:
            user = db.get_user(req.user_id)
            approve_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Одобрить", callback_data=f"request_approve_{rid}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"request_reject_{rid}")
            ]])
            await callback.message.edit_text(
                f"📝 Заявка\nОт: @{user['username'] if user else '?'}\nКанал: {req.channel_url}",
                reply_markup=approve_kb
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("request_confirm_"))
    async def request_confirm(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[16:]
        req = config.pending_requests.get(rid)
        if not req:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
        req.status = "token_wait"
        config.waiting_for_token[req.user_id] = rid
        config.save()
        try:
            await bot_instance.send_message(req.user_id, "✅ Заявка одобрена!\n\nОтправьте токен бота от @BotFather:")
            await callback.message.edit_text("⏳ Ожидаем токен от пользователя...")
        except Exception as e:
            await callback.message.edit_text(f"❌ Ошибка: {e}")
        await callback.answer()

    @router.callback_query(F.data.startswith("request_reject_"))
    async def request_reject(callback: types.CallbackQuery):
        if callback.from_user.id != MAIN_ADMIN_ID:
            await callback.answer("Нет доступа", show_alert=True)
            return
        rid = callback.data[15:]
        req = config.pending_requests.get(rid)
        if req:
            try:
                await bot_instance.send_message(req.user_id, "❌ Ваша заявка отклонена.")
            except Exception:
                pass
            del config.pending_requests[rid]
            config.save()
        await callback.message.edit_text("❌ Заявка отклонена.")
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
            await message.answer(f"✅ Бот найден: @{info.username}\n\nКак будет называться валюта?")
            await state.set_state(ConnectBotStates.WaitingCurrencyName)
            try:
                await bot_instance.send_message(MAIN_ADMIN_ID, f"✅ Токен получен: @{info.username}")
            except Exception:
                pass
        except Exception as e:
            await message.answer(f"❌ Неверный токен: {e}\nПопробуйте ещё раз:")

    @router.message(ConnectBotStates.WaitingCurrencyName)
    async def connect_currency_name(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if req:
            req.currency_name = message.text.strip()
            config.save()
        await message.answer("Отправьте эмодзи для валюты (💎, 🪙, ⭐):")
        await state.set_state(ConnectBotStates.WaitingCurrencyEmoji)

    @router.message(ConnectBotStates.WaitingCurrencyEmoji)
    async def connect_currency_emoji(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if req:
            req.currency_emoji = message.text.strip()
            config.save()
        await message.answer("Выберите функции бота:", reply_markup=build_modules_keyboard())
        await state.set_state(ConnectBotStates.WaitingModules)

    @router.callback_query(ConnectBotStates.WaitingModules, F.data.startswith("module_"))
    async def connect_select_modules(callback: types.CallbackQuery, state: FSMContext):
        module_type = callback.data[7:]
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            await callback.answer("Ошибка", show_alert=True)
            return
        if module_type == "takes":
            req.modules = ["takes"]
            config.save()
            await callback.message.edit_text("Отправьте ссылку/username канала для тейков (@channel):")
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        elif module_type == "shop":
            req.modules = ["shop"]
            config.save()
            await callback.message.edit_text("Отправьте ссылку/username группы для магазина (@group):")
            await state.set_state(ConnectBotStates.WaitingShopChannel)
        else:
            req.modules = ["takes", "shop"]
            config.save()
            await callback.message.edit_text("Сначала канал для тейков (@channel):")
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingTakesChannel)
    async def connect_takes_channel(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            return
        channel = message.text.strip()
        if not channel.startswith("@"):
            channel = "@" + channel.replace("https://t.me/", "")
        req.takes_channel = channel
        config.save()
        if "shop" in req.modules:
            await message.answer("Теперь группа для магазина (@group):")
            await state.set_state(ConnectBotStates.WaitingShopChannel)
        else:
            await finalize_bot_setup(message, state, req, bot_instance)

    @router.message(ConnectBotStates.WaitingShopChannel)
    async def connect_shop_channel(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req = config.pending_requests.get(data.get('request_id'))
        if not req:
            return
        channel = message.text.strip()
        if not channel.startswith("@"):
            channel = "@" + channel.replace("https://t.me/", "")
        req.shop_channel = channel
        config.save()
        await finalize_bot_setup(message, state, req, bot_instance)

    async def finalize_bot_setup(message, state, req, main_bot):
        """Завершение настройки и запуск подключённого бота."""
        await message.answer("⏳ Запускаю бота...")
        try:
            new_bot_id = f"bot_{req.user_id}_{int(datetime.now().timestamp())}"
            new_config = BotConfig(
                bot_id=new_bot_id, token=req.token,
                currency_name=req.currency_name, currency_emoji=req.currency_emoji,
                channel_url=req.channel_url, takes_channel=req.takes_channel,
                shop_channel=req.shop_channel, modules=req.modules,
                owner_id=req.user_id, base_exchange_rate=0.5, take_cooldown_minutes=3
            )
            config.bots[new_bot_id] = new_config

            register_user(message.from_user, new_bot_id)
            db.set_balance(req.user_id, new_bot_id, float('inf'))
            db.set_bot_data(req.user_id, new_bot_id, is_owner=1, is_admin=1, activated_at=datetime.now().isoformat())

            config.exchange_rates.rates[new_bot_id] = 0.5
            if req.request_id in config.pending_requests:
                del config.pending_requests[req.request_id]
            config.save()

            new_bot = Bot(token=req.token)
            new_dp = Dispatcher(storage=MemoryStorage())
            create_bot_handlers(new_bot_id, new_bot, new_dp)
            config.active_bots[new_bot_id] = new_bot
            config.active_dispatchers[new_bot_id] = new_dp
            asyncio.create_task(new_dp.start_polling(new_bot))

            modules_text = ", ".join(req.modules)
            await message.answer(
                f"✅ Бот успешно подключён!\n\n"
                f"💰 Валюта: {req.currency_name} {req.currency_emoji}\n"
                f"📦 Модули: {modules_text}\n\n"
                f"Вы — владелец с бесконечным балансом.\nПерейдите в бота и нажмите /start",
                reply_markup=build_main_menu("main")
            )
            try:
                await main_bot.send_message(MAIN_ADMIN_ID, f"✅ Бот {new_bot_id} успешно запущен!")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")
            await message.answer(f"❌ Ошибка: {e}", reply_markup=build_main_menu("main"))
        await state.clear()

    dp.include_router(router)


# ====================== ГЛАВНАЯ ФУНКЦИЯ ======================

async def main():
    """Точка входа: подключение БД, загрузка конфигурации, запуск ботов."""
    logger.info("Подключение к базе данных...")
    db.connect()
    logger.info("Загрузка конфигурации...")
    config.load()

    # Главный бот
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    main_dp = Dispatcher(storage=MemoryStorage())
    create_bot_handlers("main", main_bot, main_dp)
    create_connection_handlers(main_bot, main_dp)
    config.active_bots["main"] = main_bot
    config.active_dispatchers["main"] = main_dp

    # Запуск подключённых ботов
    for bot_id, bot_cfg in config.bots.items():
        if bot_id != "main":
            try:
                connected_bot = Bot(token=bot_cfg.token)
                connected_dp = Dispatcher(storage=MemoryStorage())
                create_bot_handlers(bot_id, connected_bot, connected_dp)
                config.active_bots[bot_id] = connected_bot
                config.active_dispatchers[bot_id] = connected_dp
                asyncio.create_task(connected_dp.start_polling(connected_bot))
                logger.info(f"Запущен подключённый бот: {bot_id}")
            except Exception as e:
                logger.error(f"Ошибка запуска бота {bot_id}: {e}")

    # Восстановление задач удаления пиара
    now = datetime.now()
    to_remove = []

    for deletion_id, info in config.scheduled_deletions.items():
        try:
            delete_at = datetime.fromisoformat(info['delete_at'])
            target_bot = config.active_bots.get(info['bot_id'])

            if not target_bot:
                logger.warning(f"Бот {info['bot_id']} не найден для задачи {deletion_id}")
                to_remove.append(deletion_id)
                continue

            if delete_at <= now:
                logger.info(f"Просроченная задача {deletion_id}, удаляем сейчас")
                try:
                    if info.get('is_pinned'):
                        await target_bot.unpin_chat_message(info['channel'], info['message_id'])
                    await target_bot.delete_message(info['channel'], info['message_id'])
                except Exception as e:
                    logger.error(f"Ошибка удаления просроченного: {e}")
                to_remove.append(deletion_id)
            else:
                remaining_hours = (delete_at - now).total_seconds() / 3600
                logger.info(f"Восстановлена задача {deletion_id}, осталось {remaining_hours:.1f}ч")
                asyncio.create_task(delayed_delete_message(
                    target_bot, info['channel'], info['message_id'],
                    remaining_hours, info.get('is_pinned', False), deletion_id
                ))
        except Exception as e:
            logger.error(f"Ошибка восстановления задачи {deletion_id}: {e}")
            to_remove.append(deletion_id)

    for deletion_id in to_remove:
        if deletion_id in config.scheduled_deletions:
            del config.scheduled_deletions[deletion_id]
    if to_remove:
        config.save()

    logger.info(f"Активных задач удаления: {len(config.scheduled_deletions)}")
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
