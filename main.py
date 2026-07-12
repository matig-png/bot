import asyncio
import logging
import re
import json
import os
import uuid
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

# Supabase
from supabase import create_client, Client

from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable

class RegistrationMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user: Optional[User] = data.get("event_from_user")
        
        if user and not user.is_bot:
            # Получаем bot_id из конфига или контекста (в данном случае "main" или по токену)
            # Для простоты берем bot_id из логики инициализации
            bot = data.get("bot")
            # Находим bot_id по токену в конфиге
            bot_id = "main" # По умолчанию
            for bid, cfg in config.bots.items():
                if cfg.token == bot.token:
                    bot_id = bid
                    break
            
            # РЕГИСТРАЦИЯ
            register_user(user, bot_id)
            
        return await handler(event, data)

# ====================== ЛОГИРОВАНИЕ ======================

sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ====================== КОНФИГУРАЦИЯ ======================

from dotenv import load_dotenv
import os

# Загружаем переменные из .env
load_dotenv()

# ====================== КОНФИГУРАЦИЯ ======================

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
MAIN_ADMIN_ID = int(os.getenv("MAIN_ADMIN_ID"))
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(",")))
CONFIG_FILE = "bot_config.json"
MAIN_ANNOUNCEMENT_CHANNEL = os.getenv("MAIN_ANNOUNCEMENT_CHANNEL")

# Supabase конфигурация
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

MAX_TAKES = int(os.getenv("MAX_TAKES", "3"))
TAKE_COOLDOWN_MINUTES = int(os.getenv("TAKE_COOLDOWN_MINUTES", "3"))

# Проверка обязательных переменных
if not MAIN_BOT_TOKEN or not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Не заданы обязательные переменные окружения! Проверь .env файл")

# Паттерны для аукциона
BET_PATTERN = re.compile(r'(?:ставлю|ставка)\s+(\d+)', re.IGNORECASE)
PASS_PATTERN = re.compile(r'^(?:пас|лив)$', re.IGNORECASE)
MIN_BID_INCREMENT = 5

TG_LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/)?([a-zA-Z0-9_]+)',
    re.IGNORECASE
)

# ====================== НОВОЕ: Буфер для медиагрупп ======================
media_group_buffer: Dict[str, Dict[str, Any]] = {}
MEDIA_GROUP_TIMEOUT = 1.0  # секунды ожидания завершения альбома


# ====================== БАЗА ДАННЫХ SUPABASE ======================

class Database:
    """Supabase база данных. Данные пользователей сохраняются между обновлениями бота."""

    def __init__(self):
        self.supabase: Optional[Client] = None

    def connect(self):
        """Подключение к Supabase."""
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info(f"✅ Supabase подключена: {SUPABASE_URL}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Supabase: {e}")
            raise

    # --- Пользователи ---

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получить данные пользователя."""
        try:
            response = self.supabase.table('users').select('*').eq('user_id', user_id).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователя {user_id}: {e}")
            return None

    def create_or_update_user(self, user_id: int, username: str, name: str):
        """Создать или обновить пользователя."""
        now = datetime.now().isoformat()
        try:
            existing = self.get_user(user_id)
            if existing:
                self.supabase.table('users').update({
                    'username': username,
                    'name': name
                }).eq('user_id', user_id).execute()
                logger.info(f"♻️ Обновлён пользователь {user_id} (@{username})")
            else:
                self.supabase.table('users').insert({
                    'user_id': user_id,
                    'username': username,
                    'name': name,
                    'created_at': now
                }).execute()
                logger.info(f"✅ Создан новый пользователь {user_id} (@{username})")
        except Exception as e:
            logger.error(f"❌ Ошибка создания/обновления пользователя {user_id}: {e}")

    # --- Балансы ---

    def get_balance(self, user_id: int, bot_id: str) -> float:
        """Получить баланс пользователя."""
        try:
            response = self.supabase.table('balances').select('*').eq('user_id', user_id).eq('bot_id', bot_id).execute()
            if not response.data:
                return 0
            row = response.data[0]
            return float('inf') if row['is_infinite'] else row['balance']
        except Exception as e:
            logger.error(f"❌ Ошибка получения баланса {user_id}/{bot_id}: {e}")
            return 0

    def set_balance(self, user_id: int, bot_id: str, balance: float):
        """Установить баланс."""
        is_inf = balance == float('inf')
        val = 0 if is_inf else balance
        try:
            existing = self.supabase.table('balances').select('*').eq('user_id', user_id).eq('bot_id', bot_id).execute()
            if existing.data:
                self.supabase.table('balances').update({
                    'balance': val,
                    'is_infinite': is_inf
                }).eq('user_id', user_id).eq('bot_id', bot_id).execute()
            else:
                self.supabase.table('balances').insert({
                    'user_id': user_id,
                    'bot_id': bot_id,
                    'balance': val,
                    'is_infinite': is_inf
                }).execute()
            logger.info(f"💰 Установлен баланс {user_id}/{bot_id}: {balance}")
        except Exception as e:
            logger.error(f"❌ Ошибка установки баланса {user_id}/{bot_id}: {e}")

    def add_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        """Добавить к балансу."""
        current = self.get_balance(user_id, bot_id)
        if current == float('inf'):
            return True
        self.set_balance(user_id, bot_id, current + amount)
        return True

    def deduct_balance(self, user_id: int, bot_id: str, amount: float) -> bool:
        """Списать с баланса."""
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
        try:
            response = self.supabase.table('user_bot_data').select('*').eq('user_id', user_id).eq('bot_id', bot_id).execute()
            if response.data:
                logger.debug(f"✅ Получены bot_data для {user_id}/{bot_id}")
                return response.data[0]
            
            # ВАЖНО: Возвращаем пустой словарь с пустым activated_at,
            # чтобы register_user понял, что это новый пользователь
            logger.debug(f"⚠️ bot_data не найдены для {user_id}/{bot_id}, возврат стандартных данных")
            return {
                'user_id': user_id, 
                'bot_id': bot_id,
                'quiz_passed': False, 
                'show_in_top': True,
                'is_blocked': False, 
                'is_frozen': False,
                'is_moderator': False, 
                'is_admin': False, 
                'is_owner': False,
                'activated_at': '',
                'last_promo_at': '',
                'is_announcement_mod': False,
                'is_announcement_blocked': False
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения bot_data {user_id}/{bot_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'user_id': user_id, 
                'bot_id': bot_id,
                'quiz_passed': False, 
                'show_in_top': True,
                'is_blocked': False, 
                'is_frozen': False,
                'is_moderator': False, 
                'is_admin': False, 
                'is_owner': False,
                'activated_at': '',
                'last_promo_at': '',
                'is_announcement_mod': False,
                'is_announcement_blocked': False
            }

    def set_bot_data(self, user_id: int, bot_id: str, **kwargs):
        """Обновить данные пользователя для бота."""
        try:
            existing = self.supabase.table('user_bot_data').select('*').eq('user_id', user_id).eq('bot_id', bot_id).execute()
            
            data = {
                'user_id': user_id,
                'bot_id': bot_id,
                'quiz_passed': kwargs.get('quiz_passed', False),
                'show_in_top': kwargs.get('show_in_top', True),
                'is_blocked': kwargs.get('is_blocked', False),
                'is_frozen': kwargs.get('is_frozen', False),
                'is_moderator': kwargs.get('is_moderator', False),
                'is_admin': kwargs.get('is_admin', False),
                'is_owner': kwargs.get('is_owner', False),
                'activated_at': kwargs.get('activated_at', ''),
                'last_promo_at': kwargs.get('last_promo_at', ''),
                'is_announcement_mod': kwargs.get('is_announcement_mod', False),
                'is_announcement_blocked': kwargs.get('is_announcement_blocked', False)
            }
            
            if existing.data:
                # ИСПРАВЛЕНИЕ: Обновляем только переданные поля
                update_data = {}
                for key in data.keys():
                    if key not in ['user_id', 'bot_id']:
                        if key in kwargs:  # Обновляем только явно переданные значения
                            update_data[key] = kwargs[key]
                
                if update_data:  # Если есть что обновлять
                    self.supabase.table('user_bot_data').update(update_data).eq('user_id', user_id).eq('bot_id', bot_id).execute()
                    logger.info(f"♻️ Обновлены bot_data для {user_id}/{bot_id}: {list(update_data.keys())}")
                else:
                    logger.debug(f"⚠️ Нечего обновлять для {user_id}/{bot_id}")
            else:
                # ИСПРАВЛЕНИЕ: При создании используем все поля из data
                self.supabase.table('user_bot_data').insert(data).execute()
                logger.info(f"✅ Созданы bot_data для {user_id}/{bot_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка установки bot_data {user_id}/{bot_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())  # Полный traceback для отладки

    # --- Тейки ---

    def get_take_timestamps(self, user_id: int, bot_id: str, since: datetime) -> List[str]:
        """Получить все временные метки тейков начиная с указанного времени."""
        try:
            response = self.supabase.table('take_timestamps').select('timestamp').eq('user_id', user_id).eq('bot_id', bot_id).gt('timestamp', since.isoformat()).order('timestamp', desc=True).execute()
            return [row['timestamp'] for row in response.data]
        except Exception as e:
            logger.error(f"❌ Ошибка получения timestamps {user_id}/{bot_id}: {e}")
            return []

    def add_take_timestamp(self, user_id: int, bot_id: str):
        """Записать время отправки тейка."""
        try:
            now = datetime.now().isoformat()
            self.supabase.table('take_timestamps').insert({
                'user_id': user_id,
                'bot_id': bot_id,
                'timestamp': now
            }).execute()
            
            # Очистка старых записей (оставляем последние 20)
            all_records = self.supabase.table('take_timestamps').select('id').eq('user_id', user_id).eq('bot_id', bot_id).order('id', desc=True).execute()
            if len(all_records.data) > 20:
                ids_to_keep = [r['id'] for r in all_records.data[:20]]
                self.supabase.table('take_timestamps').delete().eq('user_id', user_id).eq('bot_id', bot_id).not_.in_('id', ids_to_keep).execute()
        except Exception as e:
            logger.error(f"❌ Ошибка добавления timestamp {user_id}/{bot_id}: {e}")

    # --- Списки ---

    def get_all_users_for_bot(self, bot_id: str) -> List[Dict]:
        """Все пользователи с балансами в конкретном боте."""
        try:
            response = self.supabase.table('balances').select('user_id, balance, is_infinite').eq('bot_id', bot_id).execute()
            result = []
            for row in response.data:
                user = self.get_user(row['user_id'])
                bot_data = self.get_bot_data(row['user_id'], bot_id)
                if user:
                    result.append({
                        'user_id': row['user_id'],
                        'username': user.get('username', ''),
                        'name': user.get('name', ''),
                        'balance': row['balance'],
                        'is_infinite': row['is_infinite'],
                        'show_in_top': bot_data.get('show_in_top', True),
                        'is_owner': bot_data.get('is_owner', False)
                    })
            return result
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователей для бота {bot_id}: {e}")
            return []

    def find_user_by_input(self, input_str: str) -> Optional[int]:
        """Найти пользователя по username, имени или ID."""
        input_str = input_str.strip().lstrip('@').lower()
        try:
            uid = int(input_str)
            if self.get_user(uid):
                return uid
        except ValueError:
            pass
        
        try:
            response = self.supabase.table('users').select('user_id').or_(f'username.ilike.%{input_str}%,name.ilike.%{input_str}%').execute()
            if response.data:
                return response.data[0]['user_id']
        except Exception as e:
            logger.error(f"❌ Ошибка поиска пользователя {input_str}: {e}")
        
        return None


db = Database()

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
    announcement_channel: str = ""
    modules: List[str] = field(default_factory=lambda: ["takes", "shop"])
    take_cooldown_minutes: int = 3
    max_takes: int = 3
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
                    if 'announcement_channel' not in bot_data:
                        bot_data['announcement_channel'] = ""
                    if 'max_takes' not in bot_data:
                        bot_data['max_takes'] = 3
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

                logger.info(f"✅ Конфигурация загружена: {len(self.bots)} ботов")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")

        if "main" not in self.bots:
            self.bots["main"] = BotConfig(
                bot_id="main",
                token=MAIN_BOT_TOKEN,
                currency_name="луны",
                currency_emoji="🌗",
                channel_url="https://t.me/Wings_of_fire_CF",
                takes_channel="@Wings_of_fire_CF",
                shop_channel="@wingsoffiremagazine",
                announcement_channel=MAIN_ANNOUNCEMENT_CHANNEL,
                modules=["takes", "shop"],
                take_cooldown_minutes=TAKE_COOLDOWN_MINUTES,
                max_takes=MAX_TAKES,
                owner_id=MAIN_ADMIN_ID,
                base_exchange_rate=1.0
            )
            self.exchange_rates.rates["main"] = 1.0
            logger.info(f"✅ Главный бот создан")
            self.save()
        else:
            needs_save = False
            if not self.bots["main"].announcement_channel:
                self.bots["main"].announcement_channel = MAIN_ANNOUNCEMENT_CHANNEL
                logger.info(f"♻️ Обновлён канал объявлений: {MAIN_ANNOUNCEMENT_CHANNEL}")
                needs_save = True
            if not hasattr(self.bots["main"], 'max_takes'):
                self.bots["main"].max_takes = MAX_TAKES
                needs_save = True
            if needs_save:
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
            logger.error(f"❌ Ошибка сохранения конфигурации: {e}")


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
    WaitingAnnouncementModUsername = State()
    WaitingRemoveAnnouncementModUsername = State()


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
    _PFX + r'ху[йяеёюи]\w*',
    _PFX + r'пизд\w*',
    _PFX_EB + r'[её]б\w*',
    r'бля[дт]\w*', r'сук[аиуе]\w*', r'суч[каеьи]\w*',
    r'муда[кч]\w*', r'мудил\w*', r'мудозвон\w*',
    r'пидор\w*', r'пидар\w*', r'пидр\w*', r'педик\w*', r'педераст\w*',
    r'шлюх\w*', r'гандон\w*', r'залуп\w*', r'дроч\w*', r'манд[аоуеёяи]\w*',
    r'[её]бл[ао]\w*', r'[её]бну\w*', r'[её]бан\w*',
    r'хер[а-яё]*\w*', r'жоп[аеуы]\w*', r'срать?\w*', r'сран\w*',
    r'говн[оа]\w*', r'засранец\w*', r'засранк[аи]\w*',
]


def build_profanity_regex(bot_id: str) -> re.Pattern:
    """Строит регулярное выражение для цензуры."""
    bot_cfg = config.bots.get(bot_id)
    patterns = BASE_PROFANITY_PATTERNS.copy()
    if bot_cfg and bot_cfg.censored_words:
        for word in bot_cfg.censored_words:
            patterns.append(re.escape(word) + r'\w*')
    return re.compile(r'\b(?:' + '|'.join(patterns) + r')\b', re.IGNORECASE | re.UNICODE)


def censor_profanity(text: str, bot_id: str) -> Tuple[str, bool]:
    """Заменяет мат на спойлеры."""
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
    """Проверяет наличие маркерных слов."""
    if not text:
        return False
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg or not bot_cfg.marker_words:
        return False
    text_lower = text.lower()
    return any(word.lower() in text_lower for word in bot_cfg.marker_words)


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

    # ВСЕГДА создаём/обновляем пользователя
    db.create_or_update_user(uid, username, name)
    
    # Проверяем, нужна ли инициализация (новый пользователь или нет данных)
    existing = db.get_bot_data(uid, bot_id)
    
    # ИСПРАВЛЕНИЕ: Проверяем activated_at И что это не пустая строка
    if not existing.get('activated_at') or existing.get('activated_at') == '':
        bot_cfg = config.bots.get(bot_id)
        is_owner_flag = bot_cfg and bot_cfg.owner_id == uid
        is_admin_flag = bot_id == "main" and uid in ADMIN_IDS
        is_main_owner = bot_id == "main" and uid == MAIN_ADMIN_ID

        # Устанавливаем баланс ТОЛЬКО если его нет или он равен 0
        current_balance = db.get_balance(uid, bot_id)
        
        # Устанавливаем баланс только для новых пользователей
        if current_balance == 0 and not (is_owner_flag or is_main_owner):
            if is_owner_flag or is_main_owner:
                db.set_balance(uid, bot_id, float('inf'))
            elif is_admin_flag:
                initial_balance = bot_cfg.admin_starting_balance if bot_cfg else 100
                db.set_balance(uid, bot_id, initial_balance)
            else:
                db.set_balance(uid, bot_id, 0)
        elif is_owner_flag or is_main_owner:
            # Владельцы всегда должны иметь бесконечный баланс
            db.set_balance(uid, bot_id, float('inf'))

        # Устанавливаем роли и флаги
        db.set_bot_data(uid, bot_id,
            quiz_passed=existing.get('quiz_passed', False),  # Сохраняем существующие значения
            show_in_top=existing.get('show_in_top', True), 
            is_blocked=existing.get('is_blocked', False), 
            is_frozen=existing.get('is_frozen', False),
            is_moderator=existing.get('is_moderator', False), 
            is_announcement_mod=existing.get('is_announcement_mod', False), 
            is_announcement_blocked=existing.get('is_announcement_blocked', False),
            is_admin=(is_admin_flag or is_owner_flag or is_main_owner),
            is_owner=(is_owner_flag or is_main_owner),
            activated_at=datetime.now().isoformat(),
            last_promo_at=existing.get('last_promo_at', '')  # Сохраняем последний пиар
        )
        logger.info(f"🎉 Зарегистрирован новый пользователь {uid} (@{username}) в боте {bot_id}")
    else:
        # НОВОЕ: Для существующих пользователей только обновляем username/name
        logger.debug(f"♻️ Пользователь {uid} (@{username}) уже зарегистрирован в боте {bot_id}")


def check_admin(uid: int, bot_id: str) -> bool:
    """Является ли пользователь админом."""
    # Приоритет: переменные окружения для главного бота
    if bot_id == "main" and uid in ADMIN_IDS:
        return True
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_admin') or data.get('is_owner'))


def check_owner(uid: int, bot_id: str) -> bool:
    """Является ли пользователь владельцем."""
    if bot_id == "main" and uid == MAIN_ADMIN_ID:
        return True
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_owner'))


def check_moderator(uid: int, bot_id: str) -> bool:
    """Является ли пользователь модератором тейков (или выше)."""
    # Админы и владельцы автоматически модераторы
    if check_admin(uid, bot_id) or check_owner(uid, bot_id):
        return True
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_moderator'))


def check_announcement_moderator(uid: int, bot_id: str) -> bool:
    """Является ли пользователь модератором объявлений."""
    if check_admin(uid, bot_id) or check_owner(uid, bot_id):
        return True
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_announcement_mod'))
    

def check_announcement_blocked(uid: int, bot_id: str) -> bool:
    """Заблокирован ли пользователь для объявлений (независимо от тейков)."""
    data = db.get_bot_data(uid, bot_id)
    return bool(data.get('is_announcement_blocked', False))


def can_send_take(uid: int, bot_id: str) -> Tuple[bool, int, str]:
    """
    Проверка лимита тейков.
    У пользователя max_takes тейков, каждый восстанавливается через take_cooldown_minutes минут.
    Возвращает: (можно_ли, осталось_тейков, сообщение).
    """
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return False, 0, "Ошибка конфигурации"

    max_takes = bot_cfg.max_takes
    cooldown_minutes = bot_cfg.take_cooldown_minutes
    now = datetime.now()

    # Получаем тейки за последние max_takes * cooldown_minutes минут
    since = now - timedelta(minutes=max_takes * cooldown_minutes)
    recent_timestamps = db.get_take_timestamps(uid, bot_id, since)

    # Считаем сколько тейков ещё занимают слоты (не восстановились)
    used_slots = 0
    for ts_str in recent_timestamps:
        try:
            ts = datetime.fromisoformat(ts_str)
            minutes_ago = (now - ts).total_seconds() / 60
            if minutes_ago < cooldown_minutes:
                used_slots += 1
        except Exception:
            pass

    remaining = max_takes - used_slots

    if remaining > 0:
        return True, remaining, f"Тейков: {remaining}/{max_takes}"

    # Находим когда восстановится следующий тейк
    if recent_timestamps:
        sorted_ts = sorted(recent_timestamps)
        for ts_str in sorted_ts:
            try:
                ts = datetime.fromisoformat(ts_str)
                next_available = ts + timedelta(minutes=cooldown_minutes)
                if next_available > now:
                    remaining_time = next_available - now
                    mins = int(remaining_time.total_seconds() // 60)
                    secs = int(remaining_time.total_seconds() % 60)
                    return False, 0, f"Нет тейков. Следующий через {mins}м {secs}с (0/{max_takes})"
            except Exception:
                pass

    return True, 1, f"Тейков: 1/{max_takes}"


def sync_env_admins_to_db():
    """Синхронизирует админов из .env с базой данных при запуске."""
    if not ADMIN_IDS:
        logger.warning("⚠️ ADMIN_IDS не заданы в .env")
        return
        
    for uid in ADMIN_IDS:
        db.create_or_update_user(uid, f"admin_{uid}", "Администратор")
        db.set_balance(uid, "main", float('inf'))
        db.set_bot_data(uid, "main",
            is_admin=True,
            is_owner=(uid == MAIN_ADMIN_ID),
            is_moderator=True,
            is_announcement_mod=True,
            activated_at=datetime.now().isoformat()
        )
    logger.info(f"✅ Админы из .env синхронизированы с БД: {ADMIN_IDS}")


def can_use_promo(uid: int, bot_id: str) -> Tuple[bool, str]:
    """Проверка: 3 дня с активации + 12ч с последнего пиара."""
    data = db.get_bot_data(uid, bot_id)
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


def get_user_display_name(user_id: int) -> str:
    """Получить отображаемое имя пользователя (username в приоритете)."""
    user = db.get_user(user_id)
    if not user:
        return "Неизвестный"
    username = user.get('username', '')
    if username and not username.startswith('user'):
        return f"@{username}"
    return user.get('name', 'Неизвестный')

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
        builder.row(
            InlineKeyboardButton(text="🛒 Магазин", callback_data="shop"),
            InlineKeyboardButton(text="📢 Выложить объявление", callback_data="post_announcement")
        )

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

        if bot_cfg and "shop" in bot_cfg.modules:
            builder.row(
                InlineKeyboardButton(text="📢 Модер объявлений", callback_data="adm_announcement_mods")
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
    """Меню модераторов тейков."""
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


def build_announcement_mods_menu() -> InlineKeyboardMarkup:
    """Меню модераторов объявлений."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➕ Назначить", callback_data="ann_mod_assign"),
        InlineKeyboardButton(text="➖ Снять", callback_data="ann_mod_remove")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Список", callback_data="ann_mod_list"),
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
    """
    Клавиатура модерации тейка — пользователь НЕ заблокирован в тейках.
    Показывает кнопку 'Заблокировать (тейки)'.
    """
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Отправить", callback_data=f"take_approve_{take_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"take_reject_{take_id}")
    )
    builder.row(
        InlineKeyboardButton(text="🚫 Заблокировать (тейки)", callback_data=f"user_block_{uid}")
    )
    return builder.as_markup()


def build_take_moderation_keyboard_blocked(take_id: str, uid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура модерации тейка — пользователь уже заблокирован в тейках.
    Показывает кнопку 'Разблокировать (тейки)'.
    """
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Отправить", callback_data=f"take_approve_{take_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"take_reject_{take_id}")
    )
    builder.row(
        InlineKeyboardButton(text="🔓 Разблокировать (тейки)", callback_data=f"take_unblock_{uid}")
    )
    return builder.as_markup()


def build_published_take_keyboard(channel_msg_ids: List[int], uid: int, is_blocked: bool) -> InlineKeyboardMarkup:
    """
    Клавиатура для уже опубликованных тейков — пользователь НЕ заблокирован.
    Кнопка 'Удалить' вместо 'Отклонить'.
    """
    builder = InlineKeyboardBuilder()
    # ИЗМЕНЕНИЕ: Сохраняем список ID через запятую
    msg_ids_str = ",".join(map(str, channel_msg_ids))
    builder.row(
        InlineKeyboardButton(text="🗑 Удалить из канала", callback_data=f"take_delete_{msg_ids_str}")
    )
    builder.row(
        InlineKeyboardButton(text="🚫 Заблокировать (тейки)", callback_data=f"user_block_{uid}")
    )
    return builder.as_markup()


def build_published_take_keyboard_blocked(channel_msg_ids: List[int], uid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для уже опубликованных тейков — пользователь уже заблокирован.
    Показывает кнопку 'Разблокировать (тейки)'.
    """
    builder = InlineKeyboardBuilder()
    # ИЗМЕНЕНИЕ: Сохраняем список ID через запятую
    msg_ids_str = ",".join(map(str, channel_msg_ids))
    builder.row(
        InlineKeyboardButton(text="🗑 Удалить из канала", callback_data=f"take_delete_{msg_ids_str}")
    )
    builder.row(
        InlineKeyboardButton(text="🔓 Разблокировать (тейки)", callback_data=f"take_unblock_{uid}")
    )
    return builder.as_markup()


def build_announcement_moderation_keyboard(ann_id: str, uid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура модерации объявления — пользователь НЕ заблокирован в объявлениях.
    Блокировка объявлений независима от блокировки тейков.
    """
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"ann_approve_{ann_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ann_reject_{ann_id}")
    )
    builder.row(
        InlineKeyboardButton(text="🚫 Заблокировать (объявления)", callback_data=f"ann_block_{uid}_{ann_id}")
    )
    return builder.as_markup()


def build_announcement_moderation_keyboard_blocked(ann_id: str, uid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура модерации объявления — пользователь уже заблокирован в объявлениях.
    Показывает кнопку 'Разблокировать (объявления)'.
    """
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"ann_approve_{ann_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ann_reject_{ann_id}")
    )
    builder.row(
        InlineKeyboardButton(text="🔓 Разблокировать (объявления)", callback_data=f"ann_unblock_{uid}_{ann_id}")
    )
    return builder.as_markup()


def build_promo_confirm_keyboard() -> InlineKeyboardMarkup:
    """Подтверждение пиара."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Оплатить", callback_data="promo_pay"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    ]])


def build_quiz_keyboard(question_num: int) -> InlineKeyboardMarkup:
    """Варианты ответов викторины."""
    builder = InlineKeyboardBuilder()
    for i, answer in enumerate(BUILT_IN_QUIZ[question_num]["answers"]):
        builder.row(InlineKeyboardButton(text=answer, callback_data=f"quiz_{question_num}_{i}"))
    return builder.as_markup()

# ====================== НОВОЕ: ОБРАБОТКА МЕДИАГРУПП ======================

async def forward_take_to_channel(message: types.Message, bot_id: str, bot_instance: Bot) -> Optional[types.Message]:
    """
    Пересылает тейк в канал с цензурой мата.
    Сохраняет спойлеры на медиа и всё форматирование текста.
    Для главного бота добавляет подпись после #тейк.
    """
    try:
        bot_cfg = config.bots.get(bot_id)
        if not bot_cfg or not bot_cfg.takes_channel:
            return None

        text = message.text or message.caption or ""
        entities = message.entities or message.caption_entities

        # Для главного бота добавляем подпись после #тейк
        if bot_id == "main":
            import re as re_module
            pattern = re_module.compile(r'(#тейк)', re_module.IGNORECASE)
            if pattern.search(text):
                text = pattern.sub(r'\1\n★@Wings_teyk_bot ; @Wings_of_fire_CF★', text, count=1)

        censored, has_profanity = censor_profanity(text, bot_id)

        # Проверяем наличие спойлера на медиа
        has_media_spoiler = getattr(message, 'has_media_spoiler', False)

        send_kwargs = {
            "caption": censored if has_profanity else text,
            "parse_mode": "HTML" if has_profanity else None,
            "caption_entities": entities if not has_profanity else None
        }

        if message.photo:
            return await bot_instance.send_photo(
                bot_cfg.takes_channel,
                photo=message.photo[-1].file_id,
                has_spoiler=has_media_spoiler,
                **send_kwargs
            )
        elif message.video:
            return await bot_instance.send_video(
                bot_cfg.takes_channel,
                video=message.video.file_id,
                has_spoiler=has_media_spoiler,
                **send_kwargs
            )
        elif message.animation:
            return await bot_instance.send_animation(
                bot_cfg.takes_channel,
                animation=message.animation.file_id,
                has_spoiler=has_media_spoiler,
                **send_kwargs
            )
        elif message.document:
            return await bot_instance.send_document(
                bot_cfg.takes_channel,
                document=message.document.file_id,
                **send_kwargs
            )
        elif message.voice:
            return await bot_instance.send_voice(
                bot_cfg.takes_channel,
                voice=message.voice.file_id,
                **send_kwargs
            )
        elif message.audio:
            return await bot_instance.send_audio(
                bot_cfg.takes_channel,
                audio=message.audio.file_id,
                **send_kwargs
            )
        elif message.sticker:
            return await bot_instance.send_sticker(
                bot_cfg.takes_channel,
                sticker=message.sticker.file_id
            )
        else:
            return await bot_instance.send_message(
                bot_cfg.takes_channel,
                censored if has_profanity else text,
                parse_mode="HTML" if has_profanity else None,
                entities=entities if not has_profanity else None
            )
    except Exception as e:
        logger.error(f"Ошибка пересылки тейка: {e}")
        return None

# ====================== ОТЛОЖЕННОЕ УДАЛЕНИЕ ПИАРА ======================

async def delayed_delete_message(bot_instance: Bot, channel: str, message_id: int,
                                  hours: float, is_pinned: bool, deletion_id: str):
    """Удаляет сообщение пиара через указанное время."""
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
    except asyncio.CancelledError:
        logger.info(f"Задача удаления {deletion_id} отменена")
    except Exception as e:
        logger.error(f"Ошибка отложенного удаления: {e}")

# ====================== АУКЦИОН ======================

async def run_auction_timer(bot_instance: Bot, bot_id: str, auction_id: str):
    """
    Таймер аукциона.
    Отсчёт начинается ТОЛЬКО после первой ставки.
    Отсчёт пишется В КОММЕНТАРИЯХ под постом.
    Победитель объявляется ПОСТОМ В КАНАЛЕ.
    """
    bot_cfg = config.bots.get(bot_id)
    if not bot_cfg:
        return

    try:
        # Ждём первой ставки — без ставок не начинаем отсчёт
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
            message_id = auction['message_id']
            last_bid_time = datetime.fromisoformat(auction['last_bid_time'])
            snapshot_time = last_bid_time

            # Ждём discussion_message_id до 15 секунд
            if discussion_id and not auction.get('discussion_message_id'):
                for _ in range(30):
                    await asyncio.sleep(0.5)
                    auction = config.active_auctions.get(auction_id)
                    if not auction or auction.get('finished'):
                        return
                    if auction.get('discussion_message_id'):
                        logger.info(f"Аукцион {auction_id}: discussion_message_id получен")
                        break

            async def send_countdown(text: str):
                """Отправка цифры отсчёта как комментарий под постом."""
                current_auction = config.active_auctions.get(auction_id)
                current_disc_msg_id = current_auction.get('discussion_message_id') if current_auction else None
                current_disc_id = current_auction.get('discussion_chat_id') if current_auction else discussion_id

                # Приоритет 1: reply на пост в группе комментариев
                if current_disc_id and current_disc_msg_id:
                    try:
                        await bot_instance.send_message(
                            chat_id=current_disc_id,
                            text=text,
                            reply_to_message_id=current_disc_msg_id
                        )
                        return
                    except Exception as e:
                        logger.error(f"Ошибка reply в группу: {e}")

                # Приоритет 2: просто в группу без reply
                if current_disc_id:
                    try:
                        await bot_instance.send_message(chat_id=current_disc_id, text=text)
                        return
                    except Exception as e:
                        logger.error(f"Ошибка в группу: {e}")

                # Приоритет 3: в канал с reply
                try:
                    await bot_instance.send_message(
                        chat_id=channel_id, text=text, reply_to_message_id=message_id
                    )
                except Exception as e:
                    logger.error(f"Ошибка отсчёта {text}: {e}")

            # Ждём 2 минуты с последней ставки
            wait_2min = last_bid_time + timedelta(minutes=2)
            now = datetime.now()
            if now < wait_2min:
                await asyncio.sleep((wait_2min - now).total_seconds())

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            discussion_message_id = auction.get('discussion_message_id')
            current_last = datetime.fromisoformat(auction['last_bid_time'])
            if current_last > snapshot_time:
                logger.info(f"Аукцион {auction_id}: новая ставка, сброс")
                continue

            # Пишем "3" в комментариях
            await send_countdown("3")
            logger.info(f"Аукцион {auction_id}: отсчёт 3")
            await asyncio.sleep(30)

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            # Пишем "2" в комментариях
            await send_countdown("2")
            logger.info(f"Аукцион {auction_id}: отсчёт 2")
            await asyncio.sleep(30)

            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            # Пишем "1" в комментариях
            await send_countdown("1")
            logger.info(f"Аукцион {auction_id}: отсчёт 1")
            await asyncio.sleep(30)

            # Финальная проверка
            auction = config.active_auctions.get(auction_id)
            if not auction or auction.get('finished'):
                return
            if datetime.fromisoformat(auction['last_bid_time']) > snapshot_time:
                continue

            # === ОБЪЯВЛЕНИЕ ПОБЕДИТЕЛЯ ПОСТОМ В КАНАЛЕ ===
            winner_id = auction.get('current_bidder')
            winner_amount = auction.get('current_bid', 0)
            auction['finished'] = True
            config.save()

            if winner_id:
                winner_display = get_user_display_name(winner_id)
                winner_balance = db.get_balance(winner_id, bot_id)

                if winner_balance != float('inf') and winner_balance < winner_amount:
                    try:
                        target = discussion_id if discussion_id else channel_id
                        await bot_instance.send_message(
                            chat_id=target,
                            text=(
                                f"⚠️ У {winner_display} недостаточно средств "
                                f"({winner_amount} {bot_cfg.currency_emoji}). "
                                f"Аукцион отменён."
                            )
                        )
                    except Exception as e:
                        logger.error(f"Ошибка сообщения о нехватке: {e}")
                else:
                    db.deduct_balance(winner_id, bot_id, winner_amount)
                    try:
                        await bot_instance.send_message(
                            chat_id=bot_cfg.takes_channel,
                            text=f"Победитель: {winner_display}"
                        )
                        logger.info(f"Аукцион {auction_id}: победитель {winner_display}")
                    except Exception as e:
                        logger.error(f"Ошибка объявления победителя: {e}")
                    try:
                        await bot_instance.send_message(
                            winner_id,
                            f"🏆 Вы выиграли аукцион!\n"
                            f"💰 Списано: {winner_amount} {bot_cfg.currency_emoji}"
                        )
                    except Exception:
                        pass

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
    # ИСПРАВЛЕНИЕ: Создаём НОВЫЙ роутер для КАЖДОГО бота
    router = Router(name=f"bot_{bot_id}_handlers")
    bot_config = config.bots.get(bot_id)

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

    @router.callback_query(F.data == "balance")
    async def callback_balance(callback: types.CallbackQuery):
        cfg = config.bots.get(bot_id)
        user_data = db.get_bot_data(callback.from_user.id, bot_id)

        status_text = ""
        if user_data.get('is_frozen'):
            status_text += "❄️ Счёт заморожен\n"
        if user_data.get('is_blocked'):
            status_text += "🚫 Заблокирован для тейков\n"
        if user_data.get('is_announcement_blocked'):
            status_text += "🚫 Заблокирован для объявлений\n"

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

        can_take, remaining_takes, cooldown_msg = can_send_take(callback.from_user.id, bot_id)
        text += f"\n📝 {cooldown_msg}"

        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

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
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
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
            "Введите сумму перевода.\nМожете добавить сообщение на новой строке:\n\nПример:\n100\nСпасибо!",
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

    @router.callback_query(F.data == "rates")
    async def callback_rates(callback: types.CallbackQuery):
        text = "Базовый курс 2 к 1 рублю, ниже курс валют относительно базы\n\n📊 Курсы валют:\n\n"
        for bid, cfg in config.bots.items():
            rate = get_exchange_rate(bid)
            text += f"{cfg.currency_name} {cfg.currency_emoji}: {rate:.2f}\n"
        if config.exchange_rates.rates_locked:
            text += "\n🔒 Курсы зафиксированы"
        await callback.message.edit_text(text, reply_markup=build_main_menu(bot_id))
        await callback.answer()

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
            f"Ваш баланс: {balance:.0f} {source_cfg.currency_emoji}\n\nВведите сумму:",
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
                f"✅ Конвертировано:\n{amount:.0f} {source_cfg.currency_emoji} → {converted:.0f} {target_cfg.currency_emoji}",
                reply_markup=build_main_menu(bot_id)
            )
        else:
            await message.answer(f"❌ {error}", reply_markup=build_main_menu(bot_id))
        await state.clear()

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
                f"✅ Правильно! +{cfg.quiz_reward} {cfg.currency_emoji}\n\nВопрос 2:\n{BUILT_IN_QUIZ[2]['question']}",
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
                f"✅ Правильно! +{cfg.quiz_reward} {cfg.currency_emoji}\n\nВопрос 3:\n{BUILT_IN_QUIZ[3]['question']}",
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
            db.set_bot_data(callback.from_user.id, bot_id, quiz_passed=True)
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

    # =================== ОБЪЯВЛЕНИЯ (С ПОДДЕРЖКОЙ МЕДИАГРУПП) ===================

    @router.callback_query(F.data == "post_announcement")
    async def callback_post_announcement(callback: types.CallbackQuery, state: FSMContext):
        """Кнопка выложить объявление."""
        cfg = config.bots.get(bot_id)
        logger.info(f"Объявление: bot_id={bot_id}, channel='{cfg.announcement_channel if cfg else 'None'}'")
        if not cfg or not cfg.announcement_channel:
            await callback.answer("Канал для объявлений не настроен.", show_alert=True)
            return
        await callback.message.edit_text(
            "📢 Отправьте ваше объявление (можно несколько фото/видео)",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AnnouncementStates.WaitingAnnouncement)
        await callback.answer()

    # =================== НОВОЕ: Обработчик медиагрупп для объявлений ===================
    @router.message(AnnouncementStates.WaitingAnnouncement, F.media_group_id)
    async def process_announcement_media_group(message: types.Message, state: FSMContext):
        """Обработка медиагруппы объявления."""
        group_id = f"ann_{message.media_group_id}"
        
        if group_id not in media_group_buffer:
            media_group_buffer[group_id] = {
                'messages': [],
                'user_id': message.from_user.id,
                'bot_id': bot_id,
                'is_announcement': True,
                'state': state
            }
            # Запускаем таймер обработки
            asyncio.create_task(process_announcement_media_group_complete(group_id, bot_instance, state))
        
        media_group_buffer[group_id]['messages'].append(message)

    async def process_announcement_media_group_complete(group_id: str, bot: Bot, state: FSMContext):
        """Завершение обработки медиагруппы объявления."""
        await asyncio.sleep(MEDIA_GROUP_TIMEOUT)
        
        if group_id not in media_group_buffer:
            return
        
        group_data = media_group_buffer[group_id]
        messages = group_data['messages']
        user_id = group_data['user_id']
        
        cfg = config.bots.get(bot_id)
        if not cfg or not cfg.announcement_channel:
            del media_group_buffer[group_id]
            return
        
        # Проверяем блокировку для объявлений
        if check_announcement_blocked(user_id, bot_id):
            try:
                await bot.send_message(
                    user_id,
                    "🚫 Вы заблокированы для отправки объявлений.",
                    reply_markup=build_main_menu(bot_id)
                )
            except Exception:
                pass
            del media_group_buffer[group_id]
            await state.clear()
            return
        
        # Собираем модераторов объявлений
        all_users = db.get_all_users_for_bot(bot_id)
        announcement_mods = []
        for user in all_users:
            ud = db.get_bot_data(user['user_id'], bot_id)
            if ud.get('is_announcement_mod') and not check_admin(user['user_id'], bot_id):
                announcement_mods.append(user['user_id'])
        # Добавляем администраторов
        for user in all_users:
            if check_admin(user['user_id'], bot_id) and user['user_id'] not in announcement_mods:
                announcement_mods.append(user['user_id'])
        
        if announcement_mods:
            # Есть модераторы — отправляем на проверку
            ann_id = str(uuid.uuid4())[:8]
            ann_key = f"ann_{ann_id}"
            
            # Сохраняем все сообщения медиагруппы
            config.pending_takes[ann_key] = {
                'user_id': user_id,
                'bot_id': bot_id,
                'type': 'announcement_media_group',
                'media_group': [
                    {
                        'chat_id': msg.chat.id,
                        'message_id': msg.message_id,
                        'photo': msg.photo[-1].file_id if msg.photo else None,
                        'video': msg.video.file_id if msg.video else None,
                        'caption': msg.caption if hasattr(msg, 'caption') else None,
                        'caption_entities': serialize_entities(msg.caption_entities if hasattr(msg, 'caption_entities') else None),
                        'has_spoiler': getattr(msg, 'has_media_spoiler', False)
                    }
                    for msg in messages
                ]
            }
            config.save()
            
            is_ann_blocked = check_announcement_blocked(user_id, bot_id)
            
            for mod_uid in announcement_mods:
                try:
                    if is_ann_blocked:
                        mod_kb = build_announcement_moderation_keyboard_blocked(ann_id, user_id)
                    else:
                        mod_kb = build_announcement_moderation_keyboard(ann_id, user_id)
                    await bot.send_message(
                        mod_uid,
                        f"📢 Новое объявление (альбом: {len(messages)} медиа) на проверке",
                        reply_markup=mod_kb
                    )
                    # Пересылаем все медиа модератору
                    for msg in messages:
                        await msg.copy_to(mod_uid)
                except Exception as e:
                    logger.error(f"Ошибка отправки альбома модератору объявлений {mod_uid}: {e}")
            
            try:
                await bot.send_message(
                    user_id,
                    "📝 Объявление отправлено на модерацию.",
                    reply_markup=build_main_menu(bot_id)
                )
            except Exception:
                pass
        else:
            # Нет модераторов — публикуем сразу медиагруппу
            try:
                from aiogram.types import InputMediaPhoto, InputMediaVideo
                
                media_group = []
                for idx, msg in enumerate(messages):
                    text = msg.caption or "" if idx == 0 else ""
                    entities = msg.caption_entities if (idx == 0 and hasattr(msg, 'caption_entities')) else None
                    has_spoiler = getattr(msg, 'has_media_spoiler', False)
                    
                    if msg.photo:
                        media_group.append(InputMediaPhoto(
                            media=msg.photo[-1].file_id,
                            caption=text,
                            caption_entities=entities,
                            has_spoiler=has_spoiler
                        ))
                    elif msg.video:
                        media_group.append(InputMediaVideo(
                            media=msg.video.file_id,
                            caption=text,
                            caption_entities=entities,
                            has_spoiler=has_spoiler
                        ))
                
                await bot.send_media_group(cfg.announcement_channel, media_group)
                await bot.send_message(
                    user_id,
                    "✅ Ваше объявление опубликовано!",
                    reply_markup=build_main_menu(bot_id)
                )
                logger.info(f"Объявление-альбом от {user_id} в {cfg.announcement_channel}")
            except Exception as e:
                logger.error(f"Ошибка отправки объявления-альбома: {e}")
                try:
                    await bot.send_message(
                        user_id,
                        f"❌ Ошибка при отправке объявления: {e}",
                        reply_markup=build_main_menu(bot_id)
                    )
                except Exception:
                    pass
        
        del media_group_buffer[group_id]
        await state.clear()

    @router.message(AnnouncementStates.WaitingAnnouncement)
    async def process_announcement(message: types.Message, state: FSMContext):
        """
        Обработка одиночного объявления (не альбома).
        """
        cfg = config.bots.get(bot_id)
        if not cfg or not cfg.announcement_channel:
            await message.answer("Канал не настроен.", reply_markup=build_main_menu(bot_id))
            await state.clear()
            return

        # Проверяем блокировку именно для объявлений
        if check_announcement_blocked(message.from_user.id, bot_id):
            await message.answer(
                "🚫 Вы заблокированы для отправки объявлений.",
                reply_markup=build_main_menu(bot_id)
            )
            await state.clear()
            return

        # Собираем модераторов объявлений
        all_users = db.get_all_users_for_bot(bot_id)
        announcement_mods = []
        for user in all_users:
            ud = db.get_bot_data(user['user_id'], bot_id)
            if ud.get('is_announcement_mod') and not check_admin(user['user_id'], bot_id):
                announcement_mods.append(user['user_id'])
        # Добавляем администраторов
        for user in all_users:
            if check_admin(user['user_id'], bot_id) and user['user_id'] not in announcement_mods:
                announcement_mods.append(user['user_id'])

        if announcement_mods:
            # Есть модераторы — отправляем на проверку
            ann_id = str(uuid.uuid4())[:8]
            ann_key = f"ann_{ann_id}"
            config.pending_takes[ann_key] = {
                'user_id': message.from_user.id,
                'bot_id': bot_id,
                'chat_id': message.chat.id,
                'message_id': message.message_id,
                'type': 'announcement'
            }
            config.save()

            is_ann_blocked = check_announcement_blocked(message.from_user.id, bot_id)

            for mod_uid in announcement_mods:
                try:
                    if is_ann_blocked:
                        mod_kb = build_announcement_moderation_keyboard_blocked(ann_id, message.from_user.id)
                    else:
                        mod_kb = build_announcement_moderation_keyboard(ann_id, message.from_user.id)
                    await bot_instance.send_message(
                        mod_uid,
                        f"📢 Новое объявление на проверке",
                        reply_markup=mod_kb
                    )
                    await message.copy_to(mod_uid)
                except Exception as e:
                    logger.error(f"Ошибка отправки модератору объявлений {mod_uid}: {e}")

            await message.answer("📝 Объявление отправлено на модерацию.", reply_markup=build_main_menu(bot_id))
        else:
            # Нет модераторов — публикуем сразу через copy_message
            try:
                await bot_instance.copy_message(
                    chat_id=cfg.announcement_channel,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id
                )
                await message.answer("✅ Ваше объявление опубликовано!", reply_markup=build_main_menu(bot_id))
                logger.info(f"Объявление от {message.from_user.id} в {cfg.announcement_channel}")
            except Exception as e:
                logger.error(f"Ошибка отправки объявления: {e}")
                await message.answer(
                    f"❌ Ошибка при отправке объявления: {e}\n"
                    f"Убедитесь что бот является администратором канала.",
                    reply_markup=build_main_menu(bot_id)
                )

        await state.clear()

    @router.callback_query(F.data.startswith("ann_approve_"))
    async def announcement_approve(callback: types.CallbackQuery):
        """Одобрение объявления модератором."""
        ann_id = callback.data[12:]
        ann_key = f"ann_{ann_id}"
        ann_data = config.pending_takes.get(ann_key)
        if not ann_data:
            await callback.answer("Объявление не найдено", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        try:
            # Проверяем тип объявления
            if ann_data.get('type') == 'announcement_media_group':
                # Это медиагруппа
                from aiogram.types import InputMediaPhoto, InputMediaVideo
                
                media_group = []
                for idx, media_info in enumerate(ann_data['media_group']):
                    text = media_info.get('caption', '') if idx == 0 else ""
                    entities = restore_entities(media_info.get('caption_entities')) if idx == 0 else None
                    has_spoiler = media_info.get('has_spoiler', False)
                    
                    if media_info.get('photo'):
                        media_group.append(InputMediaPhoto(
                            media=media_info['photo'],
                            caption=text,
                            caption_entities=entities,
                            has_spoiler=has_spoiler
                        ))
                    elif media_info.get('video'):
                        media_group.append(InputMediaVideo(
                            media=media_info['video'],
                            caption=text,
                            caption_entities=entities,
                            has_spoiler=has_spoiler
                        ))
                
                await bot_instance.send_media_group(cfg.announcement_channel, media_group)
                logger.info(f"Объявление-альбом одобрено: {len(media_group)} медиа")
            else:
                # Одиночное объявление
                await bot_instance.copy_message(
                    chat_id=cfg.announcement_channel,
                    from_chat_id=ann_data['chat_id'],
                    message_id=ann_data['message_id']
                )
            
            del config.pending_takes[ann_key]
            config.save()
            await callback.message.edit_text("✅ Объявление одобрено и опубликовано.")
            try:
                await bot_instance.send_message(ann_data['user_id'], "✅ Ваше объявление одобрено и опубликовано!")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Ошибка одобрения объявления: {e}")
            await callback.answer(f"Ошибка: {e}", show_alert=True)
        await callback.answer()

    @router.callback_query(F.data.startswith("ann_reject_"))
    async def announcement_reject(callback: types.CallbackQuery):
        """Отклонение объявления модератором."""
        ann_id = callback.data[11:]
        ann_key = f"ann_{ann_id}"
        ann_data = config.pending_takes.get(ann_key)
        if ann_data:
            try:
                await bot_instance.send_message(ann_data['user_id'], "❌ Ваше объявление отклонено модератором.")
            except Exception:
                pass
            del config.pending_takes[ann_key]
            config.save()
        await callback.message.edit_text("❌ Объявление отклонено.")
        await callback.answer()

    @router.callback_query(F.data.startswith("ann_block_"))
    async def announcement_block_user(callback: types.CallbackQuery):
        """Блокировка пользователя для объявлений."""
        if not check_announcement_moderator(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        parts = callback.data[10:].split("_", 1)
        uid = int(parts[0])
        ann_id = parts[1] if len(parts) > 1 else ""
        db.set_bot_data(uid, bot_id, is_announcement_blocked=True)
        try:
            await bot_instance.send_message(uid, "🚫 Вы заблокированы для отправки объявлений.")
        except Exception:
            pass
        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_announcement_moderation_keyboard_blocked(ann_id, uid)
            )
        except Exception:
            pass
        await callback.answer("🚫 Заблокирован для объявлений", show_alert=True)

    @router.callback_query(F.data.startswith("ann_unblock_"))
    async def announcement_unblock_user(callback: types.CallbackQuery):
        """Разблокировка пользователя для объявлений."""
        if not check_announcement_moderator(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        parts = callback.data[12:].split("_", 1)
        uid = int(parts[0])
        ann_id = parts[1] if len(parts) > 1 else ""
        db.set_bot_data(uid, bot_id, is_announcement_blocked=False)
        try:
            await bot_instance.send_message(uid, "✅ Вы разблокированы для объявлений.")
        except Exception:
            pass
        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_announcement_moderation_keyboard(ann_id, uid)
            )
        except Exception:
            pass
        await callback.answer("✅ Разблокирован для объявлений", show_alert=True)


    # =================== ТЕЙКИ С ПОДДЕРЖКОЙ МЕДИАГРУПП ===================

    if bot_config and "takes" in bot_config.modules:

        # =================== НОВОЕ: Обработчик медиагрупп для тейков ===================
        async def handle_take_media_group(message: types.Message, bid: str, bot: Bot, state: FSMContext):
            """Обработка медиагруппы тейка."""
            group_id = f"take_{message.media_group_id}"
            
            if group_id not in media_group_buffer:
                media_group_buffer[group_id] = {
                    'messages': [],
                    'user_id': message.from_user.id,
                    'bot_id': bid,
                    'is_take': True,
                    'state': state
                }
                # Запускаем таймер обработки
                asyncio.create_task(process_take_media_group_complete(group_id, bot))
            
            media_group_buffer[group_id]['messages'].append(message)

        async def process_take_media_group_complete(group_id: str, bot: Bot):
            """Завершение обработки медиагруппы тейка."""
            await asyncio.sleep(MEDIA_GROUP_TIMEOUT)
            
            if group_id not in media_group_buffer:
                return
            
            group_data = media_group_buffer[group_id]
            messages = group_data['messages']
            user_id = group_data['user_id']
            bid = group_data['bot_id']
            
            cfg = config.bots.get(bid)
            if not cfg:
                del media_group_buffer[group_id]
                return
            
            user_data = db.get_bot_data(user_id, bid)
            
            # Проверка блокировки тейков
            if user_data.get('is_blocked'):
                try:
                    await bot.send_message(user_id, "🚫 Вы заблокированы для отправки тейков.")
                except Exception:
                    pass
                del media_group_buffer[group_id]
                return
            
            can_take, remaining_takes, cooldown_msg = can_send_take(user_id, bid)
            if not can_take:
                try:
                    await bot.send_message(user_id, f"⏳ {cooldown_msg}")
                except Exception:
                    pass
                del media_group_buffer[group_id]
                return
            
            # Сортируем по ID сообщения
            messages.sort(key=lambda m: m.message_id)
            first_msg = messages[0]
            text = first_msg.text or first_msg.caption or ""
            
            # Проверяем наличие #тейк
            if "#тейк" not in text.lower():
                try:
                    await bot.send_message(user_id, "⚠️ Добавьте #тейк в сообщение!")
                except Exception:
                    pass
                del media_group_buffer[group_id]
                return
            
            if cfg.takes_paused:
                # Сохраняем в очередь паузы
                take_data = {
                    'user_id': user_id, 'bot_id': bid,
                    'media_group': [
                        {
                            'photo': msg.photo[-1].file_id if msg.photo else None,
                            'video': msg.video.file_id if msg.video else None,
                            'caption': msg.caption if hasattr(msg, 'caption') else None,
                            'caption_entities': serialize_entities(msg.caption_entities if hasattr(msg, 'caption_entities') else None),
                            'has_spoiler': getattr(msg, 'has_media_spoiler', False)
                        }
                        for msg in messages
                    ],
                    'timestamp': datetime.now().isoformat()
                }
                if bid not in config.paused_takes:
                    config.paused_takes[bid] = []
                config.paused_takes[bid].append(take_data)
                config.save()
                db.add_take_timestamp(user_id, bid)
                try:
                    await bot.send_message(
                        user_id,
                        "⏸ Тейки сейчас на паузе. Ваш тейк будет отправлен когда тейки включат.",
                        reply_markup=build_main_menu(bid)
                    )
                except Exception:
                    pass
                del media_group_buffer[group_id]
                return
            
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
                # Отправка на модерацию
                take_id = str(uuid.uuid4())[:8]
                config.pending_takes[take_id] = {
                    'user_id': user_id, 'bot_id': bid,
                    'type': 'take_media_group',
                    'media_group': [
                        {
                            'photo': msg.photo[-1].file_id if msg.photo else None,
                            'video': msg.video.file_id if msg.video else None,
                            'caption': msg.caption if hasattr(msg, 'caption') else None,
                            'caption_entities': serialize_entities(msg.caption_entities if hasattr(msg, 'caption_entities') else None),
                            'has_spoiler': getattr(msg, 'has_media_spoiler', False)
                        }
                        for msg in messages
                    ]
                }
                config.save()
                
                is_blocked_flag = bool(db.get_bot_data(user_id, bid).get('is_blocked', False))
                all_users = db.get_all_users_for_bot(bid)
                for user in all_users:
                    mod_uid = user['user_id']
                    if check_moderator(mod_uid, bid):
                        try:
                            if is_blocked_flag:
                                mod_kb = build_take_moderation_keyboard_blocked(take_id, user_id)
                            else:
                                mod_kb = build_take_moderation_keyboard(take_id, user_id, False)
                            await bot.send_message(
                                mod_uid,
                                f"⚠️ Тейк-альбом ({len(messages)} медиа) на модерации\nПричина: {moderation_reason}",
                                reply_markup=mod_kb
                            )
                            # Пересылаем все медиа
                            for msg in messages:
                                await msg.copy_to(mod_uid)
                        except Exception as e:
                            logger.error(f"Ошибка отправки модератору {mod_uid}: {e}")
                
                try:
                    await bot.send_message(
                        user_id,
                        "📝 Тейк отправлен на модерацию.",
                        reply_markup=build_main_menu(bid)
                    )
                except Exception:
                    pass
            else:
                # Публикуем напрямую медиагруппу
                try:
                    from aiogram.types import InputMediaPhoto, InputMediaVideo
                    
                    media_group = []
                    for idx, msg in enumerate(messages):
                        text_caption = msg.caption or "" if idx == 0 else ""
                        entities = msg.caption_entities if (idx == 0 and hasattr(msg, 'caption_entities')) else None
                        
                        # Для главного бота добавляем подпись
                        if bid == "main" and idx == 0 and text_caption:
                            import re as re_module
                            pattern = re_module.compile(r'(#тейк)', re_module.IGNORECASE)
                            if pattern.search(text_caption):
                                text_caption = pattern.sub(r'\1\n★@Wings_teyk_bot ; @Wings_of_fire_CF★', text_caption, count=1)
                        
                        censored, has_prof = censor_profanity(text_caption, bid)
                        has_spoiler = getattr(msg, 'has_media_spoiler', False)
                        
                        if msg.photo:
                            media_group.append(InputMediaPhoto(
                                media=msg.photo[-1].file_id,
                                caption=censored if (idx == 0 and has_prof) else (text_caption if idx == 0 else ""),
                                parse_mode="HTML" if (idx == 0 and has_prof) else None,
                                caption_entities=entities if (idx == 0 and not has_prof) else None,
                                has_spoiler=has_spoiler
                            ))
                        elif msg.video:
                            media_group.append(InputMediaVideo(
                                media=msg.video.file_id,
                                caption=censored if (idx == 0 and has_prof) else (text_caption if idx == 0 else ""),
                                parse_mode="HTML" if (idx == 0 and has_prof) else None,
                                caption_entities=entities if (idx == 0 and not has_prof) else None,
                                has_spoiler=has_spoiler
                            ))
                    
                    sent_messages = await bot.send_media_group(cfg.takes_channel, media_group)
                    db.add_take_timestamp(user_id, bid)
                    _, new_remaining, new_msg = can_send_take(user_id, bid)
                    
                    try:
                        await bot.send_message(
                            user_id,
                            f"✅ Тейк отправлен в канал!\n📝 {new_msg}",
                            reply_markup=build_main_menu(bid)
                        )
                    except Exception:
                        pass
                    
                    # НОВОЕ: Отправляем админам/модераторам с кнопкой "Удалить"
                    if sent_messages:
                        channel_msg_ids = [msg.message_id for msg in sent_messages]
                        is_blocked_flag = bool(db.get_bot_data(user_id, bid).get('is_blocked', False))
                        
                        all_users = db.get_all_users_for_bot(bid)
                        for user in all_users:
                            mod_uid = user['user_id']
                            if check_moderator(mod_uid, bid):
                                try:
                                    # ИЗМЕНЕНИЕ: Отправляем альбом целиком модератору
                                    from aiogram.types import InputMediaPhoto, InputMediaVideo
                                    
                                    mod_media_group = []
                                    for idx, msg in enumerate(messages):
                                        text_caption = msg.caption or "" if idx == 0 else ""
                                        entities = msg.caption_entities if (idx == 0 and hasattr(msg, 'caption_entities')) else None
                                        has_spoiler = getattr(msg, 'has_media_spoiler', False)
                                        
                                        if msg.photo:
                                            mod_media_group.append(InputMediaPhoto(
                                                media=msg.photo[-1].file_id,
                                                caption=text_caption,
                                                caption_entities=entities,
                                                has_spoiler=has_spoiler
                                            ))
                                        elif msg.video:
                                            mod_media_group.append(InputMediaVideo(
                                                media=msg.video.file_id,
                                                caption=text_caption,
                                                caption_entities=entities,
                                                has_spoiler=has_spoiler
                                            ))
                                    
                                    # Отправляем альбом модератору
                                    await bot.send_media_group(mod_uid, mod_media_group)
                                    
                                    # ЗАТЕМ отправляем кнопки управления
                                    if is_blocked_flag:
                                        published_kb = build_published_take_keyboard_blocked(channel_msg_ids, user_id)
                                    else:
                                        published_kb = build_published_take_keyboard(channel_msg_ids, user_id, False)
                                    
                                    await bot.send_message(
                                        mod_uid,
                                        f"📝 Тейк-альбом опубликован в канале ({len(messages)} медиа)",
                                        reply_markup=published_kb
                                    )
                                except Exception as e:
                                    logger.error(f"Ошибка отправки модератору {mod_uid}: {e}")
                        
                        logger.info(f"Тейк-альбом от {user_id} опубликован: {len(messages)} медиа")
                
                except Exception as e:
                    logger.error(f"Ошибка публикации тейка-альбома: {e}")
                    try:
                        await bot.send_message(user_id, "❌ Ошибка при отправке тейка.")
                    except Exception:
                        pass
            
            del media_group_buffer[group_id]

        async def process_take_message(message: types.Message, bid: str, bot: Bot):
            """Общая логика обработки одиночного тейка."""
            uid = message.from_user.id
            register_user(message.from_user, bid)
            cfg = config.bots.get(bid)
            user_data = db.get_bot_data(uid, bid)

            # Блокировка тейков независима от блокировки объявлений
            if user_data.get('is_blocked'):
                await message.answer("🚫 Вы заблокированы для отправки тейков.")
                return False

            can_take, remaining_takes, cooldown_msg = can_send_take(uid, bid)
            if not can_take:
                await message.answer(f"⏳ {cooldown_msg}")
                return False

            text = message.text or message.caption or ""

            if cfg.takes_paused:
                take_data = {
                    'user_id': uid, 'bot_id': bid, 'text': text,
                    'photo': message.photo[-1].file_id if message.photo else None,
                    'video': message.video.file_id if message.video else None,
                    'animation': message.animation.file_id if message.animation else None,
                    'document': message.document.file_id if message.document else None,
                    'caption': message.caption,
                    'caption_entities': serialize_entities(message.caption_entities if hasattr(message, 'caption_entities') else None),
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
                    'caption': message.caption,
                    'caption_entities': serialize_entities(message.caption_entities if hasattr(message, 'caption_entities') else None)
                }
                config.save()

                is_blocked_flag = bool(db.get_bot_data(uid, bid).get('is_blocked', False))
                all_users = db.get_all_users_for_bot(bid)
                for user in all_users:
                    mod_uid = user['user_id']
                    if check_moderator(mod_uid, bid):
                        try:
                            if is_blocked_flag:
                                mod_kb = build_take_moderation_keyboard_blocked(take_id, uid)
                            else:
                                mod_kb = build_take_moderation_keyboard(take_id, uid, False)
                            await bot.send_message(
                                mod_uid,
                                f"⚠️ Тейк на модерации\nПричина: {moderation_reason}\n\n{text}",
                                reply_markup=mod_kb
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
                    _, new_remaining, new_msg = can_send_take(uid, bid)
                    await message.answer(
                        f"✅ Тейк отправлен в канал!\n📝 {new_msg}",
                        reply_markup=build_main_menu(bid)
                    )

                    # НОВОЕ: Отправляем админам/модераторам с кнопкой "Удалить"
                    channel_msg_ids = [sent.message_id]
                    is_blocked_flag = bool(db.get_bot_data(uid, bid).get('is_blocked', False))

                    all_users = db.get_all_users_for_bot(bid)
                    for user in all_users:
                        mod_uid = user['user_id']
                        if check_moderator(mod_uid, bid):
                            try:
                                if is_blocked_flag:
                                    published_kb = build_published_take_keyboard_blocked(channel_msg_ids, uid)
                                else:
                                    published_kb = build_published_take_keyboard(channel_msg_ids, uid, False)
                                
                                await bot.send_message(
                                    mod_uid,
                                    f"📝 Тейк опубликован в канале",
                                    reply_markup=published_kb
                                )
                                await message.copy_to(mod_uid)
                            except Exception as e:
                                logger.error(f"Ошибка отправки модератору {mod_uid}: {e}")

                    return True
                else:
                    await message.answer("❌ Ошибка при отправке тейка.", reply_markup=build_main_menu(bid))
                    return False

        @router.callback_query(F.data == "send_take")
        async def callback_send_take(callback: types.CallbackQuery, state: FSMContext):
            user_data = db.get_bot_data(callback.from_user.id, bot_id)
            if user_data.get('is_blocked'):
                await callback.answer("🚫 Вы заблокированы для тейков", show_alert=True)
                return
            can_take, remaining_takes, cooldown_msg = can_send_take(callback.from_user.id, bot_id)
            if not can_take:
                await callback.answer(f"⏳ {cooldown_msg}", show_alert=True)
                return
            cfg = config.bots.get(bot_id)
            pause_text = " ⏸ (на паузе — будет отправлен позже)" if cfg.takes_paused else ""
            await callback.message.edit_text(
                f"📝 Отправьте тейк с хештегом #тейк{pause_text}\n"
                f"💡 Можно отправить несколько фото/видео как альбом\n"
                f"⏱ {cooldown_msg}",
                reply_markup=build_cancel_keyboard()
            )
            await state.set_state(TakeStates.WaitingTake)
            await callback.answer()

        @router.message(TakeStates.WaitingTake, F.media_group_id)
        async def process_take_from_button_media_group(message: types.Message, state: FSMContext):
            """Обработка медиагруппы тейка из состояния."""
            group_id = f"take_{message.media_group_id}"
            text = message.text or message.caption or ""
            
            # Проверяем #тейк только у первого сообщения, остальные просто добавляем в буфер
            if group_id not in media_group_buffer and "#тейк" not in text.lower():
                await message.answer("⚠️ Добавьте #тейк в сообщение!", reply_markup=build_cancel_keyboard())
                return
            await handle_take_media_group(message, bot_id, bot_instance, state)

        @router.message(TakeStates.WaitingTake)
        async def process_take_from_button(message: types.Message, state: FSMContext):
            """Обработка одиночного тейка из состояния."""
            text = message.text or message.caption or ""
            if "#тейк" not in text.lower():
                await message.answer("⚠️ Добавьте #тейк в сообщение!", reply_markup=build_cancel_keyboard())
                return
            await process_take_message(message, bot_id, bot_instance)
            await state.clear()

        @router.message(F.media_group_id)
        async def auto_forward_take_media_group(message: types.Message, state: FSMContext):
            """Автоматическая обработка медиагруппы (альбома)."""
            if message.chat.type in ("channel", "group", "supergroup"):
                return
            current_state = await state.get_state()
            if current_state == TakeStates.WaitingTake:
                return
            
            group_id = f"take_{message.media_group_id}"
            text = message.text or message.caption or ""
            
            # Ловим первое фото с #тейк ИЛИ последующие фото из этого же альбома
            if "#тейк" in text.lower() or group_id in media_group_buffer:
                await handle_take_media_group(message, bot_id, bot_instance, state)

        @router.message(F.text.contains("#тейк") | F.caption.contains("#тейк"))
        async def auto_forward_take(message: types.Message, state: FSMContext):
            """Автоматическая обработка одиночного тейка."""
            if message.chat.type in ("channel", "group", "supergroup"):
                return
            current_state = await state.get_state()
            if current_state == TakeStates.WaitingTake:
                return
            await process_take_message(message, bot_id, bot_instance)

        @router.callback_query(F.data.startswith("take_approve_"))
        async def take_approve(callback: types.CallbackQuery):
            take_id = callback.data[13:]
            take_data = config.pending_takes.get(take_id)
            if not take_data:
                await callback.answer("Тейк не найден", show_alert=True)
                return
            cfg = config.bots.get(take_data['bot_id'])
            try:
                # Проверяем тип тейка
                if take_data.get('type') == 'take_media_group':
                    # Это медиагруппа
                    from aiogram.types import InputMediaPhoto, InputMediaVideo
                    
                    media_group = []
                    for idx, media_info in enumerate(take_data['media_group']):
                        text = media_info.get('caption', '') if idx == 0 else ""
                        entities = restore_entities(media_info.get('caption_entities')) if idx == 0 else None
                        
                        # Для главного бота добавляем подпись
                        if take_data['bot_id'] == "main" and idx == 0 and text:
                            import re as re_module
                            pattern = re_module.compile(r'(#тейк)', re_module.IGNORECASE)
                            if pattern.search(text):
                                text = pattern.sub(r'\1\n★@Wings_teyk_bot ; @Wings_of_fire_CF★', text, count=1)
                        
                        censored, has_prof = censor_profanity(text, take_data['bot_id'])
                        has_spoiler = media_info.get('has_spoiler', False)
                        
                        if media_info.get('photo'):
                            media_group.append(InputMediaPhoto(
                                media=media_info['photo'],
                                caption=censored if (idx == 0 and has_prof) else (text if idx == 0 else ""),
                                parse_mode="HTML" if (idx == 0 and has_prof) else None,
                                caption_entities=entities if (idx == 0 and not has_prof) else None,
                                has_spoiler=has_spoiler
                            ))
                        elif media_info.get('video'):
                            media_group.append(InputMediaVideo(
                                media=media_info['video'],
                                caption=censored if (idx == 0 and has_prof) else (text if idx == 0 else ""),
                                parse_mode="HTML" if (idx == 0 and has_prof) else None,
                                caption_entities=entities if (idx == 0 and not has_prof) else None,
                                has_spoiler=has_spoiler
                            ))
                    
                    await bot_instance.send_media_group(cfg.takes_channel, media_group)
                    logger.info(f"Тейк-альбом одобрен: {len(media_group)} медиа")
                else:
                    # Одиночный тейк
                    text = take_data.get('caption') or take_data.get('text', '')
                    entities = restore_entities(take_data.get('caption_entities'))
                    censored, has_profanity = censor_profanity(text, bot_id)
                    send_kwargs = {
                        "caption": censored if has_profanity else text,
                        "parse_mode": "HTML" if has_profanity else None,
                        "caption_entities": entities if not has_profanity else None
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
                            parse_mode="HTML" if has_profanity else None,
                            entities=entities if not has_profanity else None
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

        @router.callback_query(F.data.startswith("take_delete_"))
        async def take_delete_from_channel(callback: types.CallbackQuery):
            """Удаление уже опубликованного тейка из канала (включая медиагруппы)."""
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            
            # ИЗМЕНЕНИЕ: Парсим список ID через запятую
            msg_ids_str = callback.data[12:]  # Получаем "123,124,125"
            channel_msg_ids = [int(x) for x in msg_ids_str.split(",")]  # Преобразуем в список
            
            cfg = config.bots.get(bot_id)
            deleted_count = 0
            errors = []
            
            # ИЗМЕНЕНИЕ: Удаляем ВСЕ сообщения из списка
            for msg_id in channel_msg_ids:
                try:
                    await bot_instance.delete_message(cfg.takes_channel, msg_id)
                    deleted_count += 1
                    logger.info(f"Тейк {msg_id} удалён модератором {callback.from_user.id}")
                except Exception as e:
                    logger.error(f"Ошибка удаления тейка {msg_id}: {e}")
                    errors.append(str(e))
            
            if deleted_count == len(channel_msg_ids):
                await callback.message.edit_text(f"✅ Удалено {deleted_count} сообщений из канала.")
            else:
                await callback.message.edit_text(
                    f"⚠️ Удалено {deleted_count} из {len(channel_msg_ids)} сообщений.\nОшибки: {', '.join(errors)}"
                )
            
            await callback.answer()
    
        @router.callback_query(F.data.startswith("user_block_"))
        async def block_user_from_takes(callback: types.CallbackQuery):
            """Блокировка пользователя для тейков."""
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            uid = int(callback.data[11:])
            db.set_bot_data(uid, bot_id, is_blocked=True)
            try:
                await bot_instance.send_message(uid, "🚫 Вы заблокированы для отправки тейков.")
            except Exception:
                pass
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="🔓 Разблокировать (тейки)",
                            callback_data=f"take_unblock_{uid}"
                        )]
                    ])
                )
            except Exception:
                pass
            await callback.answer("🚫 Заблокирован для тейков", show_alert=True)

        @router.callback_query(F.data.startswith("take_unblock_"))
        async def unblock_user_from_takes(callback: types.CallbackQuery):
            """Разблокировка пользователя для тейков."""
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            uid = int(callback.data[13:])
            db.set_bot_data(uid, bot_id, is_blocked=False)
            try:
                await bot_instance.send_message(uid, "✅ Вы разблокированы для тейков.")
            except Exception:
                pass
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="🚫 Заблокировать (тейки)",
                            callback_data=f"user_block_{uid}"
                        )]
                    ])
                )
            except Exception:
                pass
            await callback.answer("✅ Разблокирован для тейков", show_alert=True)

        @router.callback_query(F.data.startswith("user_unblock_"))
        async def unblock_user_legacy(callback: types.CallbackQuery):
            """Разблокировка тейков — устаревший формат для обратной совместимости."""
            if not check_moderator(callback.from_user.id, bot_id):
                await callback.answer("Нет доступа", show_alert=True)
                return
            uid = int(callback.data[13:])
            db.set_bot_data(uid, bot_id, is_blocked=False)
            try:
                await bot_instance.send_message(uid, "✅ Вы разблокированы для тейков.")
            except Exception:
                pass
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="🚫 Заблокировать (тейки)",
                            callback_data=f"user_block_{uid}"
                        )]
                    ])
                )
            except Exception:
                pass
            await callback.answer("✅ Разблокирован для тейков", show_alert=True)

    dp.include_router(router)

def create_shop_admin_handlers(bot_id: str, bot_instance: Bot, dp: Dispatcher):
    """Обработчики магазина, пиара, админ-панели и канала."""
    # Создаём роутер с уникальным именем
    router = Router(name=f"shop_admin_{bot_id}_{id(dp)}")
    bot_config = config.bots.get(bot_id)

    # =================== МАГАЗИН / ПИАР ===================

    if bot_config and "shop" in bot_config.modules:

        @router.callback_query(F.data == "shop")
        async def callback_shop(callback: types.CallbackQuery):
            await callback.message.edit_text("🛒 Магазин:", reply_markup=build_shop_menu(bot_id))
            await callback.answer()

        @router.message(
            F.text.contains("#продажа") | F.caption.contains("#продажа") |
            F.text.contains("#обмен") | F.caption.contains("#обмен")
        )
        async def auto_forward_shop(message: types.Message, state: FSMContext):
            # Только личные чаты — не каналы и не группы
            if message.chat.type in ("channel", "group", "supergroup"):
                return
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
            success, error = do_transfer(
                purchase['buyer_id'], purchase['seller_id'], purchase['bot_id'], purchase['amount']
            )
            if success:
                await callback.message.edit_caption(
                    caption=f"✅ Продано! +{purchase['amount']} {cfg.currency_emoji}"
                )
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
            if user_bot_data.get('is_blocked'): flags += "🚫т"
            if user_bot_data.get('is_announcement_blocked'): flags += "🚫о"
            if user_bot_data.get('is_moderator'): flags += "👮"
            if user_bot_data.get('is_announcement_mod'): flags += "📢"
            text += f"@{user['username']}: {balance_str} {cfg.currency_emoji} {flags}\n"
        text += "\n🚫т — заблок.тейки, 🚫о — заблок.объявления"
        await callback.message.edit_text(text, reply_markup=build_admin_menu(callback.from_user.id, bot_id))
        await callback.answer()

    @router.callback_query(F.data == "adm_deduct")
    async def admin_deduct_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("Введите username для списания:", reply_markup=build_cancel_keyboard())
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
            db.set_bot_data(uid, bot_id, is_frozen=True)
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
            db.set_bot_data(uid, bot_id, is_frozen=False)
            await message.answer("🔥 Счёт разморожен.", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

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
                        entities = restore_entities(take.get('caption_entities'))
                        send_kwargs = {
                            "caption": censored if has_prof else text,
                            "parse_mode": "HTML" if has_prof else None,
                            "caption_entities": entities if not has_prof else None
                        }
                        if take.get('photo'):
                            await bot_instance.send_photo(cfg.takes_channel, photo=take['photo'], **send_kwargs)
                        elif take.get('video'):
                            await bot_instance.send_video(cfg.takes_channel, video=take['video'], **send_kwargs)
                        else:
                            await bot_instance.send_message(
                                cfg.takes_channel,
                                censored if has_prof else text,
                                parse_mode="HTML" if has_prof else None,
                                entities=entities if not has_prof else None
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

    @router.callback_query(F.data == "adm_toggle_manual")
    async def admin_toggle_manual(callback: types.CallbackQuery):
        if not check_owner(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        cfg = config.bots.get(bot_id)
        cfg.manual_control = not cfg.manual_control
        config.save()
        status = "🔒 Ручной контроль включён" if cfg.manual_control else "🔓 Авто-контроль включён"
        await callback.answer(status, show_alert=True)
        await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

    @router.callback_query(F.data == "adm_channel_quiz")
    async def admin_channel_quiz_start(callback: types.CallbackQuery, state: FSMContext):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text(
            "🎯 Провести викторину в канале\n\nОтправьте вопрос (текст, фото или видео):",
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
                'bot_id': bot_id, 'message_id': sent.message_id,
                'answer': correct_answer, 'reward': data['quiz_reward'],
                'channel': cfg.takes_channel, 'solved': False
            }
            config.save()
            await message.answer(
                f"✅ Викторина опубликована!\nПравильный ответ: {correct_answer}\n"
                f"Награда: {data['quiz_reward']} {cfg.currency_emoji}",
                reply_markup=build_admin_menu(message.from_user.id, bot_id)
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}", reply_markup=build_admin_menu(message.from_user.id, bot_id))
        await state.clear()

    # =================== ЦЕНЗУРА ===================

    @router.callback_query(F.data == "adm_censor")
    async def admin_censor_menu(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text(
            "🔧 Управление цензурой\nБазовые корни работают всегда.",
            reply_markup=build_censor_menu()
        )
        await callback.answer()

    @router.callback_query(F.data == "censor_add")
    async def censor_add_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите слово/корень:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingCensorWord)
        await callback.answer()

    @router.message(AdminStates.WaitingCensorWord)
    async def censor_add_process(message: types.Message, state: FSMContext):
        word = message.text.strip().lower()
        cfg = config.bots.get(bot_id)
        if word not in cfg.censored_words:
            cfg.censored_words.append(word)
            config.save()
        await message.answer(f"✅ Слово '{word}' добавлено.", reply_markup=build_censor_menu())
        await state.clear()

    @router.callback_query(F.data == "censor_del")
    async def censor_del_start(callback: types.CallbackQuery, state: FSMContext):
        cfg = config.bots.get(bot_id)
        if not cfg.censored_words:
            await callback.answer("Список пуст.", show_alert=True)
            return
        await callback.message.edit_text(
            f"Слова: {', '.join(cfg.censored_words)}\n\nВведите слово для удаления:",
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
        await callback.message.edit_text("Введите маркер:", reply_markup=build_cancel_keyboard())
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

    # =================== МОДЕРАТОРЫ ТЕЙКОВ ===================

    @router.callback_query(F.data == "adm_mods")
    async def admin_mods_menu(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text("👮 Управление модераторами тейков:", reply_markup=build_mods_menu())
        await callback.answer()

    @router.callback_query(F.data == "mod_assign")
    async def mod_assign_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите username модератора тейков:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingModeratorUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingModeratorUsername)
    async def mod_assign_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_moderator=True)
            await message.answer("✅ Назначен модератором тейков.", reply_markup=build_mods_menu())
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
            db.set_bot_data(uid, bot_id, is_moderator=False)
            await message.answer("✅ Снят с модераторов тейков.", reply_markup=build_mods_menu())
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
        text = ", ".join(moderators) if moderators else "(нет модераторов тейков)"
        await callback.message.edit_text(f"👮 Модераторы тейков: {text}", reply_markup=build_mods_menu())
        await callback.answer()

    # =================== МОДЕРАТОРЫ ОБЪЯВЛЕНИЙ ===================

    @router.callback_query(F.data == "adm_announcement_mods")
    async def admin_announcement_mods_menu(callback: types.CallbackQuery):
        if not check_admin(callback.from_user.id, bot_id):
            await callback.answer("Нет доступа", show_alert=True)
            return
        await callback.message.edit_text(
            "📢 Управление модераторами объявлений:",
            reply_markup=build_announcement_mods_menu()
        )
        await callback.answer()

    @router.callback_query(F.data == "ann_mod_assign")
    async def announcement_mod_assign_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text(
            "Введите username модератора объявлений:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(AdminStates.WaitingAnnouncementModUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingAnnouncementModUsername)
    async def announcement_mod_assign_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_announcement_mod=True)
            await message.answer(
                "✅ Назначен модератором объявлений.\n"
                "Теперь все объявления будут приходить ему на проверку.",
                reply_markup=build_announcement_mods_menu()
            )
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "ann_mod_remove")
    async def announcement_mod_remove_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("Введите username для снятия:", reply_markup=build_cancel_keyboard())
        await state.set_state(AdminStates.WaitingRemoveAnnouncementModUsername)
        await callback.answer()

    @router.message(AdminStates.WaitingRemoveAnnouncementModUsername)
    async def announcement_mod_remove_process(message: types.Message, state: FSMContext):
        uid = db.find_user_by_input(message.text)
        if uid:
            db.set_bot_data(uid, bot_id, is_announcement_mod=False)
            await message.answer("✅ Снят с модераторов объявлений.", reply_markup=build_announcement_mods_menu())
        else:
            await message.answer("Пользователь не найден.", reply_markup=build_cancel_keyboard())
        await state.clear()

    @router.callback_query(F.data == "ann_mod_list")
    async def announcement_mod_list_show(callback: types.CallbackQuery):
        users_list = db.get_all_users_for_bot(bot_id)
        ann_mods = []
        for user in users_list:
            user_bot_data = db.get_bot_data(user['user_id'], bot_id)
            if user_bot_data.get('is_announcement_mod'):
                user_info = db.get_user(user['user_id'])
                if user_info:
                    ann_mods.append(f"@{user_info['username']}")
        text = ", ".join(ann_mods) if ann_mods else "(нет модераторов объявлений)"
        await callback.message.edit_text(
            f"📢 Модераторы объявлений: {text}",
            reply_markup=build_announcement_mods_menu()
        )
        await callback.answer()

    # =================== СПЕЦКНОПКИ ГЛАВНОГО АДМИНА ===================

    if bot_id == "main":
        @router.callback_query(F.data == "adm_reset_rates")
        async def admin_reset_rates(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            reset_all_rates()
            await callback.answer("✅ Курсы сброшены.", show_alert=True)
            await callback.message.edit_text("Админ-панель:", reply_markup=build_admin_menu(callback.from_user.id, bot_id))

        @router.callback_query(F.data == "adm_reset_top")
        async def admin_reset_top(callback: types.CallbackQuery):
            if callback.from_user.id != MAIN_ADMIN_ID:
                await callback.answer("Нет доступа", show_alert=True)
                return
            users_list = db.get_all_users_for_bot(bot_id)
            for user in users_list:
                db.set_bot_data(user['user_id'], bot_id, show_in_top=False)
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

    # =================== ОТСЛЕЖИВАНИЕ КАНАЛА ===================

    @router.channel_post()
    async def handle_channel_post(message: types.Message):
        """Отслеживание постов в канале — создание аукционов по #аукцион."""
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
                logger.error(f"Ошибка получения группы комментариев: {e}")

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
        Только для групп — не для личных чатов и каналов.
        """
        if message.chat.type not in ("group", "supergroup"):
            return
        if not message.forward_from_chat:
            return
        forward_msg_id = message.forward_from_message_id
        if not forward_msg_id:
            return
        auction_id = str(forward_msg_id)
        auction = config.active_auctions.get(auction_id)
        if auction and not auction.get('discussion_message_id'):
            auction['discussion_message_id'] = message.message_id
            auction['discussion_chat_id'] = message.chat.id
            config.save()
            logger.info(f"Аукцион {auction_id}: ID в группе = {message.message_id}")

    @router.message(F.reply_to_message)
    async def handle_comment_reply(message: types.Message):
        """Обработка комментариев — викторины и аукционы."""
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
        comment_text_lower = comment_text.lower()

        # ===== ВИКТОРИНА =====
        quiz = config.active_quizzes.get(original_msg_id)
        if quiz and not quiz.get('solved') and quiz.get('bot_id') == bot_id:
            if comment_text_lower == quiz.get('answer', '').lower():
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
                await message.reply(
                    f"↩️ Ставка отменена.\n"
                    f"Актуальная ставка: {prev_bid['amount']} {cfg.currency_emoji} ({prev_display})"
                )
            except Exception as e:
                logger.error(f"Ошибка ответа пас/лив: {e}")
            old_task = config.auction_tasks.get(original_msg_id)
            if old_task and not old_task.done():
                old_task.cancel()
            task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, original_msg_id))
            config.auction_tasks[original_msg_id] = task
            return

        # Проверяем ставку
        bet_match = BET_PATTERN.search(comment_text)
        if not bet_match:
            return

        bet_amount = int(bet_match.group(1))
        register_user(message.from_user, bot_id)
        user_balance = db.get_balance(user_id, bot_id)
        current_bid = auction.get('current_bid', 0)

        if bet_amount <= current_bid:
            try:
                await message.reply(f"Ошибка: ставка равна или меньше предыдущей ({current_bid} {cfg.currency_emoji})")
            except Exception:
                pass
            return

        if bet_amount - current_bid < MIN_BID_INCREMENT:
            try:
                await message.reply(f"Ошибка: недостаточное повышение (минимум +{MIN_BID_INCREMENT} {cfg.currency_emoji})")
            except Exception:
                pass
            return

        if user_balance != float('inf') and user_balance < bet_amount:
            try:
                await message.reply(f"У вас недостаточно средств.\nБаланс: {user_balance:.0f} {cfg.currency_emoji}")
            except Exception as e:
                logger.error(f"Ошибка ответа аукциона: {e}")
            return

        user_info = db.get_user(user_id)
        if user_info and user_info.get('username') and not user_info['username'].startswith('user'):
            display_name = f"@{user_info['username']}"
        else:
            display_name = user_name

        if 'bid_history' not in auction:
            auction['bid_history'] = []
        auction['bid_history'].append({
            'bidder': user_id,
            'amount': bet_amount,
            'display': display_name,
            'time': datetime.now().isoformat()
        })
        auction['current_bidder'] = user_id
        auction['current_bid'] = bet_amount
        auction['last_bid_time'] = datetime.now().isoformat()
        config.save()

        try:
            await message.reply("Ставка принята")
        except Exception as e:
            logger.error(f"Ошибка подтверждения ставки: {e}")

        old_task = config.auction_tasks.get(original_msg_id)
        if old_task and not old_task.done():
            old_task.cancel()
        task = asyncio.create_task(run_auction_timer(bot_instance, bot_id, original_msg_id))
        config.auction_tasks[original_msg_id] = task

    # ИСПРАВЛЕНИЕ: Безопасное подключение роутера
    try:
        dp.include_router(router)
        logger.info(f"✅ Роутер shop_admin для {bot_id} подключён")
    except RuntimeError as e:
        logger.warning(f"⚠️ Роутер shop_admin для {bot_id} уже подключён, пропускаем")


# ====================== ПОДКЛЮЧЕНИЕ БОТОВ ======================

def create_connection_handlers(bot_instance: Bot, dp: Dispatcher):
    """Обработчики подключения новых ботов (только главный бот)."""
    # ИСПРАВЛЕНИЕ: Создаём НОВЫЙ роутер
    router = Router(name="connection_handlers")

    @router.callback_query(F.data == "connect_bot")
    async def connect_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text(
            "🤖 Подключение бота\n\nОтправьте ссылку на ваш канал:",
            reply_markup=build_cancel_keyboard()
        )
        await state.set_state(ConnectBotStates.WaitingChannelUrl)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingChannelUrl)
    async def connect_channel(message: types.Message, state: FSMContext):
        request_id = str(uuid.uuid4())[:8]
        request = PendingBotRequest(
            request_id=request_id,
            user_id=message.from_user.id,
            channel_url=message.text.strip()
        )
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
                f"📝 Заявка на подключение бота\n"
                f"От: @{user['username'] if user else '?'}\n"
                f"Канал: {message.text}",
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
            InlineKeyboardButton(text="✅ Да", callback_data=f"request_confirm_{rid}"),
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
            await bot_instance.send_message(
                req.user_id,
                "✅ Заявка одобрена!\n\nОтправьте токен бота от @BotFather:"
            )
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
            await callback.message.edit_text(
                "📝 Выбраны тейки\n\n"
                "Отправьте ссылку или ID канала для тейков\n"
                "(закрытый канал — числовой ID, например: -1001234567890):"
            )
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        elif module_type == "shop":
            req.modules = ["shop"]
            config.save()
            await callback.message.edit_text(
                "🛒 Выбран магазин\n\n"
                "Отправьте ссылку или ID канала для объявлений\n"
                "(закрытый канал — числовой ID, например: -1001234567890):"
            )
            await state.set_state(ConnectBotStates.WaitingAnnouncementChannel)
        else:
            req.modules = ["takes", "shop"]
            config.save()
            await callback.message.edit_text(
                "📝🛒 Выбраны тейки + магазин\n\n"
                "Сначала ссылка или ID канала для ТЕЙКОВ\n"
                "(закрытый канал — числовой ID, например: -1001234567890):"
            )
            await state.set_state(ConnectBotStates.WaitingTakesChannel)
        await callback.answer()

    @router.message(ConnectBotStates.WaitingTakesChannel)
    async def connect_takes_channel(message: types.Message, state: FSMContext):
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
                f"✅ Канал для тейков: {channel}\n\n"
                f"Теперь ссылка или ID канала для ОБЪЯВЛЕНИЙ\n"
                f"(закрытый канал — числовой ID, например: -1001234567890):"
            )
            await state.set_state(ConnectBotStates.WaitingAnnouncementChannel)
        else:
            await finalize_bot_setup(message, state, req, bot_instance)

    @router.message(ConnectBotStates.WaitingAnnouncementChannel)
    async def connect_announcement_channel(message: types.Message, state: FSMContext):
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
        await finalize_bot_setup(message, state, req, bot_instance)

    async def finalize_bot_setup(message, state, req, main_bot):
        """Завершение настройки и запуск подключённого бота."""
        await message.answer("⏳ Запускаю бота...")
        try:
            new_bot_id = f"bot_{req.user_id}_{int(datetime.now().timestamp())}"
            new_config = BotConfig(
                bot_id=new_bot_id,
                token=req.token,
                currency_name=req.currency_name,
                currency_emoji=req.currency_emoji,
                channel_url=req.channel_url,
                takes_channel=req.takes_channel,
                shop_channel=req.shop_channel,
                announcement_channel=req.announcement_channel,
                modules=req.modules,
                owner_id=req.user_id,
                base_exchange_rate=0.5,
                take_cooldown_minutes=TAKE_COOLDOWN_MINUTES
            )
            config.bots[new_bot_id] = new_config

            register_user(message.from_user, new_bot_id)
            db.set_balance(req.user_id, new_bot_id, float('inf'))
            db.set_bot_data(
                req.user_id, new_bot_id,
                is_owner=True, is_admin=True,
                activated_at=datetime.now().isoformat()
            )

            config.exchange_rates.rates[new_bot_id] = 0.5
            if req.request_id in config.pending_requests:
                del config.pending_requests[req.request_id]
            config.save()

            new_bot = Bot(token=req.token)
            new_dp = Dispatcher(storage=MemoryStorage())
            create_bot_handlers(new_bot_id, new_bot, new_dp)
            create_shop_admin_handlers(new_bot_id, new_bot, new_dp)
            config.active_bots[new_bot_id] = new_bot
            config.active_dispatchers[new_bot_id] = new_dp
            asyncio.create_task(new_dp.start_polling(new_bot))

            modules_text = ", ".join(req.modules)
            channels_info = ""
            if req.takes_channel:
                channels_info += f"📝 Тейки: {req.takes_channel}\n"
            if req.announcement_channel:
                channels_info += f"📢 Объявления: {req.announcement_channel}\n"

            await message.answer(
                f"✅ Бот успешно подключён!\n\n"
                f"💰 Валюта: {req.currency_name} {req.currency_emoji}\n"
                f"📦 Модули: {modules_text}\n"
                f"{channels_info}\n"
                f"Вы — владелец с бесконечным балансом.\n"
                f"Перейдите в бота и нажмите /start",
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

    main_cfg = config.bots.get("main")
    if main_cfg:
        logger.info(f"Канал объявлений главного бота: '{main_cfg.announcement_channel}'")
        logger.info(f"Лимит тейков: {MAX_TAKES} шт, восстановление каждые {TAKE_COOLDOWN_MINUTES} мин")

    # ===== НОВОЕ: Автоматическая регистрация админов из переменных окружения =====
    logger.info("Регистрация админов из переменных окружения...")
    
    # 1. Главный админ (владелец)
    main_owner_data = db.get_bot_data(MAIN_ADMIN_ID, "main")
    if not main_owner_data.get('is_owner'):
        # Создаём запись о главном админе если его нет
        existing_user = db.get_user(MAIN_ADMIN_ID)
        if not existing_user:
            db.create_or_update_user(MAIN_ADMIN_ID, f"admin{MAIN_ADMIN_ID}", "Главный админ")
        
        db.set_balance(MAIN_ADMIN_ID, "main", float('inf'))
        db.set_bot_data(MAIN_ADMIN_ID, "main",
            quiz_passed=False, show_in_top=False,
            is_blocked=False, is_frozen=False,
            is_moderator=True, is_announcement_mod=True,
            is_announcement_blocked=False,
            is_admin=True, is_owner=True,
            activated_at=datetime.now().isoformat(),
            last_promo_at=None
        )
        logger.info(f"✅ Главный админ {MAIN_ADMIN_ID} зарегистрирован")
    else:
        logger.info(f"✅ Главный админ {MAIN_ADMIN_ID} уже зарегистрирован")
    
    # 2. Остальные админы
    for admin_id in ADMIN_IDS:
        if admin_id == MAIN_ADMIN_ID:
            continue  # Главного админа уже зарегистрировали
        
        admin_data = db.get_bot_data(admin_id, "main")
        if not admin_data.get('is_admin'):
            # Создаём запись об админе если его нет
            existing_user = db.get_user(admin_id)
            if not existing_user:
                db.create_or_update_user(admin_id, f"admin{admin_id}", f"Админ {admin_id}")
            
            # Даём стартовый баланс если ещё нет
            current_balance = db.get_balance(admin_id, "main")
            if current_balance == 0:
                starting_balance = main_cfg.admin_starting_balance if main_cfg else 100
                db.set_balance(admin_id, "main", starting_balance)
            
            db.set_bot_data(admin_id, "main",
                quiz_passed=False, show_in_top=True,
                is_blocked=False, is_frozen=False,
                is_moderator=True, is_announcement_mod=True,
                is_announcement_blocked=False,
                is_admin=True, is_owner=False,
                activated_at=datetime.now().isoformat(),
                last_promo_at=None
            )
            logger.info(f"✅ Админ {admin_id} зарегистрирован")
        else:
            logger.info(f"✅ Админ {admin_id} уже зарегистрирован")
    
    # 3. Регистрируем владельцев подключённых ботов
    for bot_id, bot_cfg in config.bots.items():
        if bot_id == "main":
            continue
        if bot_cfg.owner_id:
            owner_data = db.get_bot_data(bot_cfg.owner_id, bot_id)
            if not owner_data.get('is_owner'):
                existing_user = db.get_user(bot_cfg.owner_id)
                if not existing_user:
                    db.create_or_update_user(bot_cfg.owner_id, f"owner{bot_cfg.owner_id}", f"Владелец {bot_cfg.owner_id}")
                
                db.set_balance(bot_cfg.owner_id, bot_id, float('inf'))
                db.set_bot_data(bot_cfg.owner_id, bot_id,
                    quiz_passed=False, show_in_top=False,
                    is_blocked=False, is_frozen=False,
                    is_moderator=True, is_announcement_mod=True,
                    is_announcement_blocked=False,
                    is_admin=True, is_owner=True,
                    activated_at=datetime.now().isoformat(),
                    last_promo_at=None
                )
                logger.info(f"✅ Владелец {bot_cfg.owner_id} бота {bot_id} зарегистрирован")

    # Главный бот
    main_bot = Bot(token=MAIN_BOT_TOKEN)
    main_dp = Dispatcher(storage=MemoryStorage())

   # === ВСТАВИТЬ СЮДА ===
    main_dp.message.outer_middleware(RegistrationMiddleware())
    main_dp.callback_query.outer_middleware(RegistrationMiddleware())
    
    create_bot_handlers("main", main_bot, main_dp)
    create_shop_admin_handlers("main", main_bot, main_dp)
    create_connection_handlers(main_bot, main_dp)
    
    config.active_bots["main"] = main_bot
    config.active_dispatchers["main"] = main_dp

    # Запуск подключённых ботов (БЕЗ главного)
    for bot_id, bot_cfg in config.bots.items():
        if bot_id == "main":
            continue
        try:
            connected_bot = Bot(token=bot_cfg.token)
            connected_dp = Dispatcher(storage=MemoryStorage())

             # === ВСТАВИТЬ СЮДА (для каждого дочернего бота) ===
            connected_dp.message.outer_middleware(RegistrationMiddleware())
            connected_dp.callback_query.outer_middleware(RegistrationMiddleware()) 
            
            create_bot_handlers(bot_id, connected_bot, connected_dp)
            create_shop_admin_handlers(bot_id, connected_bot, connected_dp)
            
            config.active_bots[bot_id] = connected_bot
            config.active_dispatchers[bot_id] = connected_dp
            
            asyncio.create_task(connected_dp.start_polling(connected_bot))
            logger.info(f"Запущен подключённый бот: {bot_id}")
        except Exception as e:
            logger.error(f"Ошибка запуска бота {bot_id}: {e}")

    # Восстановление задач удаления пиара после перезапуска
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
