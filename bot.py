import os
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.request import HTTPXRequest
import tempfile
import logging
import re
import numpy as np
import asyncio
import telegram
from collections import deque
import shutil
import json

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ALLOWED_EXTENSIONS = {'.xlsx', '.xls', '.xlsm'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 МБ
MAX_QUEUE_SIZE = 10  # Максимум 10 файлов в очереди на пользователя

# Время автоудаления служебных сообщений (в секундах)
MESSAGE_DELETE_DELAY = 60  # 60 секунд = 1 минута

# Время задержки перед удалением временных файлов (в секундах)
TEMP_FILE_DELETE_DELAY = 30  # 30 секунд

# Состояния пользователей для редактирования
user_edit_state = {}  # {user_id: {'message_id': 123, 'data': {...}, 'street_key': '...'}}
user_command_state = {}  # {user_id: {'action': 'merge', 'step': 1, 'data': {...}}}

# Очереди файлов для каждого пользователя
user_queues = {}
user_locks = {}
user_processing = {}

# Создаем кастомный request с большими таймаутами (без прокси)
custom_request = HTTPXRequest(
    connect_timeout=120.0,
    read_timeout=120.0,
    write_timeout=120.0,
    pool_timeout=120.0
)

def get_menu_keyboard(file_name: str = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками управления"""
    keyboard = []
    
    if file_name:
        keyboard.append([InlineKeyboardButton("📊 Статистика", callback_data="stats")])
    
    keyboard.append([InlineKeyboardButton("🗑️ Удалить последний результат", callback_data="del")])
    keyboard.append([InlineKeyboardButton("❓ Помощь", callback_data="help")])
    
    return InlineKeyboardMarkup(keyboard)

def get_edit_keyboard(street_key: str = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру для редактирования"""
    keyboard = []
    
    if street_key:
        keyboard.append([InlineKeyboardButton(f"🔹 Объединить все заявки по {street_key}", callback_data=f"merge_street_{street_key}")])
    
    keyboard.append([InlineKeyboardButton("🔹 Объединить заявки", callback_data="simple_merge")])
    keyboard.append([InlineKeyboardButton("🔹 Удалить заявку", callback_data="simple_delete")])
    keyboard.append([InlineKeyboardButton("🔹 Разделить заявки", callback_data="simple_split")])
    keyboard.append([InlineKeyboardButton("➕ Добавить заявку", callback_data="simple_add")])  # Новая кнопка
    keyboard.append([InlineKeyboardButton("✍️ Свой вариант", callback_data="custom_edit")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit")])
    
    return InlineKeyboardMarkup(keyboard)

async def send_with_retry(coroutine_func, *args, max_retries=3, delay=2, **kwargs):
    """Отправляет сообщение с повторными попытками"""
    for attempt in range(max_retries):
        try:
            result = await coroutine_func(*args, **kwargs)
            return result
        except telegram.error.TimedOut as e:
            if attempt == max_retries - 1:
                logger.error(f"Все {max_retries} попыток исчерпаны. Ошибка: {e}")
                raise e
            wait_time = delay * (attempt + 1)
            logger.warning(f"Таймаут (попытка {attempt + 1}/{max_retries}). Повтор через {wait_time} сек...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке: {e}")
            raise e

async def download_with_retry(file, file_path=None, max_retries=3):
    """Скачивает файл с повторными попытками прямо в указанный путь"""
    for attempt in range(max_retries):
        try:
            if file_path:
                await file.download_to_drive(custom_path=file_path)
                return file_path
            else:
                return await file.download_to_drive()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            wait_time = 2 ** attempt
            logger.warning(f"Ошибка скачивания (попытка {attempt + 1}): {e}. Повтор через {wait_time} сек")
            await asyncio.sleep(wait_time)

async def delayed_delete(file_path: str, delay: int = TEMP_FILE_DELETE_DELAY):
    """Удаляет файл через указанную задержку"""
    await asyncio.sleep(delay)
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
            logger.info(f"⏰ Файл {os.path.basename(file_path)} удалён через {delay} сек")
        else:
            logger.warning(f"⚠️ Файл {file_path} уже не существует")
    except Exception as e:
        logger.error(f"❌ Ошибка при отложенном удалении {file_path}: {e}")

async def delete_message_after_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int):
    """Удаляет сообщение через указанную задержку"""
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Сообщение {message_id} автоматически удалено через {delay} сек")
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение {message_id}: {e}")

def read_excel_optimized(file_path):
    """Читает Excel файл с оптимизациями"""
    try:
        return pd.read_excel(
            file_path,
            dtype=str,
            engine='openpyxl'
        )
    except Exception as e:
        logger.warning(f"Оптимизированное чтение не удалось: {e}")
        return pd.read_excel(file_path)

class ExcelDataProcessor:
    """
    Обработчик Excel файлов для извлечения данных по столбцам
    """
    
    COLUMN_MAPPING = {
        'driver': ['водитель', 'driver'],
        'phone': ['телефон водителя', 'телефон', 'водитель телефон', 'phone', 'tel'],
        'city': ['город', 'city'],
        'direction': ['направление', 'direction'],
        'address': ['адрес', 'address', 'адрес доставки'],
        'time_from': ['время с', 'time from'],
        'time_to': ['время по', 'time to'],
        'order_number': ['№ зэ', 'зэ', 'номер зэ', 'заявка', '№ заявки', 'order'],
        'invoice_number': ['№ накладной', 'накладная', 'invoice'],
        'places': ['мест', 'places', 'кол-во мест'],
        'weight': ['вес', 'weight', 'общий вес'],
        'volume': ['объем', 'volume'],
        'max_weight': ['макс вес 1 места', 'макс вес', 'max weight'],
        'max_volume': ['макс объем 1 места', 'макс объем', 'max volume'],
        'length': ['длина', 'length'],
        'width': ['ширина', 'width'],
        'height': ['высота', 'height'],
        'comment': ['комментарий', 'comment', 'примечание'],
        'executors': ['исполнители', 'исполнитель', 'грузчики', 'кол-во грузчиков', 'чел', 'executors']
    }
    
    ORDER_NUMBER_LENGTH = 11
    
    def __init__(self):
        self.data = {key: [] for key in self.COLUMN_MAPPING.keys()}
        self.data['has_fixa'] = []
        self.data['fixa_time'] = []
        self.found_columns = {}
        self.df = None
        self.original_df = None
    
    def find_columns(self, df):
        """Находит нужные колонки в DataFrame"""
        df_columns = [str(col).lower().strip() for col in df.columns]
        found = {}
        
        for data_type, variants in self.COLUMN_MAPPING.items():
            for col_idx, col_name in enumerate(df_columns):
                for variant in variants:
                    if variant in col_name:
                        found[data_type] = col_idx
                        logger.info(f"Найдена колонка '{data_type}': '{df.columns[col_idx]}' (индекс {col_idx})")
                        break
                if data_type in found:
                    break
        
        self.found_columns = found
        return found
    
    def preserve_order_number(self, value):
        """Сохраняет формат номера заявки с ведущими нулями"""
        if value is None or pd.isna(value):
            return ""
        
        if isinstance(value, (int, float)):
            str_value = str(int(value))
            if len(str_value) < self.ORDER_NUMBER_LENGTH:
                str_value = str_value.zfill(self.ORDER_NUMBER_LENGTH)
            return str_value
        
        if isinstance(value, str):
            digits_only = re.sub(r'\D', '', value)
            if digits_only and len(digits_only) <= self.ORDER_NUMBER_LENGTH:
                if len(digits_only) < self.ORDER_NUMBER_LENGTH:
                    return digits_only.zfill(self.ORDER_NUMBER_LENGTH)
            return value.strip()
        
        return str(value).strip()
    
    def is_phone_number(self, text):
        """Проверяет, является ли текст номером телефона"""
        if not text:
            return False
        text = str(text)
        digit_count = sum(c.isdigit() for c in text)
        return digit_count >= 10
    
    def format_phone_number(self, phone):
        """Форматирует номер телефона"""
        if not phone or phone == "Телефон не указан" or phone == "Нет телефона":
            return phone
        
        digits = re.sub(r'\D', '', str(phone))
        
        if len(digits) >= 10:
            if len(digits) > 10:
                digits = digits[-10:]
            clean_phone = '+7' + digits
            logger.info(f"Номер отформатирован: {phone} -> {clean_phone}")
            return clean_phone
        else:
            return phone
    
    def normalize_executors(self, value):
        """Нормализует значение исполнителей"""
        if pd.isna(value):
            return ""
        
        if isinstance(value, (int, float)):
            if value.is_integer():
                return f"{int(value)} чел"
            else:
                return f"{value} чел"
        
        exec_str = str(value).strip()
        if not exec_str or exec_str.lower() == 'nan':
            return ""
        
        exec_str_lower = exec_str.lower()
        
        if 'нтк' in exec_str_lower or 'ntk' in exec_str_lower:
            return "НТК"
        
        if ('вэ' in exec_str_lower or 've' in exec_str_lower) and ('дл' in exec_str_lower or 'дп' in exec_str_lower):
            parts = []
            
            ve_match = re.search(r'(\d+)\s*[+]?\s*(\d*)\s*(вэ|ve)', exec_str_lower)
            if ve_match:
                first = ve_match.group(1)
                second = ve_match.group(2)
                if second:
                    parts.append(f"{first} + {second} ВЭ")
                else:
                    parts.append(f"{first} ВЭ")
            
            dl_match = re.search(r'(\d+)\s*[+]?\s*(\d*)\s*(дл|дп)', exec_str_lower)
            if dl_match:
                first = dl_match.group(1)
                second = dl_match.group(2)
                suffix = dl_match.group(3).upper()
                if second:
                    parts.append(f"{first} + {second} {suffix}")
                else:
                    parts.append(f"{first} {suffix}")
            
            if parts:
                return " + ".join(parts) + " чел"
        
        if 'дл' in exec_str_lower or 'дп' in exec_str_lower:
            match = re.search(r'(\d+)\s*[+]?\s*(\d*)\s*(дл|дп)', exec_str_lower)
            if match:
                first = match.group(1)
                second = match.group(2)
                suffix = match.group(3).upper()
                if second:
                    return f"{first} + {second} {suffix} чел"
                else:
                    return f"{first} {suffix} чел"
            return f"{exec_str} чел"
        
        if 'вэ' in exec_str_lower or 've' in exec_str_lower:
            match = re.search(r'(\d+)\s*[+]?\s*(\d*)\s*вэ', exec_str_lower)
            if match:
                first = match.group(1)
                second = match.group(2)
                if second:
                    return f"{first} + {second} ВЭ чел"
                else:
                    return f"{first} ВЭ чел"
            return f"{exec_str} чел"
        
        try:
            num = float(exec_str)
            if num.is_integer():
                return f"{int(num)} чел"
            else:
                return f"{num} чел"
        except ValueError:
            pass
        
        if 'чел' not in exec_str_lower:
            return f"{exec_str} чел"
        
        return exec_str
    
    def extract_street_key(self, address):
        """
        Максимально просто - берём всё до первого вхождения " д" или " д."
        """
        if pd.isna(address) or not address:
            return ""
        
        address_str = str(address).lower().strip()
        
        # Ищем позицию " д" или " д."
        for marker in [' д ', ' д,', ' д.', ' д']:
            pos = address_str.find(marker)
            if pos != -1:
                return address_str[:pos].strip()
        
        # Если не нашли, берём первую часть до запятой
        parts = address_str.split(',')
        if parts:
            return parts[0].strip()
        
        return address_str
    
    def extract_address_key(self, address):
        """
        Максимально просто - возвращаем адрес как есть
        """
        if pd.isna(address) or not address:
            return ""
        
        return str(address).lower().strip()
    
    def extract_building_number(self, address):
        """Извлекает номер строения/корпуса"""
        if not address:
            return None
        
        addr_lower = str(address).lower()
        
        building_match = re.search(r'стр\.?\s*(\d+)', addr_lower)
        if building_match:
            return building_match.group(1)
        
        korpus_match = re.search(r'(?:к\.?|корп\.?|корпус)\s*(\d+)', addr_lower)
        if korpus_match:
            return korpus_match.group(1)
        
        slash_match = re.search(r'д\.?\s*\d+/(\d+)', addr_lower)
        if slash_match:
            return slash_match.group(1)
        
        litera_match = re.search(r'лит\.?\s*([а-я])', addr_lower)
        if litera_match:
            return litera_match.group(1)
        
        return None
    
    def are_addresses_similar_for_merge(self, addr1, addr2, time1=None, time2=None, is_fixa1=False, is_fixa2=False):
        """Проверяет, можно ли объединить адреса"""
        if not addr1 or not addr2:
            return False
        
        addr1_lower = str(addr1).lower()
        addr2_lower = str(addr2).lower()
        
        street1 = self.extract_street_key(addr1)
        street2 = self.extract_street_key(addr2)
        
        if street1 != street2:
            return False
        
        has_building1 = 'стр.' in addr1_lower
        has_building2 = 'стр.' in addr2_lower
        
        if has_building1 and has_building2:
            build1 = re.search(r'стр\.?\s*(\d+)', addr1_lower)
            build2 = re.search(r'стр\.?\s*(\d+)', addr2_lower)
            if build1 and build2 and build1.group(1) != build2.group(1):
                return False
        
        def extract_house_number(address):
            match = re.search(r'д\.?\s*(\d+)', address)
            if match:
                return match.group(1)
            return None
        
        house1 = extract_house_number(addr1_lower)
        house2 = extract_house_number(addr2_lower)
        
        if house1 and house2 and house1 == house2:
            return True
        
        return False
    
    def _check_fixa(self, time_from, time_to, address_key=""):
        """Проверяет фиксу"""
        try:
            interval_fixa = self._check_interval_fixa(time_from, time_to, address_key)
            if interval_fixa[0]:
                return interval_fixa
            
            return self._check_special_times_fixa(time_from, address_key)
            
        except Exception as e:
            logger.warning(f"Ошибка при проверке фиксы для адреса {address_key}: {e}")
        
        return False, None

    def _check_interval_fixa(self, time_from, time_to, address_key=""):
        """Проверяет фиксу по интервалу"""
        try:
            if pd.isna(time_from) or pd.isna(time_to):
                return False, None
            
            time_from_str = str(time_from).strip()
            time_to_str = str(time_to).strip()
            
            def time_to_minutes(time_str):
                try:
                    if ':' in time_str:
                        parts = time_str.split(':')
                        if len(parts) >= 2:
                            hours = int(parts[0])
                            minutes = int(parts[1][:2])
                            return hours * 60 + minutes
                except:
                    pass
                return None
            
            minutes_from = time_to_minutes(time_from_str)
            minutes_to = time_to_minutes(time_to_str)
            
            if minutes_from is not None and minutes_to is not None:
                diff = minutes_to - minutes_from
                logger.info(f"Фикса (интервал): адрес '{address_key}' - {time_from_str} - {time_to_str} = {diff} минут")
                
                if 25 <= diff <= 35:
                    logger.info(f"✅ Фикса ПО ИНТЕРВАЛУ для адреса '{address_key}': время {time_from_str}")
                    return True, time_from_str
            return False, None
        except Exception as e:
            logger.warning(f"Ошибка при проверке интервала: {e}")
            return False, None

    def _check_special_times_fixa(self, time_from, address_key=""):
        """Проверяет фиксу по спецвремени"""
        try:
            if pd.isna(time_from):
                return False, None
            
            time_from_str = str(time_from).strip()
            time_clean = re.sub(r'\s', '', time_from_str)
            special_times = ['21:30', '22:00', '22:30', '23:00']
            
            for special in special_times:
                if time_clean.startswith(special):
                    logger.info(f"✅ Фикса ПО СПЕЦВРЕМЕНИ для адреса '{address_key}': время {time_from_str}")
                    return True, time_from_str
            
            return False, None
        except Exception as e:
            logger.warning(f"Ошибка при проверке спецвремени: {e}")
            return False, None
    
    def process_merged_cells(self, df):
        if 'executors' not in self.found_columns or 'order_number' not in self.found_columns or 'address' not in self.found_columns:
            logger.warning("Не найдены необходимые колонки для обработки объединений")
            return df

        exec_col = self.found_columns['executors']
        order_col = self.found_columns['order_number']
        addr_col = self.found_columns['address']

        processed_df = df.copy()
        order_col_name = processed_df.columns[order_col]

        processed_df[order_col_name] = processed_df[order_col_name].apply(
            lambda x: self.preserve_order_number(x)
        )

        processed_df.iloc[:, exec_col] = processed_df.iloc[:, exec_col].apply(
            lambda x: self.normalize_executors(x)
        )

        # Словарь для главных строк по адресу
        main_row_for_address = {}
        rows_to_drop = []

        # Собираем исполнителей по адресам
        address_executors = {}

        for i in range(len(processed_df)):
            current_exec = processed_df.iloc[i, exec_col] if not pd.isna(processed_df.iloc[i, exec_col]) else ""
            current_address = processed_df.iloc[i, addr_col] if not pd.isna(processed_df.iloc[i, addr_col]) else ""
            current_addr_key = self.extract_address_key(current_address)

            if current_addr_key:
                if current_addr_key not in address_executors:
                    address_executors[current_addr_key] = set()
                if current_exec and str(current_exec).strip() and str(current_exec).strip().lower() != 'nan':
                    address_executors[current_addr_key].add(current_exec)

        # Определяем главные строки
        for i in range(len(processed_df)):
            current_address = processed_df.iloc[i, addr_col] if not pd.isna(processed_df.iloc[i, addr_col]) else ""
            current_addr_key = self.extract_address_key(current_address)
            current_exec = processed_df.iloc[i, exec_col] if not pd.isna(processed_df.iloc[i, exec_col]) else ""

            if current_addr_key:
                if len(address_executors.get(current_addr_key, set())) > 1:
                    logger.info(f"⚠️ Адрес '{current_addr_key}' имеет разные исполнители — пропускаем объединение")
                    continue

                if current_addr_key not in main_row_for_address:
                    main_row_for_address[current_addr_key] = i
                    if current_exec and str(current_exec).strip():
                        logger.info(f"Адрес '{current_addr_key}' — главная строка {i} с исполнителями: {current_exec}")
                    else:
                        logger.info(f"Адрес '{current_addr_key}' — главная строка {i} (без исполнителей)")

        # Объединяем номера заявок по одинаковым адресам
        for i in range(len(processed_df)):
            current_address = processed_df.iloc[i, addr_col] if not pd.isna(processed_df.iloc[i, addr_col]) else ""
            current_addr_key = self.extract_address_key(current_address)

            if current_addr_key and current_addr_key in main_row_for_address:
                main_idx = main_row_for_address[current_addr_key]

                if i != main_idx:
                    current_order = str(processed_df.iloc[i, order_col]) if not pd.isna(processed_df.iloc[i, order_col]) else ""

                    if current_order and current_order != 'nan' and current_order.strip():
                        main_order = str(processed_df.iloc[main_idx, order_col]) if not pd.isna(processed_df.iloc[main_idx, order_col]) else ""
                        existing_orders = [o.strip() for o in main_order.split(',')] if main_order else []

                        if current_order not in existing_orders:
                            if main_order:
                                new_order = f"{main_order}, {current_order}"
                            else:
                                new_order = current_order

                            processed_df.iloc[main_idx, order_col] = new_order
                            logger.info(f"Номер {current_order} добавлен к главной строке {main_idx}")

                    if i not in rows_to_drop:
                        rows_to_drop.append(i)
                        logger.info(f"Строка {i} помечена на удаление")

        # Удаляем лишние строки
        if rows_to_drop:
            rows_to_drop.sort(reverse=True)
            for row_idx in rows_to_drop:
                processed_df = processed_df.drop(row_idx).reset_index(drop=True)
            logger.info(f"Удалено {len(rows_to_drop)} строк")

        # ========== ГЛАВНОЕ: КОПИРОВАНИЕ ИСПОЛНИТЕЛЕЙ ИЗ СТРОКИ ВЫШЕ ==========
        last_valid_executors = None
        for i in range(len(processed_df)):
            current_exec = processed_df.iloc[i, exec_col] if not pd.isna(processed_df.iloc[i, exec_col]) else ""

            if current_exec and str(current_exec).strip() and str(current_exec).strip().lower() != 'nan':
                last_valid_executors = current_exec
                logger.info(f"Строка {i}: запомнены исполнители '{last_valid_executors}'")
            else:
                if last_valid_executors:
                    processed_df.iloc[i, exec_col] = last_valid_executors
                    logger.info(f"✓ В строку {i} скопированы исполнители '{last_valid_executors}' (из строки выше)")
        # ==================================================================

        return processed_df
    
    def split_different_executors(self, df):
            """Разделяет похожие адреса с разными исполнителями"""
            if 'executors' not in self.found_columns or 'order_number' not in self.found_columns or 'address' not in self.found_columns:
                return df
        
            exec_col = self.found_columns['executors']
            order_col = self.found_columns['order_number']
            addr_col = self.found_columns['address']
            time_from_col = self.found_columns.get('time_from')
            time_to_col = self.found_columns.get('time_to')
        
            processed_df = df.copy()
        
            street_groups = {}
            for i in range(len(processed_df)):
                current_address = processed_df.iloc[i, addr_col] if not pd.isna(processed_df.iloc[i, addr_col]) else ""
                street_key = self.extract_street_key(current_address)
                if street_key:
                    if street_key not in street_groups:
                        street_groups[street_key] = []
                    street_groups[street_key].append(i)
        
            rows_to_drop = []
            rows_to_keep = []
        
            for street_key, rows in street_groups.items():
                if len(rows) <= 1:
                    continue
            
                rows_info = []
                for row_idx in rows:
                    exec_val = str(processed_df.iloc[row_idx, exec_col]).strip() if not pd.isna(processed_df.iloc[row_idx, exec_col]) else ""
                    address_val = str(processed_df.iloc[row_idx, addr_col]).strip() if not pd.isna(processed_df.iloc[row_idx, addr_col]) else ""
                    order_val = str(processed_df.iloc[row_idx, order_col]).strip() if not pd.isna(processed_df.iloc[row_idx, order_col]) else ""
                
                    time_from = ""
                    time_to = ""
                    if time_from_col and time_to_col:
                        time_from = str(processed_df.iloc[row_idx, time_from_col]).strip() if not pd.isna(processed_df.iloc[row_idx, time_from_col]) else ""
                        time_to = str(processed_df.iloc[row_idx, time_to_col]).strip() if not pd.isna(processed_df.iloc[row_idx, time_to_col]) else ""
                
                    is_fixa, fixa_time = self._check_fixa(time_from, time_to, address_val)
                
                    rows_info.append({
                        'row': row_idx,
                        'exec': exec_val if exec_val and exec_val != 'nan' else "",
                        'address': address_val,
                        'order': order_val if order_val and order_val != 'nan' else "",
                        'time_from': time_from,
                        'time_to': time_to,
                        'is_fixa': is_fixa,
                        'fixa_time': fixa_time
                    })
            
                rows_with_exec = [r for r in rows_info if r['exec']]
                rows_without_exec = [r for r in rows_info if not r['exec']]
            
                unique_execs = set(r['exec'] for r in rows_with_exec)
            
                if len(unique_execs) > 1:
                    logger.info(f"⚠️ Улица '{street_key}': найдены разные исполнители: {unique_execs}")
                
                    exec_groups = {}
                    for r in rows_with_exec:
                        if r['exec'] not in exec_groups:
                            exec_groups[r['exec']] = []
                        exec_groups[r['exec']].append(r)
                
                    for exec_val, group_rows in exec_groups.items():
                        merged_groups = []
                        used = set()
                    
                        for i, r1 in enumerate(group_rows):
                            if r1['row'] in used:
                                continue
                        
                            current_group = [r1]
                            used.add(r1['row'])
                        
                            for r2 in group_rows[i+1:]:
                                if r2['row'] in used:
                                    continue
                                if self.are_addresses_similar_for_merge(
                                r1['address'], r2['address'],
                                r1['fixa_time'], r2['fixa_time'],
                                r1['is_fixa'], r2['is_fixa']
                            ):
                                    current_group.append(r2)
                                    used.add(r2['row'])
                        
                            merged_groups.append(current_group)
                    
                        for group in merged_groups:
                            main_row = min(group, key=lambda x: x['row'])['row']
                        
                            all_orders = []
                            for r in group:
                                if r['order']:
                                    for order in r['order'].split(','):
                                        order = order.strip()
                                        if order and order not in all_orders:
                                            all_orders.append(order)
                        
                            if len(all_orders) > 1:
                                processed_df.iloc[main_row, order_col] = ", ".join(all_orders)
                                logger.info(f"  → В группе '{exec_val}' объединены номера: {', '.join(all_orders)}")
                        
                            rows_to_keep.append(main_row)
                        
                            for r in group:
                                if r['row'] != main_row:
                                    rows_to_drop.append(r['row'])
            
                else:
                    exec_val = next(iter(unique_execs)) if unique_execs else "без исполнителей"
                    logger.info(f"✅ Улица '{street_key}': все исполнители '{exec_val}', группируем по адресам")
                
                    all_rows = rows_with_exec + rows_without_exec
                    merged_groups = []
                    used = set()
                
                    for i, r1 in enumerate(all_rows):
                        if r1['row'] in used:
                            continue
                    
                        current_group = [r1]
                        used.add(r1['row'])
                    
                        for r2 in all_rows[i+1:]:
                            if r2['row'] in used:
                                continue
                            if self.are_addresses_similar_for_merge(
                                r1['address'], r2['address'],
                                r1['fixa_time'], r2['fixa_time'],
                                r1['is_fixa'], r2['is_fixa']
                            ):
                                current_group.append(r2)
                                used.add(r2['row'])
                    
                        merged_groups.append(current_group)
                
                    for group in merged_groups:
                        rows_with_exec_in_group = [r for r in group if r['exec']]
                        if rows_with_exec_in_group:
                            main_row = min(rows_with_exec_in_group, key=lambda x: x['row'])['row']
                        else:
                            main_row = min(group, key=lambda x: x['row'])['row']
                    
                        all_orders = []
                        for r in group:
                            if r['order']:
                                for order in r['order'].split(','):
                                    order = order.strip()
                                    if order and order not in all_orders:
                                        all_orders.append(order)
                    
                        if len(all_orders) > 1:
                            processed_df.iloc[main_row, order_col] = ", ".join(all_orders)
                            logger.info(f"  → Для группы адресов объединены номера: {', '.join(all_orders)}")
                    
                        rows_to_keep.append(main_row)
                    
                        for r in group:
                            if r['row'] != main_row:
                                rows_to_drop.append(r['row'])
        
            if rows_to_drop:
                rows_to_drop = list(set(rows_to_drop))
                rows_to_drop = [r for r in rows_to_drop if r not in rows_to_keep]
                rows_to_drop.sort(reverse=True)
            
            for row_idx in rows_to_drop:
                processed_df = processed_df.drop(row_idx).reset_index(drop=True)
            
            if rows_to_drop:
                logger.info(f"Доп. обработка: удалено {len(rows_to_drop)} строк")
        
            return processed_df
    
    def process_phone_fallback(self, df):
        """Заполняет телефоны из колонки водителя"""
        if 'phone' not in self.found_columns:
            return df
        
        phone_col = self.found_columns['phone']
        driver_col = self.found_columns.get('driver')
        
        for i in range(len(df)):
            current_phone = str(df.iloc[i, phone_col]) if not pd.isna(df.iloc[i, phone_col]) else ""
            
            if not current_phone or current_phone == 'nan' or current_phone.strip() == "":
                if driver_col is not None:
                    driver_value = str(df.iloc[i, driver_col]) if not pd.isna(df.iloc[i, driver_col]) else ""
                    if driver_value and driver_value != 'nan' and driver_value.strip():
                        df.iloc[i, phone_col] = driver_value.strip()
                        logger.info(f"Строка {i}: телефон заменен на {driver_value.strip()}")
                    else:
                        df.iloc[i, phone_col] = "Телефон не указан"
                else:
                    df.iloc[i, phone_col] = "Телефон не указан"
        
        return df
    
    def extract_data(self, df):
        """Извлекает все данные из найденных колонок"""
        found = self.find_columns(df)
        
        if not found:
            return None, "Не найдено ни одной знакомой колонки"
        
        self.df = df.copy()
        self.original_df = df.copy()
        
        columns_to_convert = []
        
        for data_type, col_idx in found.items():
            columns_to_convert.append(col_idx)
        
        for col_idx in set(columns_to_convert):
            if col_idx < len(df.columns):
                df.iloc[:, col_idx] = df.iloc[:, col_idx].astype(str)
        
        logger.info(f"Преобразовано {len(set(columns_to_convert))} колонок в строки")
        
        if 'order_number' in found:
            order_col_idx = found['order_number']
            order_col_name = df.columns[order_col_idx]
            df[order_col_name] = df[order_col_name].apply(lambda x: self.preserve_order_number(x))
        
        try:
            processed_df = df.copy()
            
            try:
                processed_df = self.split_different_executors(processed_df)
                logger.info(f"После доп. обработки (разделение исполнителей): {len(processed_df)} строк")
            except Exception as e:
                logger.error(f"Ошибка в доп. обработке split_different_executors: {e}")
                processed_df = df.copy()
            
            processed_df = self.process_merged_cells(processed_df)
            logger.info(f"После обработки объединений (стабильная версия): {len(processed_df)} строк")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке: {e}")
            processed_df = df.copy()
        
        try:
            processed_df = self.process_phone_fallback(processed_df)
        except Exception as e:
            logger.error(f"Ошибка при обработке телефонов: {e}")
        
        has_time = 'time_from' in found and 'time_to' in found
        if has_time:
            logger.info("Найдены колонки времени, будет выполняться проверка фикс")
        
        for key in self.data:
            self.data[key] = []
        
        for idx in range(len(processed_df)):
            row_data = {}
            has_data = False
            
            for data_type, col_idx in found.items():
                if col_idx < len(processed_df.columns):
                    value = processed_df.iloc[idx, col_idx]
                    str_value = str(value) if pd.notna(value) else ""
                    if str_value and str_value != 'nan':
                        if data_type == 'order_number':
                            row_data[data_type] = self.preserve_order_number(str_value)
                        else:
                            row_data[data_type] = str_value.strip()
                        has_data = True
                    else:
                        row_data[data_type] = ""
            
            if has_data:
                for key in self.data:
                    if key in ['has_fixa', 'fixa_time']:
                        continue
                    self.data[key].append(row_data.get(key, ""))
                
                if has_time:
                    time_from_val = row_data.get('time_from', '')
                    time_to_val = row_data.get('time_to', '')
                    addr_val = row_data.get('address', '')
                    addr_key = self.extract_address_key(addr_val) if addr_val else ""
                    is_fixa, fixa_time = self._check_fixa(time_from_val, time_to_val, addr_key)
                    self.data['has_fixa'].append(is_fixa)
                    self.data['fixa_time'].append(fixa_time if is_fixa else "")
                else:
                    self.data['has_fixa'].append(False)
                    self.data['fixa_time'].append("")
        
        logger.info(f"После обработки: {len(self.data['order_number'])} записей")
        
        exec_count = sum(1 for x in self.data['executors'] if x and x != 'Исполнители не указаны' and x != '')
        logger.info(f"Записей с исполнителями: {exec_count}")
        
        fixa_count = sum(1 for x in self.data['has_fixa'] if x)
        logger.info(f"Записей с фиксой: {fixa_count}")
        
        return self.data, "OK"
    
    def group_by_city(self, data):
        """Группирует заявки по городам"""
        if 'city' not in self.found_columns:
            return {'Все заявки': data}
        
        city_data = {}
        
        for i in range(len(data['order_number'])):
            if not data['order_number'][i]:
                continue
            
            city = data['city'][i] if data['city'][i] else "Город не указан"
            
            if city not in city_data:
                city_data[city] = {
                    'order_number': [],
                    'address': [],
                    'executors': [],
                    'phone': [],
                    'driver': [],
                    'city': [],
                    'has_fixa': [],
                    'fixa_time': []
                }
                logger.info(f"Создана группа для города: '{city}'")
            
            for key in city_data[city]:
                if key in data:
                    city_data[city][key].append(data[key][i])
        
        logger.info(f"Найдены города: {list(city_data.keys())}")
        return city_data
    
    def format_main_result(self, data):
        """Форматирует результат"""
        result_lines = []
        
        city_groups = self.group_by_city(data)
        sorted_cities = sorted(city_groups.keys())
        
        logger.info(f"Найдено городов для отображения: {len(sorted_cities)}")
        
        for city_idx, city in enumerate(sorted_cities):
            city_data = city_groups[city]
            
            if not city_data['order_number']:
                continue
            
            if city_idx > 0:
                result_lines.append("")
            
            result_lines.append(f"📍 **{city}**")
            result_lines.append("")
            
            city_counter = 1
            
            for i in range(len(city_data['order_number'])):
                if not city_data['order_number'][i]:
                    continue
                
                order_numbers = city_data['order_number'][i].strip()
                address = city_data['address'][i] if i < len(city_data['address']) and city_data['address'][i] else "Адрес не указан"
                executors = city_data['executors'][i] if i < len(city_data['executors']) and city_data['executors'][i] else "Исполнители не указаны"
                phone = city_data['phone'][i] if i < len(city_data['phone']) and city_data['phone'][i] else "Телефон не указан"
                has_fixa = city_data.get('has_fixa', [False])[i] if i < len(city_data.get('has_fixa', [])) else False
                fixa_time = city_data.get('fixa_time', [''])[i] if i < len(city_data.get('fixa_time', [])) else ""
                
                entry_lines = []
                entry_lines.append(f"{city_counter}) {order_numbers}")
                
                if has_fixa and fixa_time:
                    entry_lines.append(f"Фикса: {fixa_time}")
                
                entry_lines.append(address)
                entry_lines.append(executors)
                
                if phone and phone != "Телефон не указан" and phone != "Нет телефона":
                    if self.is_phone_number(phone):
                        clean_phone = self.format_phone_number(phone)
                        entry_lines.append(clean_phone)
                    else:
                        entry_lines.append("Телефон не указан")
                        entry_lines.append(phone)
                else:
                    entry_lines.append("Телефон не указан")
                
                result_lines.extend(entry_lines)
                
                if i < len(city_data['order_number']) - 1:
                    result_lines.append("")
                
                city_counter += 1
            
            logger.info(f"Город '{city}' обработан: {city_counter-1} заявок")
        
        return "\n".join(result_lines)
    
    def get_all_columns_info(self):
        info = []
        for key, col_idx in self.found_columns.items():
            info.append(f"• {key}: найдена (индекс {col_idx})")
        return "\n".join(info)
    
    def get_processing_stats(self):
        if self.original_df is not None and self.df is not None:
            return {
                'original_rows': len(self.original_df),
                'processed_rows': len(self.df),
                'merged_count': len(self.original_df) - len(self.df)
            }
        return None

def apply_corrections(original_data, corrections):
    """Применяет исправления к данным"""
    corrected = {key: list(value) for key, value in original_data.items()}
    
    for corr in corrections:
        for i, order in enumerate(corrected['order_number']):
            order_str = str(order)
            corr_numbers = corr['order_number'].split(',')
            
            match = False
            for num in corr_numbers:
                if num.strip() in order_str:
                    match = True
                    break
            
            if match:
                corrected['address'][i] = corr['address']
                corrected['executors'][i] = corr['executors']
                corrected['phone'][i] = corr['phone']
                if corr['order_number'] != order_str:
                    corrected['order_number'][i] = corr['order_number']
                break
    
    return corrected

async def show_correction_result(update, context, original_data, corrected_data, old_message_id, file_name, processor, success_message):
    """Показывает результат исправления"""
    user_id = update.effective_user.id
    
    # Определяем, откуда пришёл вызов
    if hasattr(update, 'callback_query') and update.callback_query:
        # Это callback query - используем callback_query.message.chat
        chat_id = update.callback_query.message.chat_id
        message_obj = update.callback_query.message
    else:
        # Это обычное сообщение
        chat_id = update.effective_chat.id
        message_obj = update.message
    
    new_result = processor.format_main_result(corrected_data)
    
    # Удаляем старое сообщение с результатом
    try:
        await context.bot.delete_message(
            chat_id=chat_id,
            message_id=old_message_id
        )
    except Exception as e:
        logger.error(f"Не удалось удалить сообщение: {e}")
    
    result_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Исправить", callback_data="edit")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("🗑️ Удалить", callback_data="del")]
    ])
    
    # Отправляем новый результат (это не служебное, не удаляем)
    if len(new_result) > 4000:
        parts = [new_result[i:i+4000] for i in range(0, len(new_result), 4000)]
        for i, part in enumerate(parts, 1):
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"📋 **Исправленный результат (часть {i}/{len(parts)}):**\n\n{part}",
                parse_mode='Markdown',
                reply_markup=result_keyboard if i == 1 else None
            )
            if 'result_messages' not in context.chat_data:
                context.chat_data['result_messages'] = []
            context.chat_data['result_messages'].append(msg.message_id)
    else:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"📋 **Исправленный результат:**\n\n{new_result}",
            parse_mode='Markdown',
            reply_markup=result_keyboard
        )
        if 'result_messages' not in context.chat_data:
            context.chat_data['result_messages'] = []
        context.chat_data['result_messages'].append(msg.message_id)
    
    context.chat_data['last_data'] = corrected_data
    context.chat_data['last_formatted_result'] = new_result
    
    if user_id in user_edit_state:
        del user_edit_state[user_id]
    
    # Отправляем подтверждение (служебное - удалим через 60 сек)
    confirm_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=success_message
    )
    asyncio.create_task(delete_message_after_delay(context, chat_id, confirm_msg.message_id, MESSAGE_DELETE_DELAY))

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню с кнопками управления"""
    file_name = context.chat_data.get('last_file_name', None)
    
    msg = await send_with_retry(
        update.message.reply_text,
        "📋 **Меню управления:**\n\nВыберите действие:",
        parse_mode='Markdown',
        reply_markup=get_menu_keyboard(file_name)
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def delete_last_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет последнее сообщение с результатом"""
    try:
        chat_id = update.effective_chat.id
        
        if 'result_messages' not in context.chat_data or not context.chat_data['result_messages']:
            msg = await send_with_retry(
                update.message.reply_text,
                "❌ Нет сообщений с результатами для удаления"
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        last_msg_id = context.chat_data['result_messages'].pop()
        
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
            msg = await send_with_retry(
                update.message.reply_text,
                "✅ Последний результат удалён"
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            
        except Exception as e:
            logger.error(f"Ошибка при удалении: {e}")
            msg = await send_with_retry(
                update.message.reply_text,
                f"❌ Не удалось удалить: {str(e)[:200]}"
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            
    except Exception as e:
        logger.error(f"Ошибка в команде /del: {e}")
        msg = await send_with_retry(
            update.message.reply_text,
            f"❌ Ошибка: {str(e)[:200]}"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def show_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает сохранённые ID результатов"""
    chat_id = update.effective_chat.id
    
    if 'result_messages' in context.chat_data:
        msg = await send_with_retry(
            update.message.reply_text,
            f"📝 ID результатов: {context.chat_data['result_messages']}"
        )
    else:
        msg = await send_with_retry(
            update.message.reply_text,
            "❌ Нет сохранённых результатов"
        )
    
    asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки меню"""
    query = update.callback_query
    await query.answer()
    
    action = query.data
    chat_id = update.effective_chat.id
    
    if action == "stats":
        if 'last_data' not in context.chat_data or 'last_processor' not in context.chat_data:
            back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back")]])
            msg = await query.edit_message_text(
                "❌ Нет данных для статистики. Сначала отправьте файл.",
                reply_markup=back_keyboard
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        data = context.chat_data['last_data']
        processor = context.chat_data['last_processor']
        file_name = context.chat_data.get('last_file_name', 'файл')
        
        total_orders = sum(1 for x in data['order_number'] if x)
        fixa_count = sum(1 for x in data['has_fixa'] if x)
        proc_stats = processor.get_processing_stats()
        
        stats = (
            f"📊 **Статистика по файлу:** `{file_name}`\n\n"
            f"• Всего записей с номерами: {total_orders}\n"
            f"• Найдено адресов: {sum(1 for x in data['address'] if x)}\n"
            f"• Найдено исполнителей: {sum(1 for x in data['executors'] if x)}\n"
            f"• Найдено телефонов: {sum(1 for x in data['phone'] if x)}\n"
            f"• Найдено городов: {len(set(data['city'])) if 'city' in data else 0}\n"
            f"• Записей с фиксой: {fixa_count}\n"
        )
        
        if proc_stats:
            stats += (
                f"\n📈 **Статистика обработки:**\n"
                f"• Исходных строк: {proc_stats['original_rows']}\n"
                f"• После обработки: {proc_stats['processed_rows']}\n"
                f"• Объединено строк: {proc_stats['merged_count']}\n"
            )
        
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад в меню", callback_data="back")]
        ])
        
        msg = await query.edit_message_text(
            stats,
            parse_mode='Markdown',
            reply_markup=back_keyboard
        )
        asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
        
    elif action == "del":
        if 'result_messages' not in context.chat_data or not context.chat_data['result_messages']:
            back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back")]])
            msg = await query.edit_message_text(
                "❌ Нет результатов для удаления",
                reply_markup=back_keyboard
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        last_msg_id = context.chat_data['result_messages'].pop()
        
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_msg_id)
            back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back")]])
            msg = await query.edit_message_text(
                "✅ Последний результат удалён",
                reply_markup=back_keyboard
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
        except Exception as e:
            logger.error(f"Ошибка при удалении: {e}")
            back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back")]])
            msg = await query.edit_message_text(
                f"❌ Не удалось удалить",
                reply_markup=back_keyboard
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
            
    elif action == "help":
        help_text = """
        📚 **Доступные команды:**
        
        /start - Начать работу
        /help - Показать это сообщение
        /menu - Открыть меню с кнопками
        /columns - Показать все возможные названия колонок
        /stats - Статистика по последнему файлу
        /last - Информация о последнем файле
        /queue - Показать состояние очереди
        /del - Удалить последний результат
        /show - Показать сохранённые ID результатов
        
        **📤 Как отправлять файлы:**
        Просто прикрепите Excel файл (.xlsx, .xls, .xlsm)
        """
        
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад в меню", callback_data="back")]
        ])
        
        msg = await query.edit_message_text(
            help_text,
            parse_mode='Markdown',
            reply_markup=back_keyboard
        )
        asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
        
    elif action == "back":
        # Возвращаемся к результату обработки
        if 'last_formatted_result' in context.chat_data and 'last_file_name' in context.chat_data:
            formatted_result = context.chat_data['last_formatted_result']
            file_name = context.chat_data['last_file_name']
        
        # Удаляем сообщение со статистикой
        await query.message.delete()
        
        # Отправляем результат с названием файла в заголовке
        if len(formatted_result) > 4000:
            parts = [formatted_result[i:i+4000] for i in range(0, len(formatted_result), 4000)]
            for i, part in enumerate(parts, 1):
                msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📋 **Результат обработки: `{file_name}` (часть {i}/{len(parts)}):**\n\n{part}",
                    parse_mode='Markdown',
                    reply_markup=get_menu_keyboard(file_name) if i == 1 else None
                )
                if 'result_messages' not in context.chat_data:
                    context.chat_data['result_messages'] = []
                context.chat_data['result_messages'].append(msg.message_id)
            else:
                msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📋 **Результат обработки: `{file_name}`**\n\n{formatted_result}",
                    parse_mode='Markdown',
                    reply_markup=get_menu_keyboard(file_name)
                )
                if 'result_messages' not in context.chat_data:
                    context.chat_data['result_messages'] = []
                context.chat_data['result_messages'].append(msg.message_id)
        else:
            # Если результата нет, показываем просто меню
            file_name = context.chat_data.get('last_file_name', None)
            msg = await query.edit_message_text(
                "📋 **Меню управления:**\n\nВыберите действие:",
                parse_mode='Markdown',
                reply_markup=get_menu_keyboard(file_name)
            )
            asyncio.create_task(delete_message_after_delay(context, chat_id, msg.message_id, MESSAGE_DELETE_DELAY))
    
    elif action.startswith("merge_street_"):
        street_key = action.replace("merge_street_", "")
        await handle_merge_street(update, context, street_key)
    
    elif action == "simple_merge":
        await start_simple_merge(update, context)
    elif action == "simple_delete":
        await start_simple_delete(update, context)
    elif action == "simple_split":
        await start_simple_split(update, context)
    elif action == "simple_add":  # Новая кнопка
        await start_simple_add(update, context)
    elif action == "custom_edit":
        await start_custom_edit(update, context)
    elif action == "cancel_edit":
        user_id = update.effective_user.id
        if user_id in user_edit_state:
            del user_edit_state[user_id]
        if user_id in user_command_state:
            del user_command_state[user_id]
        
        await query.message.delete()
        
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Редактирование отменено"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
    
    elif action == "edit":
        await start_edit_mode(update, context)
    
    elif action in ["confirm_merge", "confirm_delete", "confirm_split", "confirm_add", "cancel_merge", "cancel_delete", "cancel_split", "cancel_add"]:
        await handle_command_confirmation(update, context)

async def start_edit_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запускает режим редактирования"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    message_id = query.message.message_id
    
    if 'last_data' in context.chat_data and 'last_processor' in context.chat_data:
        street_key = None
        if 'last_formatted_result' in context.chat_data:
            lines = context.chat_data['last_formatted_result'].split('\n')
            for line in lines:
                if line.startswith('📍'):
                    street_key = line.replace('📍', '').strip()
                    break
        
        user_edit_state[user_id] = {
            'message_id': message_id,
            'data': context.chat_data['last_data'],
            'processor': context.chat_data['last_processor'],
            'file_name': context.chat_data.get('last_file_name'),
            'street_key': street_key
        }
        
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📝 **Что нужно исправить?**\n\nВыберите действие:",
            parse_mode='Markdown',
            reply_markup=get_edit_keyboard(street_key)
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
    else:
        msg = await query.edit_message_text(
            "❌ Нет данных для редактирования",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back")]])
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def start_custom_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запускает режим ручного редактирования"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    if user_id not in user_edit_state:
        msg = await query.edit_message_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    user_edit_state[user_id]['waiting_for_input'] = True
    
    # Удаляем сообщение с меню
    await query.message.delete()
    
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📝 **Режим ручного редактирования**\n\n"
        "Отправьте мне правильный вариант. Можно использовать:\n\n"
        "🔹 **Простые команды:**\n"
        "• `объединить 16 и 17`\n"
        "• `удалить 17`\n"
        "• `разделить 16` - разделить все номера\n"
        "• `разделить 16: 00310043732, 00310043983` - разделить конкретные номера\n\n"
        "🔹 **Полный формат (с / или |):**\n"
        "`НОМЕР_ЗАЯВКИ / АДРЕС / ИСПОЛНИТЕЛИ / ТЕЛЕФОН`\n\n"
        "🔹 **С указанием фиксы:**\n"
        "`НОМЕР_ЗАЯВКИ / Фикса: ВРЕМЯ / АДРЕС / ИСПОЛНИТЕЛИ / ТЕЛЕФОН`\n\n"
        "**Примеры:**\n"
        "`16) 00310043732,00310043983 / Красноярск г, Телевизорная ул, д. 1, стр. 78 / 1 чел / +79836101547`\n"
        "`16) 00310043732,00310043983 / Фикса: 09:30 / Красноярск г, Телевизорная ул, д. 1, стр. 78 / 1 чел / +79836101547`\n\n"
        "Можно исправить несколько заявок сразу (каждая с новой строки)\n"
        "Или напишите `отмена` для выхода",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def start_simple_merge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс объединения заявок"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if user_id not in user_edit_state:
        msg = await query.edit_message_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    user_command_state[user_id] = {
        'action': 'merge',
        'step': 1,
        'message_id': query.message.message_id
    }
    
    msg = await query.edit_message_text(
        "📝 **Объединение заявок**\n\n"
        "Напишите **номера заявок из списка**, которые нужно объединить:\n\n"
        "Примеры:\n"
        "• `16 и 17`\n"
        "• `16,17,18`\n"
        "• `16+17+18`\n\n"
        "Или напишите `отмена` для выхода",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def start_simple_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс удаления заявки"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if user_id not in user_edit_state:
        msg = await query.edit_message_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    user_command_state[user_id] = {
        'action': 'delete',
        'step': 1,
        'message_id': query.message.message_id
    }
    
    msg = await query.edit_message_text(
        "📝 **Удаление заявки**\n\n"
        "Напишите **номер заявки из списка**, которую нужно удалить:\n\n"
        "Примеры:\n"
        "• `17`\n"
        "• `16,17,18`\n\n"
        "Или напишите `отмена` для выхода",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def start_simple_split(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс разделения заявки"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if user_id not in user_edit_state:
        msg = await query.edit_message_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    user_command_state[user_id] = {
        'action': 'split',
        'step': 1,
        'message_id': query.message.message_id
    }
    
    msg = await query.edit_message_text(
        "📝 **Разделение заявок**\n\n"
        "Напишите, что нужно разделить:\n\n"
        "🔹 **Разделить все номера в заявке:**\n"
        "• `16` - разделить заявку 16 на отдельные\n\n"
        "🔹 **Разделить конкретные номера:**\n"
        "• `16: 00310043732, 00310043983` - разделить только указанные номера\n"
        "• `16: 00310043732` - выделить один номер в отдельную заявку\n\n"
        "Или напишите `отмена` для выхода",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

# Новая функция для добавления заявки
async def start_simple_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс добавления новой заявки"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if user_id not in user_edit_state:
        msg = await query.edit_message_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    user_command_state[user_id] = {
        'action': 'add',
        'step': 1,
        'message_id': query.message.message_id
    }
    
    await query.message.delete()
    
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📝 **Добавление новой заявки**\n\n"
        "Напишите новую заявку в формате:\n\n"
        "🔹 **Без фиксы:**\n"
        "`18) 00310012345 / Адрес / 2 чел / +79123456789`\n\n"
        "🔹 **С фиксой:**\n"
        "`18) 00310012345 / Фикса: 14:30 / Адрес / 2 чел / +79123456789`\n\n"
        "Заявка будет добавлена под указанным номером, все последующие автоматически перенумеруются.\n\n"
        "Или напишите `отмена` для выхода",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_custom_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текст для ручного редактирования"""
    user_id = update.effective_user.id
    
    if user_id not in user_edit_state or not user_edit_state[user_id].get('waiting_for_input'):
        return False
    
    text = update.message.text.strip().lower()
    logger.info(f"Ручное редактирование от пользователя {user_id}: {text}")
    
    if text == 'отмена':
        if user_id in user_edit_state:
            del user_edit_state[user_id]
        msg = await update.message.reply_text("❌ Редактирование отменено")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    edit_info = user_edit_state[user_id]
    original_data = edit_info['data']
    message_id = edit_info['message_id']
    file_name = edit_info.get('file_name')
    processor = edit_info['processor']
    
    if text.startswith('объединить'):
        await handle_merge_command(update, context, text, original_data, message_id, file_name, processor)
        return True
    
    elif text.startswith('удалить'):
        await handle_delete_command(update, context, text, original_data, message_id, file_name, processor)
        return True
    
    elif text.startswith('разделить'):
        await handle_split_command(update, context, text, original_data, message_id, file_name, processor)
        return True
    
    else:
        await handle_format_input(update, context, text, original_data, message_id, file_name, processor)
        return True

async def handle_merge_command(update, context, text, original_data, message_id, file_name, processor):
    """Обрабатывает команду объединения"""
    import re
    numbers = re.findall(r'\d+', text)
    
    if len(numbers) < 2:
        msg = await update.message.reply_text(
            "❌ Неправильный формат. Используйте:\n"
            "`объединить 16 и 17`\n"
            "`объединить 16,17,18`"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    target = numbers[0]
    sources = numbers[1:]
    
    target_idx = int(target) - 1
    source_indices = [int(s) - 1 for s in sources]
    
    total_orders = len(original_data['order_number'])
    
    # Проверяем границы
    if target_idx < 0 or target_idx >= total_orders:
        msg = await update.message.reply_text(f"❌ Заявка с номером {target} не найдена")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    invalid_indices = [i for i in source_indices if i < 0 or i >= total_orders]
    if invalid_indices:
        msg = await update.message.reply_text(f"❌ Некоторые заявки не найдены")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    # Показываем предпросмотр
    preview = "🔍 **Предпросмотр изменений:**\n\n"
    preview += "**Будут объединены:**\n"
    preview += f"• Заявка {target}: {original_data['order_number'][target_idx]}\n"
    for i, idx in enumerate(source_indices):
        preview += f"• Заявка {sources[i]}: {original_data['order_number'][idx]}\n"
    
    # Собираем все номера
    all_numbers = [str(original_data['order_number'][target_idx])]
    for idx in source_indices:
        all_numbers.append(str(original_data['order_number'][idx]))
    
    # Объединяем уникальные номера
    combined = []
    for num_str in all_numbers:
        for num in num_str.replace(' ', '').split(','):
            if num not in combined:
                combined.append(num)
    
    new_numbers = ', '.join(combined)
    
    preview += f"\n**Результат в заявке {target}:**\n"
    preview += f"• {new_numbers}\n"
    preview += f"\n*Заявки {', '.join(map(str, sources))} будут удалены*"
    
    user_command_state[update.effective_user.id] = {
        'action': 'merge_confirm',
        'target_idx': target_idx,
        'source_indices': source_indices,
        'new_numbers': new_numbers,
        'display_numbers': numbers
    }
    
    confirm_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Применить", callback_data="confirm_merge")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_merge")]
    ])
    
    msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_delete_command(update, context, text, original_data, message_id, file_name, processor):
    """Обрабатывает команду удаления"""
    import re
    numbers = re.findall(r'\d+', text)
    
    if not numbers:
        msg = await update.message.reply_text(
            "❌ Неправильный формат. Используйте:\n"
            "`удалить 17`\n"
            "`удалить 16,17,18`"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    indices = [int(n) - 1 for n in numbers]
    total_orders = len(original_data['order_number'])
    
    invalid_indices = [i for i in indices if i < 0 or i >= total_orders]
    if invalid_indices:
        msg = await update.message.reply_text(f"❌ Некоторые заявки не найдены")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    preview = "🔍 **Предпросмотр изменений:**\n\n"
    preview += "**Будут удалены:**\n"
    for i, idx in enumerate(indices):
        preview += f"• Заявка {numbers[i]}: {original_data['order_number'][idx]}\n"
    
    user_command_state[update.effective_user.id] = {
        'action': 'delete_confirm',
        'indices': indices,
        'numbers': numbers
    }
    
    confirm_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Применить", callback_data="confirm_delete")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")]
    ])
    
    msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_split_command(update, context, text, original_data, message_id, file_name, processor):
    """Обрабатывает команду разделения"""
    import re
    numbers = re.findall(r'\d+', text)
    
    if not numbers:
        msg = await update.message.reply_text(
            "❌ Неправильный формат. Используйте:\n"
            "`разделить 16` - разделить все номера\n"
            "`разделить 16: 00310043732, 00310043983` - разделить конкретные номера"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    num = numbers[0]
    idx = int(num) - 1
    
    total_orders = len(original_data['order_number'])
    
    if idx < 0 or idx >= total_orders:
        msg = await update.message.reply_text(f"❌ Заявка с номером {num} не найдена")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    order_str = str(original_data['order_number'][idx])
    all_orders = order_str.split(', ')
    
    # Проверяем, есть ли двоеточие (значит указаны конкретные номера)
    if ':' in text:
        # Извлекаем номера после двоеточия
        parts = text.split(':')
        if len(parts) > 1:
            specific_numbers = [n.strip() for n in parts[1].replace(',', ' ').split() if n.strip()]
            
            # Проверяем, что все указанные номера есть в заявке
            not_found = [n for n in specific_numbers if n not in all_orders]
            if not_found:
                msg = await update.message.reply_text(
                    f"❌ Номера {', '.join(not_found)} не найдены в заявке {num}"
                )
                asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                return
            
            # Разделяем: оставляем в исходной заявке остальные номера
            remaining = [n for n in all_orders if n not in specific_numbers]
            
            if not remaining:
                msg = await update.message.reply_text(
                    f"❌ Нельзя разделить все номера. Используйте `{num}` для полного разделения"
                )
                asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                return
            
            preview = "🔍 **Предпросмотр изменений:**\n\n"
            preview += f"**Из заявки {num} будут выделены:**\n"
            for n in specific_numbers:
                preview += f"• {n}\n"
            preview += f"\n**В заявке {num} останутся:**\n"
            preview += f"• {', '.join(remaining)}\n"
            
            user_command_state[update.effective_user.id] = {
                'action': 'split_specific_confirm',
                'idx': idx,
                'num': num,
                'remaining': remaining,
                'specific': specific_numbers
            }
    else:
        # Полное разделение всех номеров
        if len(all_orders) <= 1:
            msg = await update.message.reply_text(f"❌ Заявка {num} уже разделена (содержит только один номер)")
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        preview = "🔍 **Предпросмотр изменений:**\n\n"
        preview += f"**Заявка {num} будет разделена на {len(all_orders)} частей:**\n"
        for j, ord_num in enumerate(all_orders, 1):
            preview += f"{j}) {ord_num}\n"
        
        user_command_state[update.effective_user.id] = {
            'action': 'split_all_confirm',
            'idx': idx,
            'num': num,
            'orders': all_orders
        }
    
    confirm_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Применить", callback_data="confirm_split")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_split")]
    ])
    
    msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_format_input(update, context, text, original_data, message_id, file_name, processor):
    """Обрабатывает ввод в формате с разделителями"""
    lines = text.split('\n')
    corrections = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        parts = None
        if '|' in line:
            parts = line.split('|')
        elif '/' in line:
            parts = line.split('/')
        
        if not parts:
            msg = await update.message.reply_text(
                f"❌ Ошибка в строке: {line}\n"
                f"Используйте / или | как разделитель"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        try:
            if len(parts) == 4:
                first_part = parts[0].strip()
                if ') ' in first_part:
                    order_num = first_part.split(') ')[1]
                elif ')' in first_part:
                    order_num = first_part.split(')')[1].strip()
                else:
                    order_num = first_part
                
                order_num = re.sub(r'[^\d,]', '', order_num)
                
                correction = {
                    'order_number': order_num,
                    'address': parts[1].strip(),
                    'executors': parts[2].strip(),
                    'phone': parts[3].strip()
                }
                corrections.append(correction)
                
            elif len(parts) == 5:
                first_part = parts[0].strip()
                if ') ' in first_part:
                    order_num = first_part.split(') ')[1]
                elif ')' in first_part:
                    order_num = first_part.split(')')[1].strip()
                else:
                    order_num = first_part
                
                order_num = re.sub(r'[^\d,]', '', order_num)
                
                fixa_part = parts[1].strip()
                fixa_time = re.sub(r'[^\d:]', '', fixa_part)
                
                address = f"{fixa_part}\n{parts[2].strip()}"
                
                correction = {
                    'order_number': order_num,
                    'address': address,
                    'executors': parts[3].strip(),
                    'phone': parts[4].strip()
                }
                corrections.append(correction)
            else:
                msg = await update.message.reply_text(
                    f"❌ Ошибка в строке: {line}\n"
                    f"Должно быть 4 части (без фиксы) или 5 частей (с фиксой)\n"
                    f"Пример без фиксы: 16) 00310043732 / Адрес / 1 чел / +79836101547\n"
                    f"Пример с фиксой: 16) 00310043732 / Фикса: 09:30 / Адрес / 1 чел / +79836101547"
                )
                asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                return
                
        except Exception as e:
            msg = await update.message.reply_text(f"❌ Ошибка в строке: {line}\n{str(e)}")
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return
    
    if corrections:
        corrected_data = apply_corrections(original_data, corrections)
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    "✅ Исправления применены!")

async def handle_simple_command_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод для простых команд"""
    user_id = update.effective_user.id
    
    if user_id not in user_command_state:
        return False
    
    state = user_command_state[user_id]
    text = update.message.text.strip()
    logger.info(f"Простая команда от пользователя {user_id}: {text}, действие: {state['action']}")
    
    if text.lower() == 'отмена':
        if user_id in user_command_state:
            del user_command_state[user_id]
        msg = await update.message.reply_text("❌ Операция отменена")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    if user_id not in user_edit_state:
        msg = await update.message.reply_text("❌ Сессия редактирования истекла")
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    edit_info = user_edit_state[user_id]
    original_data = edit_info['data']
    
    # Извлекаем все числа из текста (это порядковые номера заявок)
    import re
    numbers = re.findall(r'\d+', text)
    
    if state['action'] == 'merge':
        if len(numbers) < 2:
            msg = await update.message.reply_text(
                "❌ Нужно указать минимум 2 номера. Например: `16 и 17`",
                parse_mode='Markdown'
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        # Проверяем, что все номера в пределах списка
        total_orders = len(original_data['order_number'])
        invalid_numbers = [n for n in numbers if int(n) < 1 or int(n) > total_orders]
        
        if invalid_numbers:
            msg = await update.message.reply_text(
                f"❌ Номера {', '.join(map(str, invalid_numbers))} выходят за пределы списка (всего {total_orders} заявок)"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        # Преобразуем в индексы (пользовательский номер 16 -> индекс 15)
        indices = [int(n) - 1 for n in numbers]
        
        target_idx = indices[0]
        source_indices = indices[1:]
        
        # Показываем предпросмотр
        preview = "🔍 **Предпросмотр изменений:**\n\n"
        preview += "**Будут объединены:**\n"
        preview += f"• Заявка {numbers[0]}: {original_data['order_number'][target_idx]}\n"
        for i, idx in enumerate(source_indices):
            preview += f"• Заявка {numbers[i+1]}: {original_data['order_number'][idx]}\n"
        
        # Собираем все номера ЗЭ
        all_numbers = [str(original_data['order_number'][target_idx])]
        for idx in source_indices:
            all_numbers.append(str(original_data['order_number'][idx]))
        
        # Объединяем уникальные номера
        combined = []
        for num_str in all_numbers:
            for num in num_str.replace(' ', '').split(','):
                if num not in combined:
                    combined.append(num)
        
        new_numbers = ', '.join(combined)
        
        preview += f"\n**Результат в заявке {numbers[0]}:**\n"
        preview += f"• {new_numbers}\n"
        preview += f"\n*Заявки {', '.join(numbers[1:])} будут удалены*"
        
        user_command_state[user_id] = {
            'action': 'merge_confirm',
            'target_idx': target_idx,
            'source_indices': source_indices,
            'new_numbers': new_numbers,
            'display_numbers': numbers
        }
        
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Применить", callback_data="confirm_merge")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_merge")]
        ])
        
        msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    elif state['action'] == 'delete':
        if not numbers:
            msg = await update.message.reply_text(
                "❌ Укажите номер заявки. Например: `17`"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        total_orders = len(original_data['order_number'])
        invalid_numbers = [n for n in numbers if int(n) < 1 or int(n) > total_orders]
        
        if invalid_numbers:
            msg = await update.message.reply_text(
                f"❌ Номера {', '.join(map(str, invalid_numbers))} выходят за пределы списка (всего {total_orders} заявок)"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        indices = [int(n) - 1 for n in numbers]
        
        preview = "🔍 **Предпросмотр изменений:**\n\n"
        preview += "**Будут удалены:**\n"
        for i, idx in enumerate(indices):
            preview += f"• Заявка {numbers[i]}: {original_data['order_number'][idx]}\n"
        
        user_command_state[user_id] = {
            'action': 'delete_confirm',
            'indices': indices,
            'numbers': numbers
        }
        
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Применить", callback_data="confirm_delete")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")]
        ])
        
        msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    elif state['action'] == 'split':
        if not numbers:
            msg = await update.message.reply_text(
                "❌ Укажите номер заявки. Например: `16` или `16: 00310043732, 00310043983`"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        num = numbers[0]
        idx = int(num) - 1
        
        if idx < 0 or idx >= len(original_data['order_number']):
            msg = await update.message.reply_text(f"❌ Заявка с номером {num} не найдена")
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        order_str = str(original_data['order_number'][idx])
        all_orders = order_str.split(', ')
        
        # Проверяем, есть ли двоеточие (значит указаны конкретные номера)
        if ':' in text:
            # Извлекаем номера после двоеточия
            parts = text.split(':')
            if len(parts) > 1:
                specific_numbers = [n.strip() for n in parts[1].replace(',', ' ').split() if n.strip()]
                
                # Проверяем, что все указанные номера есть в заявке
                not_found = [n for n in specific_numbers if n not in all_orders]
                if not_found:
                    msg = await update.message.reply_text(
                        f"❌ Номера {', '.join(not_found)} не найдены в заявке {num}"
                    )
                    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                    return True
                
                # Разделяем: оставляем в исходной заявке остальные номера
                remaining = [n for n in all_orders if n not in specific_numbers]
                
                if not remaining:
                    msg = await update.message.reply_text(
                        f"❌ Нельзя разделить все номера. Используйте `{num}` для полного разделения"
                    )
                    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                    return True
                
                preview = "🔍 **Предпросмотр изменений:**\n\n"
                preview += f"**Из заявки {num} будут выделены:**\n"
                for n in specific_numbers:
                    preview += f"• {n}\n"
                preview += f"\n**В заявке {num} останутся:**\n"
                preview += f"• {', '.join(remaining)}\n"
                
                user_command_state[user_id] = {
                    'action': 'split_specific_confirm',
                    'idx': idx,
                    'num': num,
                    'remaining': remaining,
                    'specific': specific_numbers
                }
        else:
            # Полное разделение всех номеров
            if len(all_orders) <= 1:
                msg = await update.message.reply_text(f"❌ Заявка {num} уже разделена (содержит только один номер)")
                asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
                return True
            
            preview = "🔍 **Предпросмотр изменений:**\n\n"
            preview += f"**Заявка {num} будет разделена на {len(all_orders)} частей:**\n"
            for j, ord_num in enumerate(all_orders, 1):
                preview += f"{j}) {ord_num}\n"
            
            user_command_state[user_id] = {
                'action': 'split_all_confirm',
                'idx': idx,
                'num': num,
                'orders': all_orders
            }
        
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Применить", callback_data="confirm_split")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_split")]
        ])
        
        msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return True
    
    # НОВЫЙ БЛОК: обработка добавления заявки
    elif state['action'] == 'add':
        # Проверяем, что пользователь ввёл что-то
        if not text or text.lower() == 'отмена':
            if user_id in user_command_state:
                del user_command_state[user_id]
            msg = await update.message.reply_text("❌ Добавление отменено")
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            return True
        
        # Парсим введённую заявку
        success, result = await parse_and_preview_add(update, context, text, original_data)
        
        if success:
            # Сохраняем данные для подтверждения
            user_command_state[user_id] = {
                'action': 'add_confirm',
                'new_entry': result['new_entry'],
                'insert_idx': result['insert_idx']
            }
            
            # Показываем предпросмотр
            preview = "🔍 **Предпросмотр добавления:**\n\n"
            preview += f"**Новая заявка будет добавлена под номером {result['insert_idx'] + 1}:**\n"
            preview += f"• {result['new_entry']['order_number']}\n"
            if result['new_entry'].get('has_fixa') and result['new_entry'].get('fixa_time'):
                preview += f"  Фикса: {result['new_entry']['fixa_time']}\n"
            preview += f"• {result['new_entry']['address']}\n"
            preview += f"• {result['new_entry']['executors']}\n"
            preview += f"• {result['new_entry']['phone']}\n\n"
            preview += f"*Все заявки с номера {result['insert_idx'] + 2} будут сдвинуты*"
            
            confirm_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Добавить", callback_data="confirm_add")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel_add")]
            ])
            
            msg = await update.message.reply_text(preview, reply_markup=confirm_keyboard, parse_mode='Markdown')
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        else:
            # Ошибка парсинга - показываем сообщение и остаёмся в режиме ввода
            msg = await update.message.reply_text(f"❌ {result}")
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
            # Не удаляем состояние, чтобы пользователь мог попробовать снова
            return True
        
        return True
    
    return False

# Новая вспомогательная функция для парсинга добавления
async def parse_and_preview_add(update, context, text, original_data):
    """Парсит ввод для добавления заявки и возвращает предпросмотр"""
    
    # Определяем разделитель (теперь только |, но можно оставить поддержку /)
    delimiter = None
    if '|' in text:
        delimiter = '|'
    elif '/' in text:
        delimiter = '/'
        # Предупреждаем, что лучше использовать |
        logger.info("Рекомендуется использовать | вместо / в адресах с дробями")
    else:
        return False, "Используйте | или / как разделитель. Рекомендуется | для адресов с дробями."
    
    # Разделяем по первому вхождению разделителя
    parts = text.split(delimiter)
    
    # Очищаем части от лишних пробелов
    parts = [p.strip() for p in parts]
    
    try:
        if len(parts) == 4:  # Без фиксы
            first_part = parts[0]
            
            # Извлекаем номер и номер заявки
            if ') ' in first_part:
                num_str = first_part.split(') ')[0].strip()
                order_num = first_part.split(') ')[1].strip()
            elif ')' in first_part:
                num_str = first_part.split(')')[0].strip()
                order_num = first_part.split(')')[1].strip()
            else:
                # Если нет номера в начале, добавляем в конец
                order_num = first_part
                num_str = None
            
            # Проверяем границы, если указан номер
            total_orders = len(original_data['order_number'])
            if num_str and num_str.isdigit():
                insert_idx = int(num_str) - 1
                if insert_idx < 0 or insert_idx > total_orders:
                    return False, f"Номер должен быть от 1 до {total_orders + 1}"
            else:
                # Добавляем в конец
                insert_idx = total_orders
            
            # Очищаем номер заявки
            order_num = re.sub(r'[^\d,]', '', order_num)
            
            new_entry = {
                'order_number': order_num,
                'address': parts[1],
                'executors': parts[2],
                'phone': parts[3],
                'has_fixa': False,
                'fixa_time': ''
            }
            
        elif len(parts) == 5:  # С фиксой
            first_part = parts[0]
            
            if ') ' in first_part:
                num_str = first_part.split(') ')[0].strip()
                order_num = first_part.split(') ')[1].strip()
            elif ')' in first_part:
                num_str = first_part.split(')')[0].strip()
                order_num = first_part.split(')')[1].strip()
            else:
                order_num = first_part
                num_str = None
            
            total_orders = len(original_data['order_number'])
            if num_str and num_str.isdigit():
                insert_idx = int(num_str) - 1
                if insert_idx < 0 or insert_idx > total_orders:
                    return False, f"Номер должен быть от 1 до {total_orders + 1}"
            else:
                insert_idx = total_orders
            
            order_num = re.sub(r'[^\d,]', '', order_num)
            fixa_part = parts[1]
            fixa_time = re.sub(r'[^\d:]', '', fixa_part)
            
            # Объединяем фиксу с адресом для отображения
            address = f"{fixa_part}\n{parts[2]}"
            
            new_entry = {
                'order_number': order_num,
                'address': address,
                'executors': parts[3],
                'phone': parts[4],
                'has_fixa': True,
                'fixa_time': fixa_time
            }
            
        else:
            return False, f"Должно быть 4 части (без фиксы) или 5 частей (с фиксой). Получено: {len(parts)}\nИспользуйте | как разделитель"
        
        return True, {
            'new_entry': new_entry,
            'insert_idx': insert_idx
        }
        
    except Exception as e:
        return False, f"Ошибка парсинга: {str(e)}"

async def handle_command_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает подтверждение команд"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    action = query.data
    
    # Удаляем сообщение с предпросмотром
    await query.message.delete()
    
    if user_id not in user_command_state or user_id not in user_edit_state:
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Сессия истекла"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    state = user_command_state[user_id]
    edit_info = user_edit_state[user_id]
    original_data = edit_info['data']
    message_id = edit_info['message_id']
    file_name = edit_info.get('file_name')
    processor = edit_info['processor']
    
    if action == "confirm_merge" and state['action'] == 'merge_confirm':
        corrected_data = {key: list(value) for key, value in original_data.items()}
        corrected_data['order_number'][state['target_idx']] = state['new_numbers']
        
        # Удаляем исходные строки (сортируем в обратном порядке)
        for src_idx in sorted(state['source_indices'], reverse=True):
            for key in corrected_data:
                if src_idx < len(corrected_data[key]):
                    corrected_data[key].pop(src_idx)
        
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    f"✅ Объединены заявки {', '.join(map(str, state['display_numbers'][1:]))} в заявку {state['display_numbers'][0]}")
        
        if user_id in user_command_state:
            del user_command_state[user_id]
    
    elif action == "confirm_delete" and state['action'] == 'delete_confirm':
        corrected_data = {key: list(value) for key, value in original_data.items()}
        for idx in sorted(state['indices'], reverse=True):
            for key in corrected_data:
                if idx < len(corrected_data[key]):
                    corrected_data[key].pop(idx)
        
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    f"✅ Удалены заявки {', '.join(map(str, state['numbers']))}")
        
        if user_id in user_command_state:
            del user_command_state[user_id]
    
    elif action == "confirm_split" and state['action'] == 'split_all_confirm':
        corrected_data = {key: list(value) for key, value in original_data.items()}
        
        base_row = {key: corrected_data[key][state['idx']] for key in corrected_data}
        
        corrected_data['order_number'][state['idx']] = state['orders'][0]
        
        for j, ord_num in enumerate(state['orders'][1:], 1):
            for key in corrected_data:
                if key == 'order_number':
                    corrected_data[key].insert(state['idx'] + j, ord_num)
                else:
                    corrected_data[key].insert(state['idx'] + j, base_row[key])
        
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    f"✅ Разделена заявка {state['num']} на {len(state['orders'])} отдельных")
        
        if user_id in user_command_state:
            del user_command_state[user_id]
    
    elif action == "confirm_split" and state['action'] == 'split_specific_confirm':
        corrected_data = {key: list(value) for key, value in original_data.items()}
        
        # Обновляем исходную заявку
        corrected_data['order_number'][state['idx']] = ', '.join(state['remaining'])
        
        # Создаём новые заявки для выделенных номеров
        base_row = {key: corrected_data[key][state['idx']] for key in corrected_data}
        
        for j, ord_num in enumerate(state['specific'], 1):
            for key in corrected_data:
                if key == 'order_number':
                    corrected_data[key].insert(state['idx'] + j, ord_num)
                else:
                    corrected_data[key].insert(state['idx'] + j, base_row[key])
        
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    f"✅ Из заявки {state['num']} выделены {len(state['specific'])} номеров")
        
        if user_id in user_command_state:
            del user_command_state[user_id]
    
    # НОВЫЙ БЛОК: подтверждение добавления заявки
    elif action == "confirm_add" and state['action'] == 'add_confirm':
        corrected_data = {key: list(value) for key, value in original_data.items()}
        
        # Вставляем новую заявку
        insert_idx = state['insert_idx']
        new_entry = state['new_entry']
        
        for key in corrected_data:
            if key == 'order_number':
                corrected_data[key].insert(insert_idx, new_entry['order_number'])
            elif key == 'address':
                corrected_data[key].insert(insert_idx, new_entry['address'])
            elif key == 'executors':
                corrected_data[key].insert(insert_idx, new_entry['executors'])
            elif key == 'phone':
                corrected_data[key].insert(insert_idx, new_entry['phone'])
            elif key == 'has_fixa':
                corrected_data[key].insert(insert_idx, new_entry.get('has_fixa', False))
            elif key == 'fixa_time':
                corrected_data[key].insert(insert_idx, new_entry.get('fixa_time', ''))
            else:
                # Для остальных полей дублируем данные из соседней строки или оставляем пустыми
                if len(corrected_data[key]) > insert_idx:
                    corrected_data[key].insert(insert_idx, corrected_data[key][insert_idx] if insert_idx < len(corrected_data[key]) else "")
                else:
                    corrected_data[key].insert(insert_idx, "")
        
        await show_correction_result(update, context, original_data, corrected_data,
                                    message_id, file_name, processor,
                                    f"✅ Добавлена новая заявка под номером {insert_idx + 1}")
        
        if user_id in user_command_state:
            del user_command_state[user_id]
    
    elif action in ["cancel_merge", "cancel_delete", "cancel_split", "cancel_add"]:
        if user_id in user_command_state:
            del user_command_state[user_id]
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Операция отменена"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_merge_street(update: Update, context: ContextTypes.DEFAULT_TYPE, street_key: str):
    """Объединяет все заявки на указанной улице"""
    query = update.callback_query
    
    await query.message.delete()
    
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"🔄 Функция объединения всех заявок по **{street_key}** в разработке\n\n"
        f"Пока можете воспользоваться ручным режимом",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✍️ Свой вариант", callback_data="custom_edit")]])
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Единый обработчик для всех текстовых сообщений в режиме редактирования"""
    user_id = update.effective_user.id
    
    # Сначала проверяем, есть ли активная простая команда
    if user_id in user_command_state:
        result = await handle_simple_command_input(update, context)
        if result:
            return
    
    # Затем проверяем, есть ли режим ручного редактирования
    if user_id in user_edit_state and user_edit_state[user_id].get('waiting_for_input'):
        await handle_custom_edit_input(update, context)
        return
    
    # Если ни то, ни другое - игнорируем
    return False

async def process_single_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str, document):
    """Обрабатывает один файл"""
    try:
        df = read_excel_optimized(file_path)
        logger.info(f"Файл прочитан: {len(df)} строк, колонки: {list(df.columns)}")
        
        processor = ExcelDataProcessor()
        data, message = processor.extract_data(df)
        
        if data is None:
            error_msg = await send_with_retry(
                update.message.reply_text,
                f"❌ {message}"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        context.chat_data['last_processor'] = processor
        context.chat_data['last_data'] = data
        context.chat_data['last_file_name'] = document.file_name
        
        formatted_result = processor.format_main_result(data)
        context.chat_data['last_formatted_result'] = formatted_result
        
        if not formatted_result:
            error_msg = await send_with_retry(
                update.message.reply_text,
                "❌ Не найдено записей с номерами заявок"
            )
            asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
            return
        
        result_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Исправить", callback_data="edit")],
            [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
            [InlineKeyboardButton("🗑️ Удалить", callback_data="del")]
        ])
        
        # Результат не удаляем - это основной результат
        if len(formatted_result) > 4000:
            parts = [formatted_result[i:i+4000] for i in range(0, len(formatted_result), 4000)]
            for i, part in enumerate(parts, 1):
                msg = await send_with_retry(
                    update.message.reply_text,
                    text=f"📋 **Результат обработки: `{document.file_name}` (часть {i}/{len(parts)}):**\n\n{part}",
                    parse_mode='Markdown',
                    reply_markup=result_keyboard if i == 1 else None
                )
                
                if msg and hasattr(msg, 'message_id'):
                    if 'result_messages' not in context.chat_data:
                        context.chat_data['result_messages'] = []
                    context.chat_data['result_messages'].append(msg.message_id)
                    logger.info(f"✅ ID результата {msg.message_id} сохранён")
        else:
            msg = await send_with_retry(
                update.message.reply_text,
                f"📋 **Результат обработки: `{document.file_name}`**\n\n{formatted_result}",
                parse_mode='Markdown',
                reply_markup=result_keyboard
            )
            
            if msg and hasattr(msg, 'message_id'):
                if 'result_messages' not in context.chat_data:
                    context.chat_data['result_messages'] = []
                context.chat_data['result_messages'].append(msg.message_id)
                logger.info(f"✅ ID результата {msg.message_id} сохранён")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла: {str(e)}")
        error_msg = await send_with_retry(
            update.message.reply_text,
            f"❌ Ошибка при обработке: {str(e)[:200]}"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))

async def process_file_queue(user_id):
    """Обрабатывает очередь файлов"""
    if user_id not in user_queues:
        return
    
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    
    async with user_locks[user_id]:
        user_processing[user_id] = True
        
        try:
            while user_queues[user_id]:
                update, context, file_path, document, progress_msg = user_queues[user_id].popleft()
                
                try:
                    await send_with_retry(
                        progress_msg.edit_text,
                        f"📊 Обрабатываю файл {document.file_name}...\n⏳ В очереди осталось: {len(user_queues[user_id])}"
                    )
                    
                    start_time = asyncio.get_event_loop().time()
                    
                    await process_single_file(update, context, file_path, document)
                    
                    elapsed = asyncio.get_event_loop().time() - start_time
                    logger.info(f"Файл {document.file_name} обработан за {elapsed:.2f} сек")
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке файла из очереди: {e}")
                    error_msg = await send_with_retry(
                        update.message.reply_text,
                        f"❌ Ошибка при обработке файла {document.file_name}: {str(e)[:200]}"
                    )
                    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
                finally:
                    asyncio.create_task(delayed_delete(file_path, delay=TEMP_FILE_DELETE_DELAY))
                    logger.info(f"⏳ Запланировано удаление файла через {TEMP_FILE_DELETE_DELAY} сек")
                    
                    try:
                        await progress_msg.delete()
                        logger.info(f"✅ Сообщение удалено")
                    except Exception as e:
                        logger.error(f"❌ Ошибка удаления сообщения: {e}")
                    
        finally:
            user_processing[user_id] = False

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    user_id = update.effective_user.id
    
    file_ext = os.path.splitext(document.file_name)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        error_msg = await send_with_retry(
            update.message.reply_text,
            "❌ Пожалуйста, отправьте Excel файл (.xlsx, .xls, .xlsm)"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    if document.file_size > MAX_FILE_SIZE:
        error_msg = await send_with_retry(
            update.message.reply_text,
            f"❌ Файл слишком большой (макс. {MAX_FILE_SIZE // (1024*1024)} МБ)"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    if user_id not in user_queues:
        user_queues[user_id] = deque(maxlen=MAX_QUEUE_SIZE)
    
    if len(user_queues[user_id]) >= MAX_QUEUE_SIZE:
        error_msg = await send_with_retry(
            update.message.reply_text,
            f"❌ Слишком много файлов в очереди (макс. {MAX_QUEUE_SIZE}). Подождите обработки."
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    queue_position = len(user_queues[user_id]) + 1
    progress_msg = await send_with_retry(
        update.message.reply_text,
        f"📥 Файл '{document.file_name}' добавлен в очередь\n"
        f"📊 Позиция в очереди: {queue_position}\n"
        f"⏳ Ожидайте..."
    )
    
    try:
        file = await context.bot.get_file(document.file_id)
        
        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
            file_path = tmp_file.name
            await download_with_retry(file, file_path)
            logger.info(f"📁 Файл сохранён во временный файл: {file_path}")
        
        user_queues[user_id].append((update, context, file_path, document, progress_msg))
        
        if not user_processing.get(user_id, False):
            asyncio.create_task(process_file_queue(user_id))
        
    except Exception as e:
        logger.error(f"Ошибка при подготовке файла: {str(e)}")
        error_msg = await send_with_retry(
            progress_msg.edit_text,
            f"❌ Ошибка при подготовке файла: {str(e)[:200]}"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await send_with_retry(
        update.message.reply_text,
        "👋 Привет! Я бот для обработки Excel файлов.\n\n"
        "📋 **Основные данные для вывода:**\n"
        "• Телефон водителя (если нет, подставляется ФИО из 'Водитель')\n"
        "• Адрес\n"
        "• № ЗЭ (номер заявки) - сохраняются ведущие нули (11 знаков)\n"
        "• Исполнители (нормализация: добавляется 'чел' ко всем значениям, кроме НТК)\n"
        "• Город (автоматическая группировка)\n"
        "• **Фикса по времени** - если интервал 30 минут или время с 21:30, 22:00, 22:30, 23:00\n"
        "• **Автоматические ссылки** - Telegram сам сделает номера кликабельными\n\n"
        f"🔧 **Дополнительная обработка:**\n"
        f"• Объединение номеров заявок по одинаковым адресам\n"
        f"• Автозаполнение телефонов (из колонки 'Водитель')\n"
        f"• Копирование исполнителей из других заявок\n"
        f"• Очередь обработки (до {MAX_QUEUE_SIZE} файлов)\n"
        f"• Поддержка файлов до {MAX_FILE_SIZE // (1024*1024)} МБ\n"
        f"• **Команда /menu** - открыть меню с кнопками\n"
        f"• **Команда /del** - удалить последний результат\n"
        f"• **Команда /show** - показать сохранённые ID результатов\n\n"
        f"📤 Отправляйте файлы - они встанут в очередь и обработаются по порядку!\n\n"
        f"⏱️ Все служебные сообщения автоматически удаляются через {MESSAGE_DELETE_DELAY // 60} минуту.\n\n"
        f"🆕 **НОВОЕ:** Ручное редактирование результатов! Нажмите кнопку ✏️ Исправить под результатом",
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'last_data' not in context.chat_data or 'last_processor' not in context.chat_data:
        error_msg = await send_with_retry(
            update.message.reply_text,
            "❌ Сначала отправьте файл для обработки"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    data = context.chat_data['last_data']
    processor = context.chat_data['last_processor']
    file_name = context.chat_data.get('last_file_name', 'файл')
    
    total_orders = sum(1 for x in data['order_number'] if x)
    fixa_count = sum(1 for x in data['has_fixa'] if x)
    proc_stats = processor.get_processing_stats()
    
    stats = (
        f"📊 **Статистика по файлу:** `{file_name}`\n\n"
        f"• Всего записей с номерами: {total_orders}\n"
        f"• Найдено адресов: {sum(1 for x in data['address'] if x)}\n"
        f"• Найдено исполнителей: {sum(1 for x in data['executors'] if x)}\n"
        f"• Найдено телефонов: {sum(1 for x in data['phone'] if x)}\n"
        f"• Найдено городов: {len(set(data['city'])) if 'city' in data else 0}\n"
        f"• Записей с фиксой: {fixa_count}\n"
    )
    
    if proc_stats:
        stats += (
            f"\n📈 **Статистика обработки:**\n"
            f"• Исходных строк: {proc_stats['original_rows']}\n"
            f"• После обработки: {proc_stats['processed_rows']}\n"
            f"• Объединено строк: {proc_stats['merged_count']}\n"
        )
    
    stats += f"\n🔍 **Найденные колонки:**\n{processor.get_all_columns_info()}"
    
    msg = await send_with_retry(
        update.message.reply_text,
        stats,
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает состояние очереди"""
    user_id = update.effective_user.id
    
    if user_id not in user_queues or not user_queues[user_id]:
        msg = await send_with_retry(
            update.message.reply_text,
            "📭 Очередь пуста"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    queue_info = f"📊 **Очередь файлов:**\n\n"
    queue_info += f"• Всего в очереди: {len(user_queues[user_id])}\n"
    queue_info += f"• Обработка: {'✅ активна' if user_processing.get(user_id, False) else '⏸️ приостановлена'}\n\n"
    queue_info += "**Файлы в очереди:**\n"
    
    for i, (_, _, _, document, _) in enumerate(user_queues[user_id], 1):
        queue_info += f"{i}. `{document.file_name}`\n"
    
    msg = await send_with_retry(
        update.message.reply_text,
        queue_info,
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def columns_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "📋 **Все доступные для поиска колонки:**\n\n"
    for key, variants in ExcelDataProcessor.COLUMN_MAPPING.items():
        text += f"**{key}**: {', '.join(variants)}\n"
    msg = await send_with_retry(
        update.message.reply_text,
        text,
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def last_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'last_file_name' not in context.chat_data:
        error_msg = await send_with_retry(
            update.message.reply_text,
            "❌ Нет информации о последнем файле"
        )
        asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, error_msg.message_id, MESSAGE_DELETE_DELAY))
        return
    
    file_name = context.chat_data['last_file_name']
    data = context.chat_data.get('last_data', {})
    
    total_orders = sum(1 for x in data.get('order_number', []) if x)
    fixa_count = sum(1 for x in data.get('has_fixa', []) if x)
    
    info = (
        f"📁 **Последний файл:** `{file_name}`\n"
        f"📊 Записей в файле: {total_orders}\n"
        f"⏱️ Записей с фиксой: {fixa_count}\n\n"
        f"💡 Используйте /stats для подробной статистики"
    )
    
    msg = await send_with_retry(
        update.message.reply_text,
        info,
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = f"""
    📚 **Доступные команды:**
    
    /start - Начать работу
    /help - Это сообщение
    /menu - Открыть меню с кнопками
    /columns - Показать все возможные названия колонок
    /stats - Статистика по последнему файлу
    /last - Информация о последнем файле
    /queue - Показать состояние очереди
    /del - Удалить последний результат
    /show - Показать сохранённые ID результатов
    
    **📤 Как отправлять файлы:**
    • Просто прикрепите Excel файл (.xlsx, .xls, .xlsm)
    • Можно отправлять несколько файлов подряд
    • Максимум {MAX_QUEUE_SIZE} файлов в очереди
    • Максимальный размер: {MAX_FILE_SIZE // (1024*1024)} МБ
    
    **📋 Что ищет бот:**
    • Телефон водителя (если нет, подставляется ФИО из 'Водитель')
    • Адрес
    • № ЗЭ (номер заявки) - сохраняются ведущие нули (11 знаков)
    • Исполнители (нормализация: добавляется 'чел' ко всем значениям, кроме НТК)
    • Город (для группировки)
    • Время с / Время по (для определения фиксы)
    
    **⏱️ Фикса по времени:**
    • Интервал 30 минут (25-35 мин)
    • Спецвремя: 21:30, 22:00, 22:30, 23:00
    
    **📞 Автоматические ссылки:**
    Telegram сам распознает номера телефонов и делает их кликабельными.
    
    **🔧 Дополнительная обработка:**
    • Объединение номеров заявок по одинаковым адресам
    • Автозаполнение телефонов (из колонки 'Водитель')
    • Копирование исполнителей из других заявок
    • Очередь обработки для множества файлов
    • Детальное логирование фикс
    • Группировка по городам
    
    **🆕 НОВОЕ:**
    • Автоматическое разделение заявок с разными исполнителями по одному адресу
    • Учёт строений и корпусов
    • Учёт литер (для случаев с разными исполнителями)
    • ✏️ **Ручное редактирование** - нажмите кнопку "Исправить" под результатом
    • 📝 **Простые команды**: 
      - "объединить 16 и 17"
      - "удалить 17"
      - "разделить 16" (разделить все номера)
      - "разделить 16: 00310043732, 00310043983" (разделить конкретные номера)
    • 🔄 **Удобный формат**: можно использовать / вместо |, можно указывать фиксу
    • ➕ **Добавить заявку** - вставить новую заявку под нужным номером
    
    **⏱️ Автоудаление:**
    • Все служебные сообщения удаляются через {MESSAGE_DELETE_DELAY // 60} минуту
    • Временные файлы удаляются через {TEMP_FILE_DELETE_DELAY} секунд
    """
    msg = await send_with_retry(
        update.message.reply_text,
        help_text,
        parse_mode='Markdown'
    )
    asyncio.create_task(delete_message_after_delay(context, update.effective_chat.id, msg.message_id, MESSAGE_DELETE_DELAY))

def main():
    application = Application.builder().token(BOT_TOKEN).request(custom_request).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("del", delete_last_message))
    application.add_handler(CommandHandler("show", show_messages))
    application.add_handler(CommandHandler("columns", columns_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("last", last_file_command))
    application.add_handler(CommandHandler("queue", queue_command))
    
    application.add_handler(CallbackQueryHandler(menu_callback))
    
    # Единый обработчик текста
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    print("✅ Бот запущен и готов к работе!")
    print("📊 Режим: интеллектуальная обработка Excel с очередью")
    print(f"📥 Максимальный размер файла: {MAX_FILE_SIZE // (1024*1024)} МБ")
    print(f"📊 Максимальная очередь: {MAX_QUEUE_SIZE} файлов")
    print("👥 Исполнители: 'чел' добавляется ко всем значениям (кроме НТК)")
    print("   ✅ Поддержка комбинаций: 1 + 1 ВЭ + 1 ДЛ")
    print("⏱️ Фикса по времени: 30 мин ИЛИ спецвремя 21:30,22:00,22:30,23:00")
    print("📞 Телефоны: автоматическое распознавание Telegram")
    print("📋 Копирование исполнителей: из предыдущей строки")
    print("⏱️ Таймауты: увеличены до 120 секунд, добавлены повторные попытки")
    print("⚡ Оптимизация: преобразуются только нужные колонки")
    print(f"🗑️ Отложенное удаление файлов: через {TEMP_FILE_DELETE_DELAY} секунд")
    print(f"⏱️ Автоудаление служебных сообщений: через {MESSAGE_DELETE_DELAY // 60} минуту")
    print("📋 Команда /menu - меню с кнопками")
    print("🗑️ Команда /del - удалить последний результат")
    print("🔍 Команда /show - показать сохранённые ID результатов")
    print("🆕 НОВАЯ ФУНКЦИЯ: разделение заявок с разными исполнителями по одному адресу")
    print("✏️ НОВОЕ: ручное редактирование результатов (кнопка под результатом)")
    print("📝 НОВОЕ: простые команды (объединить, удалить, разделить)")
    print("➕ НОВОЕ: добавление заявок")
    print("🔄 НОВОЕ: поддержка фиксы в ручном режиме")
    print("🏗️ НОВОЕ: учёт литер при разных исполнителях")
    print("📨 Ожидание файлов...")
    print("ℹ️ Команды: /help, /stats, /last, /columns, /queue, /menu, /del, /show")
    
    application.run_polling()

if __name__ == '__main__':
    main()