from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import tempfile
import pandas as pd
import psycopg2
import psycopg2.extras
import re
from bot import ExcelDataProcessor

app = Flask(__name__)
CORS(app)

# ==================== КОНФИГУРАЦИЯ БД ====================
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """Возвращает соединение с PostgreSQL"""
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Создаёт таблицу для удалёнок, если её нет"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS remotes (
                        id SERIAL PRIMARY KEY,
                        address TEXT UNIQUE NOT NULL,
                        distance_km REAL NOT NULL,
                        hourly_rate REAL NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # Создаём индекс для быстрого поиска по адресу
                cur.execute('CREATE INDEX IF NOT EXISTS idx_address ON remotes(address)')
            conn.commit()
        print("✅ Таблица remotes готова")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")

# ==================== КОЭФФИЦИЕНТЫ ФОРМУЛЫ ====================
DISTANCE_COEFF_1 = 2
DISTANCE_COEFF_2 = 36

def get_car_multiplier(workers):
    """Возвращает множитель машин в зависимости от количества исполнителей"""
    if workers <= 4:
        return 1
    elif workers <= 8:
        return 2
    elif workers <= 12:
        return 3
    elif workers <= 16:
        return 4
    else:
        return (workers + 3) // 4

# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================
def load_remotes():
    """Загружает список удалёнок из PostgreSQL"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute('SELECT id, address, distance_km, hourly_rate FROM remotes ORDER BY id')
                return cur.fetchall()
    except Exception as e:
        print(f"❌ Ошибка загрузки из БД: {e}")
        return []

def add_remote_to_db(address, distance_km, hourly_rate):
    """Добавляет новую удалёнку в БД"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO remotes (address, distance_km, hourly_rate)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (address) DO UPDATE SET
                        distance_km = EXCLUDED.distance_km,
                        hourly_rate = EXCLUDED.hourly_rate
                    RETURNING id
                ''', (address, distance_km, hourly_rate))
                new_id = cur.fetchone()[0]
            conn.commit()
            return new_id
    except Exception as e:
        print(f"❌ Ошибка добавления в БД: {e}")
        return None

def delete_remote_from_db(remote_id):
    """Удаляет удалёнку из БД по ID"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM remotes WHERE id = %s', (remote_id,))
                deleted = cur.rowcount
            conn.commit()
            return deleted > 0
    except Exception as e:
        print(f"❌ Ошибка удаления из БД: {e}")
        return False

def update_remote_in_db(remote_id, address, distance_km, hourly_rate):
    """Обновляет удалёнку в БД"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    UPDATE remotes 
                    SET address = %s, distance_km = %s, hourly_rate = %s
                    WHERE id = %s
                ''', (address, distance_km, hourly_rate, remote_id))
                updated = cur.rowcount
            conn.commit()
            return updated > 0
    except Exception as e:
        print(f"❌ Ошибка обновления в БД: {e}")
        return False

# ==================== ТАБЛИЦА ТАРИФОВ ====================
CITY_RATES = {
    'Абакан': 428, 'Ангарск': 423, 'Апатиты': 495, 'Арзамас': 482,
    'Артем': 428, 'Асбест': 428, 'Астрахань': 428, 'Ачинск': 428,
    'Барнаул': 428, 'Белогорск': 706, 'Бердск': 487, 'Бийск': 428,
    'Благовещенск': 423, 'Бор': 482, 'Борисоглебск': 482, 'Братск': 403,
    'Владивосток': 480, 'Воркута': 482, 'Горно-Алтайск': 428, 'Екатеринбург': 391,
    'Елец': 482, 'Железногорск': 495, 'Забайкальск': 707, 'Иркутск': 487,
    'Ишим': 745, 'Калуга': 482, 'Каменск-Уральский': 428, 'Канск': 428,
    'Кемерово': 428, 'Кимры': 482, 'Кирово-Чепецк': 482, 'Клинцы': 482,
    'Кольчугино': 482, 'Комсомольск-на-Амуре': 423, 'Красноярск': 428, 'Курган': 420,
    'Кызыл': 428, 'Ленинск-Кузнецкий': 487, 'Ливны': 482, 'Липецк': 482,
    'Лиски': 482, 'Магнитогорск': 535, 'Мурманск': 440, 'Муром': 482,
    'Мценск': 482, 'Находка': 750, 'Нижний Новгород': 428, 'Новокузнецк': 428,
    'Новомосковск': 482, 'Новосибирск': 428, 'Новоуральск': 428, 'Норильск': 428,
    'Оренburg': 482, 'Первоуральск': 428, 'Переславль-Залесский': 535,
    'Петропавловск-Камчатский': 1100, 'Плесецк': 482, 'Прокопьевск': 428,
    'Рославль': 482, 'Россошь': 482, 'Рубцовск': 428, 'Рыбинск': 482,
    'Ставрополь': 482, 'Тверь': 482, 'Тобольск': 635, 'Томск': 460,
    'Тула': 482, 'Тюмень': 675, 'Узловая': 482, 'Улан-Удэ': 402,
    'Усинск': 482, 'Уссурийск': 1177, 'Хабаровск': 482, 'Чита': 423,
    'Шуя': 482, 'Щекино': 482, 'Южно-Сахалинск': 1100, 'Юрга': 488,
    'Якутск': 610, 'Ярославль': 428
}

CITY_MINIMUM = {
    'Абакан': 2, 'Ангарск': 2, 'Апатиты': 2, 'Арзамас': 2,
    'Артем': 2, 'Асбест': 2, 'Астрахань': 2, 'Ачинск': 2,
    'Барнаул': 2, 'Белогорск': 2, 'Бердск': 2, 'Бийск': 2,
    'Благовещенск': 2, 'Бор': 2, 'Борисоглебск': 2, 'Братск': 2,
    'Владивосток': 2, 'Воркута': 2, 'Горно-Алтайск': 3, 'Екатеринбург': 2,
    'Елец': 2, 'Железногорск': 2, 'Забайкальск': 2, 'Иркутск': 2,
    'Ишим': 2, 'Калуга': 2, 'Каменск-Уральский': 2, 'Канск': 3,
    'Кемерово': 2, 'Кимры': 2, 'Кирово-Чепецк': 2, 'Клинцы': 2,
    'Кольчугино': 2, 'Комсомольск-на-Амуре': 2, 'Красноярск': 2, 'Курган': 2,
    'Кызыл': 3, 'Ленинск-Кузнецкий': 2, 'Ливны': 2, 'Липецк': 2,
    'Лиски': 2, 'Магнитогорск': 2, 'Мурманск': 2, 'Муром': 2,
    'Мценск': 2, 'Находка': 2, 'Нижний Новгород': 2, 'Новокузнецк': 2,
    'Новомосковск': 2, 'Новосибирск': 2, 'Новоуральск': 2, 'Норильск': 3,
    'Оренбург': 2, 'Первоуральск': 2, 'Переславль-Залесский': 2,
    'Петропавловск-Камчатский': 1, 'Плесецк': 2, 'Прокопьевск': 2,
    'Рославль': 2, 'Россошь': 2, 'Рубцовск': 2, 'Рыбинск': 2,
    'Ставрополь': 2, 'Тверь': 2, 'Тобольск': 1, 'Томск': 2,
    'Тула': 2, 'Тюмень': 1, 'Узловая': 2, 'Улан-Удэ': 2,
    'Усинск': 2, 'Уссурийск': 1, 'Хабаровск': 2, 'Чита': 2,
    'Шуя': 2, 'Щекино': 2, 'Южно-Сахалинск': 1, 'Юрга': 2,
    'Якутск': 2, 'Ярославль': 2
}

def get_city_from_address(address):
    """Извлекает название города из адреса"""
    address_lower = address.lower()
    for city in CITY_RATES.keys():
        if city.lower() in address_lower:
            return city
    return None

# ==================== ОСНОВНЫЕ ЭНДПОИНТЫ ====================
@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("📥 Получен файл...")
        
        if 'file' not in request.files:
            return "❌ Нет файла", 400
        
        file = request.files['file']
        user_id = request.form.get('user_id', 'unknown')
        
        if file.filename == '':
            return "❌ Файл не выбран", 400
        
        print(f"📄 Имя файла: {file.filename}")
        print(f"👤 User ID: {user_id}")
        
        temp_dir = tempfile.gettempdir()
        file_ext = os.path.splitext(file.filename)[1].lower()
        temp_path = os.path.join(temp_dir, f"mini_app_{user_id}_{os.urandom(4).hex()}{file_ext}")
        file.save(temp_path)
        print(f"💾 Сохранён в: {temp_path}")
        
        print("⚙️ Запускаем обработку...")
        df = pd.read_excel(temp_path, dtype=str)
        processor = ExcelDataProcessor()
        data, message = processor.extract_data(df)
        
        if data is None:
            os.remove(temp_path)
            return f"❌ Ошибка: {message}", 400
        
        result = processor.format_main_result(data)
        
        os.remove(temp_path)
        print("✅ Обработка завершена!")
        
        return result, 200, {'Content-Type': 'text/plain; charset=utf-8'}
        
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        return f"❌ Ошибка сервера: {str(e)}", 500

@app.route('/health', methods=['GET'])
def health():
    return "OK", 200

# ==================== ЭНДПОИНТЫ ДЛЯ РАБОТЫ С УДАЛЁНКАМИ ====================
@app.route('/remotes', methods=['GET'])
def get_remotes():
    """Возвращает список удалёнок"""
    remotes = load_remotes()
    return jsonify(remotes)

@app.route('/remotes', methods=['POST'])
def add_remote():
    """Добавляет новую удалёнку"""
    data = request.json
    address = data.get('address')
    distance_km = data.get('distance_km')
    hourly_rate = data.get('hourly_rate')
    
    if not address or distance_km is None or hourly_rate is None:
        return jsonify({'success': False, 'error': 'Не все поля заполнены'}), 400
    
    new_id = add_remote_to_db(address, distance_km, hourly_rate)
    if new_id:
        return jsonify({'success': True, 'id': new_id})
    else:
        return jsonify({'success': False, 'error': 'Ошибка добавления'}), 500

@app.route('/remotes/<int:remote_id>', methods=['DELETE'])
def delete_remote(remote_id):
    """Удаляет удалёнку по ID"""
    if delete_remote_from_db(remote_id):
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Запись не найдена'}), 404

@app.route('/remotes/<int:remote_id>', methods=['PUT'])
def update_remote(remote_id):
    """Обновляет удалёнку"""
    data = request.json
    address = data.get('address')
    distance_km = data.get('distance_km')
    hourly_rate = data.get('hourly_rate')
    
    if update_remote_in_db(remote_id, address, distance_km, hourly_rate):
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Запись не найдена'}), 404

@app.route('/calculate-items', methods=['POST'])
def calculate_items():
    data = request.json
    result_text = data.get('result', '')
    
    # Парсим заявки из результата
    lines = result_text.split('\n')
    
    addresses = []
    cities_detected = []
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Ищем город-заголовок (📍 **Город**)
        if line.startswith('📍'):
            i += 1
            i += 1
            continue
        
        # Ищем номер заявки (1), 2) и т.д.)
        if re.match(r'^\d+\)', line):
            i += 1
            # Следующая строка — адрес
            if i < len(lines):
                address = lines[i].strip()
                addresses.append(address)
                # Определяем город для этого адреса
                city = get_city_from_address(address)
                cities_detected.append(city)
                i += 1
            # Пропускаем строку с исполнителями
            i += 1
            # Пропускаем строку с телефоном
            i += 1
            continue
        
        i += 1
    
    # Формируем отчёт
    report_lines = ["🧮 **Расчёт по пунктам**\n"]
    point_num = 1
    
    for idx, addr in enumerate(addresses):
        city = cities_detected[idx]
        if city:
            rate = CITY_RATES.get(city, 0)
            minimum = CITY_MINIMUM.get(city, 1)
            total = rate * minimum
            report_lines.append(f"П{point_num}: {total}")
        else:
            # Город не найден в таблице — показываем адрес
            report_lines.append(f"П{point_num}: {addr}")
        point_num += 1
    
    return "\n".join(report_lines)

# ==================== ПРОВЕРКА УДАЛЁНОК ====================
@app.route('/check-remotes', methods=['POST'])
def check_remotes():
    """Проверяет адреса из результата по базе удалёнок"""
    data = request.json
    result_text = data.get('result', '')
    
    # Парсим адреса и количество исполнителей из результата
    addresses = []
    executors_count = []
    lines = result_text.split('\n')
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
        if re.match(r'^\d+\)', line) or 'Фикса' in line or '📍' in line or line.startswith('+'):
            i += 1
            continue
        
        if 'чел' in line:
            workers_match = re.search(r'(\d+)', line)
            if workers_match and executors_count:
                executors_count[-1] = int(workers_match.group(1))
            i += 1
            continue
        
        if line and not line.startswith('+') and 'чел' not in line and 'Фикса' not in line and '📍' not in line:
            addresses.append(line)
            executors_count.append(1)
            i += 1
            continue
        
        i += 1
    
    # Загружаем базу удалёнок
    remotes = load_remotes()
    remote_dict = {r['address']: r for r in remotes}
    
    report_lines = ["📍 **Проверка удалёнок**\n"]
    found = False
    counter = 1
    
    for addr, workers in zip(addresses, executors_count):
        matched = False
        
        # Проверяем точное совпадение
        if addr in remote_dict:
            matched = True
            found = True
            r = remote_dict[addr]
            distance = r['distance_km']
            rate = r['hourly_rate']
            car_multiplier = get_car_multiplier(workers)
            distance_part = distance * DISTANCE_COEFF_1 * DISTANCE_COEFF_2 * car_multiplier
            rate_part = rate * workers
            total = distance_part + rate_part
            
            report_lines.append(f"{counter}) {addr}")
            report_lines.append(f"   {distance}км×2×36×{car_multiplier}({distance_part})+{rate}×{workers}чел={total}")
            report_lines.append("")
            counter += 1
        else:
            # Проверяем частичное совпадение
            for remote_addr, r in remote_dict.items():
                if remote_addr in addr or addr in remote_addr:
                    matched = True
                    found = True
                    distance = r['distance_km']
                    rate = r['hourly_rate']
                    car_multiplier = get_car_multiplier(workers)
                    distance_part = distance * DISTANCE_COEFF_1 * DISTANCE_COEFF_2 * car_multiplier
                    rate_part = rate * workers
                    total = distance_part + rate_part
                    
                    report_lines.append(f"{counter}) {addr}")
                    report_lines.append(f"   (по базе: {remote_addr})")
                    report_lines.append(f"   {distance}км×2×36×{car_multiplier}({distance_part})+{rate}×{workers}чел={total}")
                    report_lines.append("")
                    counter += 1
                    break
        
        if not matched:
            report_lines.append(f"{counter}) {addr}")
            report_lines.append(f"   ❌ Не найдено в базе")
            report_lines.append("")
            counter += 1
    
    if not found:
        report_lines.append("❌ Совпадений с базой не найдено")
    
    return "\n".join(report_lines)

# ==================== ЗАПУСК ====================
if __name__ == '__main__':
    # Инициализируем базу данных при старте
    init_db()
    print("🚀 Запуск веб-сервера на http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
