from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import tempfile
import pandas as pd
import json
import re
from bot import ExcelDataProcessor

app = Flask(__name__)
CORS(app)

REMOTE_FILE = 'remote_addresses.json'

def load_remotes():
    """Загружает список удалёнок из JSON-файла"""
    if not os.path.exists(REMOTE_FILE):
        return []
    with open(REMOTE_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data.get('addresses', [])

def save_remotes(addresses):
    """Сохраняет список удалёнок в JSON-файл"""
    with open(REMOTE_FILE, 'w', encoding='utf-8') as f:
        json.dump({'addresses': addresses}, f, ensure_ascii=False, indent=2)

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

@app.route('/remotes', methods=['GET'])
def get_remotes():
    """Возвращает список удалёнок"""
    return jsonify(load_remotes())

@app.route('/remotes', methods=['POST'])
def add_remote():
    """Добавляет новую удалёнку"""
    data = request.json
    addresses = load_remotes()
    
    new_id = max([a.get('id', 0) for a in addresses]) + 1 if addresses else 1
    
    new_entry = {
        'id': new_id,
        'address': data.get('address'),
        'distance_km': data.get('distance_km'),
        'hourly_rate': data.get('hourly_rate')
    }
    
    addresses.append(new_entry)
    save_remotes(addresses)
    return jsonify({'success': True, 'id': new_id})

@app.route('/remotes/<int:remote_id>', methods=['DELETE'])
def delete_remote(remote_id):
    """Удаляет удалёнку по ID"""
    addresses = load_remotes()
    addresses = [a for a in addresses if a.get('id') != remote_id]
    save_remotes(addresses)
    return jsonify({'success': True})

@app.route('/remotes/<int:remote_id>', methods=['PUT'])
def update_remote(remote_id):
    """Обновляет удалёнку"""
    data = request.json
    addresses = load_remotes()
    
    for a in addresses:
        if a.get('id') == remote_id:
            a['address'] = data.get('address', a['address'])
            a['distance_km'] = data.get('distance_km', a['distance_km'])
            a['hourly_rate'] = data.get('hourly_rate', a['hourly_rate'])
            break
    
    save_remotes(addresses)
    return jsonify({'success': True})

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
        
        # Пропускаем пустые строки
        if not line:
            i += 1
            continue
        
        # Номер заявки — пропускаем
        if re.match(r'^\d+\)', line):
            i += 1
            continue
        
        # Фикса — пропускаем
        if 'Фикса' in line:
            i += 1
            continue
        
        # Город (📍) — пропускаем
        if '📍' in line:
            i += 1
            continue
        
        # Телефон — пропускаем
        if line.startswith('+'):
            i += 1
            continue
        
        # Строка с исполнителями — пропускаем, но запоминаем количество
        if 'чел' in line:
            # Извлекаем первое число из строки исполнителей
            workers_match = re.search(r'(\d+)', line)
            if workers_match:
                executors_count[-1] = int(workers_match.group(1)) if executors_count else 1
            i += 1
            continue
        
        # Всё остальное — считаем адресом
        if line and not line.startswith('+') and 'чел' not in line and 'Фикса' not in line and '📍' not in line:
            addresses.append(line)
            executors_count.append(1)  # временно, потом перезапишется из строки с исполнителями
            i += 1
            continue
        
        i += 1
    
    # Загружаем базу удалёнок
    remotes = load_remotes()
    remote_dict = {r['address']: r for r in remotes}
    
    # Формируем отчёт
    report_lines = ["📍 **Проверка удалёнок**\n"]
    found = False
    
    for addr, workers in zip(addresses, executors_count):
        # Проверяем точное совпадение
        if addr in remote_dict:
            found = True
            r = remote_dict[addr]
            distance = r['distance_km']
            rate = r['hourly_rate']
            total = (distance * 2 * 36) + (rate * workers)
            report_lines.append(f"✅ {addr}")
            report_lines.append(f"   📏 {distance} км × 2 × 36 = {distance * 2 * 36}")
            report_lines.append(f"   👥 {workers} чел × {rate} ₽ = {rate * workers}")
            report_lines.append(f"   💰 **ИТОГО: {total:,.0f} ₽**\n")
        else:
            # Проверяем частичное совпадение
            for remote_addr, r in remote_dict.items():
                if remote_addr in addr or addr in remote_addr:
                    found = True
                    distance = r['distance_km']
                    rate = r['hourly_rate']
                    total = (distance * 2 * 36) + (rate * workers)
                    report_lines.append(f"✅ {addr}")
                    report_lines.append(f"   (по базе: {remote_addr})")
                    report_lines.append(f"   📏 {distance} км × 2 × 36 = {distance * 2 * 36}")
                    report_lines.append(f"   👥 {workers} чел × {rate} ₽ = {rate * workers}")
                    report_lines.append(f"   💰 **ИТОГО: {total:,.0f} ₽**\n")
                    break
    
    if not found:
        report_lines.append("❌ Совпадений с базой не найдено")
    
    return "\n".join(report_lines)

if __name__ == '__main__':
    print("🚀 Запуск веб-сервера на http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
