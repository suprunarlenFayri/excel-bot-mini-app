from flask import Flask, request
from flask_cors import CORS
import os
import tempfile
import pandas as pd
from bot import ExcelDataProcessor  # Импортируем твой класс

app = Flask(__name__)
CORS(app)  # Разрешаем запросы из Mini App

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("📥 Получен файл...")
        
        # Получаем файл
        if 'file' not in request.files:
            return "❌ Нет файла", 400
        
        file = request.files['file']
        user_id = request.form.get('user_id', 'unknown')
        
        if file.filename == '':
            return "❌ Файл не выбран", 400
        
        print(f"📄 Имя файла: {file.filename}")
        print(f"👤 User ID: {user_id}")
        
        # Сохраняем во временный файл
        temp_dir = tempfile.gettempdir()
        file_ext = os.path.splitext(file.filename)[1].lower()
        temp_path = os.path.join(temp_dir, f"mini_app_{user_id}_{os.urandom(4).hex()}{file_ext}")
        file.save(temp_path)
        print(f"💾 Сохранён в: {temp_path}")
        
        # Обрабатываем Excel
        print("⚙️ Запускаем обработку...")
        df = pd.read_excel(temp_path, dtype=str)
        processor = ExcelDataProcessor()
        data, message = processor.extract_data(df)
        
        if data is None:
            os.remove(temp_path)
            return f"❌ Ошибка: {message}", 400
        
        # Форматируем результат
        result = processor.format_main_result(data)
        
        # Удаляем временный файл
        os.remove(temp_path)
        print("✅ Обработка завершена!")
        
        return result, 200, {'Content-Type': 'text/plain; charset=utf-8'}
        
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        return f"❌ Ошибка сервера: {str(e)}", 500

@app.route('/health', methods=['GET'])
def health():
    return "OK", 200

if __name__ == '__main__':
    print("🚀 Запуск веб-сервера на http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)