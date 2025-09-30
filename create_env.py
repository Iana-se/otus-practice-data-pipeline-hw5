import json
from pathlib import Path

# Пути к файлам
json_file = Path("infra/variables.json")
env_file = Path(".env")

# Читаем JSON
with open(json_file, "r") as f:
    data = json.load(f)

# Конвертируем в формат .env
lines = []
for key, value in data.items():
    # Если значение содержит переносы строк — оборачиваем в двойные кавычки
    if "\n" in value or " " in value or value.startswith("{"):
        # Экранируем внутренние кавычки для безопасного .env
        value = value.replace('"', '\\"')
        line = f'{key}="{value}"'
    else:
        line = f'{key}={value}'
    lines.append(line)

# Записываем в .env
with open(env_file, "w") as f:
    f.write("\n".join(lines) + "\n")

print(f".env файл успешно создан по пути {env_file.resolve()}")
