# Краткое описание решения

## Реализованные программы

В репозитории созданы две полнофункциональные Python-программы для работы с данными Яндекс.Метрики и ClickHouse:

### 1. load_ym_to_clickhouse.py (16 KB)
**Назначение:** Автоматическая загрузка данных визитов из Яндекс.Метрики в ClickHouse

**Возможности:**
- ✓ Загрузка всех 40 требуемых параметров визитов
- ✓ Работа через Logs API с автоматическим ожиданием обработки
- ✓ Создание таблицы в ClickHouse с правильной схемой
- ✓ Полная обработка ошибок на всех этапах
- ✓ Детальное логирование процесса
- ✓ Поддержка конфигурации через JSON файл или переменные окружения
- ✓ Работает на macOS, Linux, Windows

**Загружаемые поля (40 параметров):**
- visitID, watchIDs, date, isNewUser
- startURL, endURL, visitDuration, bounce
- clientID, goalsID, goalsDateTime, referer
- deviceCategory, operatingSystemRoot
- DirectPlatform, DirectConditionType
- UTMCampaign, UTMContent, UTMMedium, UTMSource, UTMTerm
- TrafficSource, pageViews
- purchaseID, purchaseDateTime, purchaseRevenue, purchaseCurrency, purchaseProductQuantity
- productsPurchaseID, productsID, productsName, productsCategory
- regionCity
- impressionsURL, impressionsDateTime, impressionsProductID
- AdvEngine, ReferalSource, SearchEngineRoot, SearchPhrase

### 2. query_clickhouse.py (12 KB)
**Назначение:** Выполнение запросов к ClickHouse с красивым форматированием результатов (как в Jupyter Notebook)

**Возможности:**
- ✓ Красивое форматирование таблиц с цветным выводом
- ✓ Отображение первых 100 строк (настраивается)
- ✓ Автоматическое ограничение длинных строк
- ✓ Статистика по данным (типы, null-значения, числовая статистика)
- ✓ Интерактивный режим для последовательных запросов
- ✓ Поддержка различных форматов (grid, fancy_grid, simple, html, latex)
- ✓ Произвольные SQL запросы
- ✓ Полная обработка ошибок

## Дополнительные файлы

- **README.md** (9.1 KB) - Обновлен с описанием новых программ и примерами использования
- **USAGE_GUIDE.md** (22 KB) - Подробное руководство по использованию с примерами
- **config.example.json** - Пример файла конфигурации
- **requirements.txt** - Список зависимостей (requests, pandas, tabulate, colorama, plotly)
- **.gitignore** - Исключение временных файлов и секретов из git

## Обработка ошибок

Обе программы включают полную обработку ошибок:

### load_ym_to_clickhouse.py:
- Валидация конфигурации
- Проверка доступности Logs API
- Таймауты при ожидании обработки (до 30 минут)
- Обработка сетевых ошибок
- Обработка ошибок ClickHouse
- Детальное логирование всех операций

### query_clickhouse.py:
- Валидация конфигурации и подключения
- Обработка ошибок SQL запросов
- Graceful обработка пустых результатов
- Обработка длинных строк и больших датасетов
- Защита от некорректного ввода в интерактивном режиме

## Примеры использования

### Загрузка данных:
```bash
# С конфигурационным файлом
python load_ym_to_clickhouse.py --config config.json

# С переменными окружения
export YM_TOKEN=your_token
export YM_COUNTER_ID=123456
export YM_START_DATE=2024-01-01
export YM_END_DATE=2024-01-31
export CH_HOST=https://clickhouse.example.com:8443
export CH_USER=user
export CH_PASS=password
python load_ym_to_clickhouse.py
```

### Просмотр данных:
```bash
# Первые 100 строк таблицы
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100

# Произвольный запрос
python query_clickhouse.py --config config.json --query "SELECT date, COUNT(*) FROM analytics.ym_visits GROUP BY date LIMIT 100"

# С статистикой
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100 --stats

# Интерактивный режим
python query_clickhouse.py --config config.json --interactive
```

## Технические детали

**Язык:** Python 3.8+
**Зависимости:** requests, pandas, tabulate, colorama, plotly
**Кросс-платформенность:** macOS, Linux, Windows
**Тип выгрузки:** Визиты (visits)
**Количество полей:** 40 (все требуемые)

## Архитектура решения

1. **Модульность:** Программы используют существующий класс `simple_ch_client` из `some_funcs.py`
2. **Конфигурация:** Гибкая настройка через JSON или переменные окружения
3. **Логирование:** Стандартный модуль logging с уровнями INFO/ERROR
4. **Обработка ошибок:** Собственные классы исключений для разных типов ошибок
5. **CLI интерфейс:** Использование argparse для удобного CLI

## Выполнение требований

✓ Программа для загрузки данных из Яндекс.Метрики в ClickHouse
✓ Программа для запроса к таблице с красивым форматированием (как в Jupyter)
✓ Загрузка всех 40 требуемых параметров визитов
✓ Работает на macOS (и других ОС)
✓ Полная обработка ошибок
✓ Документация и примеры использования

## Установка и запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Настройка конфигурации
cp config.example.json config.json
# Отредактируйте config.json с вашими данными

# Загрузка данных
python load_ym_to_clickhouse.py --config config.json

# Просмотр данных
python query_clickhouse.py --config config.json --table database.table_name --limit 100
```

Подробную документацию см. в **USAGE_GUIDE.md**
