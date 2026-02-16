# Простой экспорт данных из Яндекс.Метрики в ClickHouse

## Описание

Скрипт `export_ym_simple.py` — это упрощённая версия экспорта данных из Яндекс.Метрики в ClickHouse, которая выгружает **только те параметры, которые используются в notebooks (.ipynb файлах)**.

### Основные отличия от `load_ym_to_clickhouse.py`

| Параметр | load_ym_to_clickhouse.py | export_ym_simple.py |
|----------|--------------------------|---------------------|
| Количество полей | ~36 полей | 8 полей (hits), 10 полей (visits) |
| Названия таблиц | ym_visits | hits_simple, visits_simple |
| Проверка доступности полей | ✓ | ✓✓ (более детальная) |
| Сложность | Высокая | Низкая |

### Какие поля выгружаются

#### Hits (Хиты) - 8 полей:
1. `ym:pv:browser` → Browser
2. `ym:pv:clientID` → ClientID
3. `ym:pv:date` → EventDate
4. `ym:pv:dateTime` → EventTime
5. `ym:pv:deviceCategory` → DeviceCategory
6. `ym:pv:lastTrafficSource` → TraficSource
7. `ym:pv:operatingSystemRoot` → OSRoot
8. `ym:pv:URL` → URL

**Таблица в ClickHouse:** `hits_simple`

#### Visits (Визиты) - 10 полей:
1. `ym:s:browser` → Browser
2. `ym:s:clientID` → ClientID
3. `ym:s:date` → StartDate
4. `ym:s:dateTime` → StartTime
5. `ym:s:deviceCategory` → DeviceCategory
6. `ym:s:lastTrafficSource` → TraficSource
7. `ym:s:operatingSystemRoot` → OSRoot
8. `ym:s:purchaseID` → (обрабатывается в Purchases, Revenue)
9. `ym:s:purchaseRevenue` → Revenue, Purchases
10. `ym:s:startURL` → StartURL

**Таблица в ClickHouse:** `visits_simple`

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Настройка

### Вариант 1: Использование конфигурационного файла

1. Скопируйте пример конфигурации:
```bash
cp config_simple.example.json config_simple.json
```

2. Отредактируйте `config_simple.json`:
```json
{
  "ym_token": "ваш_токен_яндекс_метрики",
  "ym_counter_id": "номер_счётчика",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "ch_host": "https://your-clickhouse-host:8443",
  "ch_user": "имя_пользователя",
  "ch_pass": "пароль",
  "ch_cacert": "YandexInternalRootCA.crt",
  "ch_database": "имя_базы_данных",
  "export_hits": true,
  "export_visits": true
}
```

3. Запустите скрипт:
```bash
python export_ym_simple.py --config config_simple.json
```

### Вариант 2: Использование переменных окружения

```bash
export YM_TOKEN="ваш_токен"
export YM_COUNTER_ID="номер_счётчика"
export YM_START_DATE="2024-01-01"
export YM_END_DATE="2024-01-31"
export CH_HOST="https://your-clickhouse-host:8443"
export CH_USER="имя_пользователя"
export CH_PASS="пароль"
export CH_DATABASE="имя_базы_данных"
export EXPORT_HITS="true"
export EXPORT_VISITS="true"

python export_ym_simple.py
```

## Использование

### Выгрузка всех данных (hits и visits)
```bash
python export_ym_simple.py --config config_simple.json
```

### Выгрузка только hits
```bash
python export_ym_simple.py --config config_simple.json --hits-only
```

### Выгрузка только visits
```bash
python export_ym_simple.py --config config_simple.json --visits-only
```

## Проверка доступности полей

Скрипт автоматически проверяет доступность всех полей перед выгрузкой:

✓ **Все поля доступны** - выгрузка продолжается со всеми полями
⚠ **Некоторые поля недоступны** - выгрузка продолжается с доступными полями
❌ **Нет доступных полей** - выгрузка прерывается с ошибкой

## Структура таблиц в ClickHouse

### Таблица `hits_simple`

```sql
CREATE TABLE hits_simple (
    Browser String,
    ClientID UInt64,
    EventDate Date,
    EventTime DateTime,
    DeviceCategory String,
    TraficSource String,
    OSRoot String,
    URL String
) ENGINE = MergeTree()
ORDER BY (intHash32(ClientID), EventDate)
SAMPLE BY intHash32(ClientID)
SETTINGS index_granularity=8192
```

### Таблица `visits_simple`

```sql
CREATE TABLE visits_simple (
    Browser String,
    ClientID UInt64,
    StartDate Date,
    StartTime DateTime,
    DeviceCategory String,
    TraficSource String,
    OSRoot String,
    Purchases Int32,
    Revenue Double,
    StartURL String
) ENGINE = MergeTree()
ORDER BY (intHash32(ClientID), StartDate)
SAMPLE BY intHash32(ClientID)
SETTINGS index_granularity=8192
```

## Примеры запросов к данным

### Проверка загруженных данных

```sql
-- Количество строк в таблицах
SELECT count() FROM hits_simple;
SELECT count() FROM visits_simple;

-- Диапазон дат
SELECT min(EventDate), max(EventDate) FROM hits_simple;
SELECT min(StartDate), max(StartDate) FROM visits_simple;
```

### Анализ трафика

```sql
-- Топ 10 браузеров (hits)
SELECT Browser, count() as cnt
FROM hits_simple
GROUP BY Browser
ORDER BY cnt DESC
LIMIT 10;

-- Распределение по устройствам (visits)
SELECT DeviceCategory, count() as cnt
FROM visits_simple
GROUP BY DeviceCategory
ORDER BY cnt DESC;

-- Источники трафика
SELECT TraficSource, count() as cnt
FROM visits_simple
GROUP BY TraficSource
ORDER BY cnt DESC
LIMIT 10;
```

### Анализ покупок

```sql
-- Общая статистика по покупкам
SELECT
    count() as total_visits,
    sum(Purchases) as total_purchases,
    sum(Revenue) as total_revenue,
    avg(Revenue) as avg_revenue
FROM visits_simple;

-- Конверсия по устройствам
SELECT
    DeviceCategory,
    count() as visits,
    sum(Purchases) as purchases,
    sum(Revenue) as revenue,
    (sum(Purchases) * 100.0 / count()) as conversion_rate
FROM visits_simple
GROUP BY DeviceCategory
ORDER BY conversion_rate DESC;
```

## Устранение неполадок

### Ошибка: "Missing required configuration"
Проверьте, что все обязательные параметры заданы в конфигурационном файле или переменных окружения.

### Ошибка: "Failed to connect to ClickHouse"
- Проверьте правильность хоста ClickHouse
- Убедитесь, что файл сертификата `YandexInternalRootCA.crt` существует
- Проверьте имя пользователя и пароль

### Ошибка: "Some fields are not available"
Это предупреждение означает, что некоторые поля недоступны для вашего счётчика. Скрипт продолжит работу с доступными полями.

### Ошибка: "Request processing timeout"
Увеличьте время ожидания или разбейте период выгрузки на более мелкие интервалы.

## Логирование

Скрипт выводит подробную информацию о процессе выполнения:
- ✓ Успешные операции
- ⚠ Предупреждения
- ❌ Ошибки

Пример вывода:
```
============================================================
YANDEX METRICA SIMPLE EXPORT TO CLICKHOUSE
============================================================

2024-01-15 10:00:00 - INFO - Configuration validated successfully
2024-01-15 10:00:01 - INFO - ClickHouse connection established

============================================================
EXPORTING HITS DATA
============================================================
2024-01-15 10:00:02 - INFO - Validating 8 fields for hits...
2024-01-15 10:00:03 - INFO - ✓ All 8 fields are available for hits
2024-01-15 10:00:04 - INFO - ✓ Logs API request created for hits with ID: 12345
...
```

## Сравнение с notebooks

Этот скрипт полностью воспроизводит логику из notebook-ов:
- `1a. get_data_via_logs_api.ipynb` - получение данных через API
- `2. upload_data_to_clickhouse.ipynb` - загрузка в ClickHouse

Основное преимущество — автоматизация и проверка доступности полей.
