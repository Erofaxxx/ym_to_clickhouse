# Полный экспорт данных из Яндекс.Метрики в ClickHouse

## Описание проблемы в старом скрипте

В оригинальном скрипте `load_ym_to_clickhouse.py` была обнаружена проблема с выгрузкой данных:
- **ClientID показывал 0** вместо реальных значений
- Причина: неправильное переименование колонок (простое удаление префикса вместо явного маппинга)

```python
# НЕПРАВИЛЬНО (старый метод):
df_renamed.columns = [col.replace('ym:s:', '') for col in df.columns]
# Результат: ym:s:clientID → clientID (может быть проблема с порядком)

# ПРАВИЛЬНО (новый метод):
df_renamed = df.rename(columns={'ym:s:clientID': 'ClientID', ...})
# Результат: явное соответствие каждого поля
```

## Решение: export_ym_complete.py

Новый скрипт `export_ym_complete.py` объединяет:
- ✅ Надёжность `export_ym_simple.py` (правильный маппинг полей)
- ✅ Полноту `load_ym_to_clickhouse.py` (все ~40 параметров)
- ✅ Graceful handling недоступных полей (не падает, просто пропускает)

## Ключевые улучшения

### 1. Явный маппинг полей
Каждое поле API явно сопоставляется с колонкой ClickHouse:

```python
VISITS_FIELD_MAPPING = {
    'ym:s:visitID': 'VisitID',
    'ym:s:clientID': 'ClientID',    # ← Явное указание!
    'ym:s:date': 'StartDate',
    # ... и т.д.
}
```

### 2. Автоматическая проверка доступности
Скрипт проверяет каждое поле перед выгрузкой:
- ✓ Доступно → выгружается
- ✗ Недоступно → пропускается с предупреждением

### 3. Динамическое создание таблиц
Таблица создаётся только с теми колонками, для которых есть данные.

## Список всех выгружаемых полей

### Hits (8 полей)
1. Browser - браузер
2. ClientID - идентификатор клиента
3. EventDate - дата события
4. EventTime - время события
5. DeviceCategory - категория устройства
6. TraficSource - источник трафика
7. OSRoot - операционная система
8. URL - URL страницы

### Visits (~40 полей)
1. VisitID - ID визита
2. WatchIDs - ID просмотров
3. StartDate - дата начала визита
4. StartTime - время начала визита
5. IsNewUser - новый пользователь (0/1)
6. StartURL - URL начальной страницы
7. EndURL - URL конечной страницы
8. VisitDuration - длительность визита (сек)
9. Bounce - отказ (0/1)
10. **ClientID - идентификатор клиента** ← ИСПРАВЛЕНО!
11. GoalsID - достигнутые цели
12. GoalsDateTime - время достижения целей
13. Referer - реферер
14. DeviceCategory - категория устройства
15. OSRoot - операционная система
16. Browser - браузер
17. TraficSource - источник трафика
18. UTMCampaign - UTM кампания
19. UTMContent - UTM содержание
20. UTMMedium - UTM канал
21. UTMSource - UTM источник
22. UTMTerm - UTM термин
23. TrafficSource - источник трафика (детально)
24. PageViews - количество просмотров страниц
25. PurchaseID - ID покупок
26. PurchaseDateTime - дата/время покупок
27. PurchaseRevenue - выручка от покупок
28. PurchaseCurrency - валюта покупок
29. PurchaseProductQuantity - количество товаров
30. ProductsPurchaseID - ID покупок товаров
31. ProductsID - ID товаров
32. ProductsName - названия товаров
33. ProductsCategory - категории товаров
34. RegionCity - город
35. ImpressionsURL - URL показов
36. ImpressionsDateTime - дата/время показов
37. ImpressionsProductID - ID товаров в показах
38. AdvEngine - рекламная система
39. ReferalSource - реферальный источник
40. SearchEngineRoot - поисковая система
41. SearchPhrase - поисковая фраза

## Установка и использование

### Установка зависимостей
```bash
pip install -r requirements.txt
```

### Настройка конфигурации

Используйте существующий `config.json` или создайте новый:

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

### Запуск

```bash
# Выгрузка всех данных (hits + visits)
python export_ym_complete.py --config config.json

# Только hits
python export_ym_complete.py --config config.json --hits-only

# Только visits
python export_ym_complete.py --config config.json --visits-only
```

### С переменными окружения

```bash
export YM_TOKEN="ваш_токен"
export YM_COUNTER_ID="номер_счётчика"
export YM_START_DATE="2024-01-01"
export YM_END_DATE="2024-01-31"
export CH_HOST="https://clickhouse:8443"
export CH_USER="user"
export CH_PASS="password"
export CH_DATABASE="analytics"

python export_ym_complete.py
```

## Структура таблиц в ClickHouse

### Таблица `hits_complete`

```sql
CREATE TABLE hits_complete (
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

### Таблица `visits_complete`

```sql
CREATE TABLE visits_complete (
    VisitID UInt64,
    WatchIDs String,
    StartDate Date,
    StartTime DateTime,
    IsNewUser UInt8,
    StartURL String,
    EndURL String,
    VisitDuration UInt32,
    Bounce UInt8,
    ClientID UInt64,
    GoalsID String,
    GoalsDateTime String,
    Referer String,
    DeviceCategory String,
    OSRoot String,
    Browser String,
    TraficSource String,
    UTMCampaign String,
    UTMContent String,
    UTMMedium String,
    UTMSource String,
    UTMTerm String,
    TrafficSource String,
    PageViews UInt32,
    PurchaseID String,
    PurchaseDateTime String,
    PurchaseRevenue String,
    PurchaseCurrency String,
    PurchaseProductQuantity String,
    ProductsPurchaseID String,
    ProductsID String,
    ProductsName String,
    ProductsCategory String,
    RegionCity String,
    ImpressionsURL String,
    ImpressionsDateTime String,
    ImpressionsProductID String,
    AdvEngine String,
    ReferalSource String,
    SearchEngineRoot String,
    SearchPhrase String
) ENGINE = MergeTree()
ORDER BY (intHash32(ClientID), StartDate)
SAMPLE BY intHash32(ClientID)
SETTINGS index_granularity=8192
```

**Примечание:** Если некоторые поля недоступны для вашего счётчика, соответствующие колонки не будут созданы.

## Пример вывода скрипта

```
============================================================
YANDEX METRICA COMPLETE EXPORT TO CLICKHOUSE
============================================================

2024-01-15 10:00:00 - INFO - Configuration validated successfully
2024-01-15 10:00:01 - INFO - ClickHouse connection established

============================================================
EXPORTING HITS DATA
============================================================
2024-01-15 10:00:02 - INFO - Validating 8 fields for hits...
2024-01-15 10:00:03 - INFO - ✓ All 8 fields are available for hits
2024-01-15 10:00:04 - INFO - ✓ Logs API request created for hits with ID: 12345
2024-01-15 10:00:05 - INFO - Waiting for request 12345 to be processed...
2024-01-15 10:01:05 - INFO - Request status: processed
2024-01-15 10:01:05 - INFO - ✓ Request processed successfully. Parts: 2
2024-01-15 10:01:06 - INFO - Downloading hits data from 2 parts...
2024-01-15 10:01:10 - INFO -   ✓ Part 0 downloaded: 500000 rows
2024-01-15 10:01:15 - INFO -   ✓ Part 1 downloaded: 412070 rows
2024-01-15 10:01:15 - INFO - ✓ Total rows downloaded for hits: 912070
2024-01-15 10:01:16 - INFO - ✓ Created table: hits_complete with 8 columns
2024-01-15 10:01:20 - INFO - ✓ Successfully uploaded 912070 rows to hits_complete
2024-01-15 10:01:20 - INFO - ✓ Hits export completed successfully!

============================================================
EXPORTING VISITS DATA
============================================================
2024-01-15 10:01:21 - INFO - Validating 41 fields for visits...
2024-01-15 10:01:22 - INFO - Testing fields individually for visits...
2024-01-15 10:01:23 - INFO -   ✓ ym:s:visitID
2024-01-15 10:01:24 - INFO -   ✓ ym:s:clientID
...
2024-01-15 10:01:50 - WARNING -   ✗ ym:s:DirectPlatform - error
2024-01-15 10:01:51 - WARNING - ⚠ Some fields are not available and will be skipped:
2024-01-15 10:01:51 - WARNING -   - ym:s:DirectPlatform
2024-01-15 10:01:51 - INFO - Continuing with 40 available fields
2024-01-15 10:01:52 - INFO - ✓ Logs API request created for visits with ID: 12346
...
2024-01-15 10:05:30 - INFO - ✓ Successfully uploaded 106194 rows to visits_complete
2024-01-15 10:05:30 - INFO - ✓ Visits export completed successfully!

============================================================
ALL EXPORTS COMPLETED SUCCESSFULLY!
============================================================
Tables created in database 'analytics':
  - hits_complete (8 fields)
  - visits_complete (40 fields)
```

## Примеры запросов

### Проверка выгруженных данных

```sql
-- Количество строк
SELECT count() FROM hits_complete;
SELECT count() FROM visits_complete;

-- Диапазон дат
SELECT min(EventDate), max(EventDate) FROM hits_complete;
SELECT min(StartDate), max(StartDate) FROM visits_complete;

-- Проверка ClientID (должны быть реальные значения, не 0!)
SELECT ClientID, count() as cnt
FROM visits_complete
GROUP BY ClientID
ORDER BY cnt DESC
LIMIT 10;
```

### Анализ трафика

```sql
-- Топ браузеров
SELECT Browser, count() as visits
FROM visits_complete
GROUP BY Browser
ORDER BY visits DESC
LIMIT 10;

-- Распределение по устройствам
SELECT DeviceCategory, count() as visits
FROM visits_complete
GROUP BY DeviceCategory;

-- Источники трафика
SELECT TraficSource, count() as visits
FROM visits_complete
GROUP BY TraficSource
ORDER BY visits DESC
LIMIT 10;
```

### Анализ UTM-меток

```sql
-- UTM-кампании
SELECT
    UTMSource,
    UTMCampaign,
    UTMMedium,
    count() as visits
FROM visits_complete
WHERE UTMCampaign != ''
GROUP BY UTMSource, UTMCampaign, UTMMedium
ORDER BY visits DESC
LIMIT 20;
```

### E-commerce аналитика

```sql
-- Покупки по устройствам
SELECT
    DeviceCategory,
    count() as total_visits,
    countIf(PurchaseID != '[]') as visits_with_purchase,
    (visits_with_purchase * 100.0 / total_visits) as conversion_rate
FROM visits_complete
GROUP BY DeviceCategory
ORDER BY conversion_rate DESC;

-- Популярные товары
SELECT
    ProductsName,
    count() as purchase_count
FROM visits_complete
WHERE ProductsName != '[]' AND ProductsName != ''
GROUP BY ProductsName
ORDER BY purchase_count DESC
LIMIT 20;
```

### Анализ целей

```sql
-- Достижение целей
SELECT
    GoalsID,
    count() as visits_with_goal
FROM visits_complete
WHERE GoalsID != '[]' AND GoalsID != ''
GROUP BY GoalsID
ORDER BY visits_with_goal DESC;
```

### Поведенческий анализ

```sql
-- Длительность визита по устройствам
SELECT
    DeviceCategory,
    avg(VisitDuration) as avg_duration_sec,
    avg(PageViews) as avg_pageviews,
    countIf(Bounce = 1) * 100.0 / count() as bounce_rate
FROM visits_complete
GROUP BY DeviceCategory
ORDER BY avg_duration_sec DESC;
```

## Сравнение скриптов

| Характеристика | load_ym_to_clickhouse.py | export_ym_complete.py |
|---------------|--------------------------|----------------------|
| ClientID | ❌ Неправильно (0) | ✅ Правильно |
| Маппинг полей | ❌ Простое удаление префикса | ✅ Явное соответствие |
| Обработка ошибок | ⚠️ Падает при недоступных полях | ✅ Пропускает недоступные |
| Таблицы | ym_visits | hits_complete, visits_complete |
| Количество полей | ~36 (visits) | 8 (hits) + ~40 (visits) |
| Логирование | Базовое | Детальное с эмодзи |

## Устранение неполадок

### ClientID показывает 0
✅ **Исправлено!** Используйте `export_ym_complete.py` вместо `load_ym_to_clickhouse.py`

### Ошибка при недоступных полях
✅ **Исправлено!** Скрипт автоматически пропускает недоступные поля

### Некоторые поля не выгружаются
Это нормально - не все поля доступны для всех счётчиков. Скрипт выведет предупреждение и продолжит работу с доступными полями.

## Миграция с других скриптов

### Из load_ym_to_clickhouse.py
```bash
# 1. Остановите использование старого скрипта
# 2. Запустите новый
python export_ym_complete.py --config config.json

# 3. Проверьте ClientID
SELECT ClientID, count() FROM visits_complete GROUP BY ClientID LIMIT 10;

# 4. Если всё ок, можете удалить старую таблицу
DROP TABLE ym_visits;
```

### Из export_ym_simple.py
```bash
# Новый скрипт - это расширенная версия
# Просто запустите его для получения всех полей
python export_ym_complete.py --config config.json

# Таблицы не конфликтуют:
# - hits_simple vs hits_complete
# - visits_simple vs visits_complete
```

## Рекомендации

1. **Используйте export_ym_complete.py** для полного анализа данных
2. **Проверяйте ClientID** после первой выгрузки
3. **Сохраните логи** первого запуска для анализа доступных полей
4. **Не удаляйте старые таблицы** сразу - дайте время на проверку
5. **Настройте периодическую выгрузку** через cron или другой планировщик

## Техническая поддержка

Если обнаружите проблемы:
1. Проверьте логи выполнения
2. Убедитесь, что все поля доступны для вашего счётчика
3. Проверьте версии библиотек (`pip list | grep -E "requests|pandas"`)
4. Создайте issue в репозитории с полным логом выполнения
