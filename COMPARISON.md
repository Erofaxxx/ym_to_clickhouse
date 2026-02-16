# Сравнение скриптов экспорта

## Обзор

В репозитории теперь есть два скрипта для экспорта данных из Яндекс.Метрики в ClickHouse:

### 1. `load_ym_to_clickhouse.py` (Полный экспорт)
**Цель:** Полный экспорт всех доступных параметров визитов

**Характеристики:**
- 📊 **Полей:** ~36 параметров
- 📋 **Таблица:** `ym_visits`
- 🎯 **Назначение:** Детальный анализ с максимумом данных
- 🔧 **Сложность:** Высокая

**Экспортируемые поля (36):**
```
visitID, watchIDs, date, isNewUser, startURL, endURL, visitDuration,
bounce, clientID, goalsID, goalsDateTime, referer, deviceCategory,
operatingSystemRoot, UTMCampaign, UTMContent, UTMMedium, UTMSource,
UTMTerm, TrafficSource, pageViews, purchaseID, purchaseDateTime,
purchaseRevenue, purchaseCurrency, purchaseProductQuantity,
productsPurchaseID, productsID, productsName, productsCategory,
regionCity, impressionsURL, impressionsDateTime, impressionsProductID,
AdvEngine, ReferalSource, SearchEngineRoot, SearchPhrase
```

### 2. `export_ym_simple.py` (Упрощённый экспорт) ✨ **НОВЫЙ**
**Цель:** Экспорт только тех полей, что используются в .ipynb notebooks

**Характеристики:**
- 📊 **Полей:** 8 (hits) + 10 (visits)
- 📋 **Таблицы:** `hits_simple`, `visits_simple`
- 🎯 **Назначение:** Быстрый анализ основных метрик
- 🔧 **Сложность:** Низкая

**Экспортируемые поля:**

**Hits (8 полей):**
```
browser, clientID, date, dateTime, deviceCategory,
lastTrafficSource, operatingSystemRoot, URL
```

**Visits (10 полей):**
```
browser, clientID, date, dateTime, deviceCategory,
lastTrafficSource, operatingSystemRoot, purchaseID,
purchaseRevenue, startURL
```

## Детальное сравнение

| Параметр | load_ym_to_clickhouse.py | export_ym_simple.py |
|----------|--------------------------|---------------------|
| **Количество полей** | ~36 | 8 (hits) + 10 (visits) |
| **Источники данных** | visits | hits + visits |
| **Названия таблиц** | `ym_visits` | `hits_simple`, `visits_simple` |
| **Проверка полей** | Есть | Улучшенная |
| **Обработка покупок** | Нет | Да (Revenue, Purchases) |
| **Совместимость с notebooks** | Частичная | Полная |
| **Размер выгрузки** | Большой | Маленький |
| **Скорость работы** | Медленнее | Быстрее |
| **Использование памяти** | Больше | Меньше |

## Когда использовать каждый скрипт

### Используйте `load_ym_to_clickhouse.py` когда:
- ✅ Нужен полный набор данных для глубокого анализа
- ✅ Требуются UTM-метки, цели, e-commerce данные
- ✅ Планируется сложная аналитика и сегментация
- ✅ Важны данные о продуктах и показах

### Используйте `export_ym_simple.py` когда:
- ✅ Нужны только базовые метрики
- ✅ Хотите воспроизвести результаты из notebooks
- ✅ Требуется быстрая выгрузка
- ✅ Ограничены ресурсы (память, диск)
- ✅ Нужны данные и по хитам, и по визитам

## Примеры использования

### Полный экспорт (load_ym_to_clickhouse.py)
```bash
python load_ym_to_clickhouse.py --config config.json
```

**Результат:**
- Таблица `ym_visits` с 36 колонками
- Детальные данные о визитах

### Упрощённый экспорт (export_ym_simple.py)
```bash
# Выгрузить всё
python export_ym_simple.py --config config_simple.json

# Только хиты
python export_ym_simple.py --config config_simple.json --hits-only

# Только визиты
python export_ym_simple.py --config config_simple.json --visits-only
```

**Результат:**
- Таблица `hits_simple` с 8 колонками
- Таблица `visits_simple` с 10 колонками
- Соответствие данным из notebooks

## Запросы к данным

### Общие метрики (оба скрипта)

```sql
-- Трафик по устройствам
SELECT DeviceCategory, count() as visits
FROM visits_simple  -- или ym_visits
GROUP BY DeviceCategory;

-- Топ источников
SELECT TraficSource, count() as visits
FROM visits_simple  -- или ym_visits
GROUP BY TraficSource
ORDER BY visits DESC
LIMIT 10;
```

### Только для export_ym_simple.py

```sql
-- Анализ хитов
SELECT
    Browser,
    count() as pageviews,
    uniq(ClientID) as unique_users
FROM hits_simple
GROUP BY Browser
ORDER BY pageviews DESC
LIMIT 10;

-- Конверсия по устройствам
SELECT
    DeviceCategory,
    count() as visits,
    sum(Purchases) as purchases,
    sum(Revenue) as revenue,
    (sum(Purchases) * 100.0 / count()) as conversion_rate
FROM visits_simple
GROUP BY DeviceCategory;
```

### Только для load_ym_to_clickhouse.py

```sql
-- UTM-кампании
SELECT
    UTMCampaign,
    UTMSource,
    count() as visits
FROM ym_visits
WHERE UTMCampaign != ''
GROUP BY UTMCampaign, UTMSource
ORDER BY visits DESC;

-- Достижение целей
SELECT
    goalsID,
    count() as visits
FROM ym_visits
WHERE goalsID != ''
GROUP BY goalsID;
```

## Миграция между скриптами

### Из notebooks в export_ym_simple.py
Данные будут **идентичными**, так как скрипт использует те же поля.

### Из export_ym_simple.py в load_ym_to_clickhouse.py
Если нужно больше данных:
1. Запустите `load_ym_to_clickhouse.py`
2. Таблицы не конфликтуют (`ym_visits` vs `hits_simple/visits_simple`)
3. Можно использовать обе одновременно

### Из load_ym_to_clickhouse.py в export_ym_simple.py
Если хотите упростить:
1. Запустите `export_ym_simple.py`
2. Получите более лёгкие таблицы
3. Старую таблицу `ym_visits` можно оставить или удалить

## Рекомендации

1. **Начните с export_ym_simple.py** — проще и быстрее
2. **Переходите на load_ym_to_clickhouse.py** если нужен расширенный анализ
3. **Используйте оба** для разных задач (простой + детальный анализ)
4. **Проверяйте доступность полей** — оба скрипта делают это автоматически

## Поддержка и документация

- `export_ym_simple.py` → см. `README_SIMPLE.md`
- `load_ym_to_clickhouse.py` → см. `USAGE_GUIDE.md`
- Общая документация → `README.md`
