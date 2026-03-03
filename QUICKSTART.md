# Быстрый старт с export_ym_complete.py

## Что это?

**export_ym_complete.py** - улучшенная версия скрипта экспорта данных из Яндекс.Метрики в ClickHouse.

✅ **Исправляет проблему ClientID=0** из старого скрипта
✅ **Выгружает все ~40 полей** для visits и 8 для hits
✅ **Автоматически пропускает недоступные поля** без ошибок

## Установка

```bash
# 1. Клонируйте репозиторий (если ещё не сделали)
git clone https://github.com/Erofaxxx/ym_to_clickhouse.git
cd ym_to_clickhouse

# 2. Установите зависимости
pip install -r requirements.txt
```

## Быстрый запуск

```bash
# 1. Создайте конфигурацию (или используйте существующий config.json)
cp config.example.json config.json

# 2. Отредактируйте config.json своими данными:
# - ym_token: ваш токен Яндекс.Метрики
# - ym_counter_id: номер счётчика
# - start_date, end_date: период выгрузки
# - ch_host, ch_user, ch_pass: доступ к ClickHouse
# - ch_database: имя базы данных

# 3. Запустите экспорт
python export_ym_complete.py --config config.json
```

## Что будет выгружено?

### Таблица hits_complete (8 полей):
- Browser, ClientID, EventDate, EventTime
- DeviceCategory, TraficSource, OSRoot, URL

### Таблица visits_complete (~40 полей):
- Базовые: VisitID, ClientID, StartDate, StartTime, Browser, etc.
- UTM-метки: UTMCampaign, UTMSource, UTMMedium, etc.
- E-commerce: PurchaseID, PurchaseRevenue, ProductsName, etc.
- Поведение: VisitDuration, Bounce, PageViews, Goals, etc.
- Трафик: TrafficSource, AdvEngine, SearchPhrase, etc.

## Проверка результатов

```sql
-- 1. Проверьте количество данных
SELECT count() FROM hits_complete;
SELECT count() FROM visits_complete;

-- 2. Проверьте ClientID (должны быть реальные значения!)
SELECT ClientID, count() as visits
FROM visits_complete
GROUP BY ClientID
ORDER BY visits DESC
LIMIT 10;

-- ✅ Правильный результат:
-- ClientID должен быть большим числом (например, 123456789012)
-- НЕ должно быть: ClientID = 0 для всех строк
```

## Выборочная выгрузка

```bash
# Только hits
python export_ym_complete.py --config config.json --hits-only

# Только visits
python export_ym_complete.py --config config.json --visits-only
```

## Что делать если...

### Некоторые поля недоступны
✅ **Это нормально!** Скрипт автоматически пропустит их и выведет предупреждение.

### ClientID показывает 0
❌ **Вы используете старый скрипт!** Используйте `export_ym_complete.py` вместо `load_ym_to_clickhouse.py`.

### Ошибка при выполнении
1. Проверьте config.json - все ли параметры заполнены?
2. Проверьте доступ к ClickHouse
3. Проверьте токен Яндекс.Метрики
4. Посмотрите полный лог ошибки

## Сравнение скриптов

| Скрипт | ClientID | Полей | Рекомендация |
|--------|----------|-------|--------------|
| **export_ym_complete.py** | ✅ Правильно | 8 + ~40 | 🌟 Используйте |
| export_ym_simple.py | ✅ Правильно | 8 + 10 | ✅ Для базовой аналитики |
| load_ym_to_clickhouse.py | ❌ Может быть 0 | ~36 | ⚠️ Не используйте |

## Документация

- 📖 **Полная документация:** [README_COMPLETE.md](README_COMPLETE.md)
- 🔧 **Исправление ClientID:** [CLIENTID_FIX.md](CLIENTID_FIX.md)
- 📊 **Сравнение скриптов:** [COMPARISON.md](COMPARISON.md)

## Примеры анализа

```sql
-- Топ источников трафика
SELECT TraficSource, count() as visits
FROM visits_complete
GROUP BY TraficSource
ORDER BY visits DESC
LIMIT 10;

-- Конверсия по устройствам
SELECT
    DeviceCategory,
    count() as visits,
    countIf(PurchaseID != '[]') as purchases,
    (purchases * 100.0 / visits) as conversion_rate
FROM visits_complete
GROUP BY DeviceCategory
ORDER BY conversion_rate DESC;

-- Эффективность UTM-кампаний
SELECT
    UTMSource,
    UTMCampaign,
    count() as visits,
    countIf(PurchaseID != '[]') as purchases
FROM visits_complete
WHERE UTMCampaign != ''
GROUP BY UTMSource, UTMCampaign
ORDER BY purchases DESC
LIMIT 20;
```

## Поддержка

Если возникли вопросы или проблемы:
1. Проверьте документацию выше
2. Создайте issue в репозитории
3. Приложите полный лог выполнения скрипта

---

**Главное:** Используйте `export_ym_complete.py` для надёжной выгрузки с правильными значениями ClientID! 🎉
