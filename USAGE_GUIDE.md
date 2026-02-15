# Руководство по использованию программ YM to ClickHouse

Этот документ содержит подробные инструкции по использованию программ для загрузки данных из Яндекс.Метрики в ClickHouse и их визуализации.

## Содержание

1. [Установка и настройка](#установка-и-настройка)
2. [Программа загрузки данных](#программа-загрузки-данных)
3. [Программа визуализации данных](#программа-визуализации-данных)
4. [Примеры использования](#примеры-использования)
5. [Обработка ошибок](#обработка-ошибок)
6. [FAQ](#faq)

## Установка и настройка

### Требования

- Python 3.8 или выше
- Доступ к Яндекс.Метрике с OAuth токеном
- Доступ к ClickHouse серверу
- Операционная система: macOS, Linux, Windows

### Установка зависимостей

```bash
pip install -r requirements.txt
```

Будут установлены следующие пакеты:
- `requests` - для работы с API Яндекс.Метрики и ClickHouse
- `pandas` - для обработки данных
- `tabulate` - для красивого форматирования таблиц
- `colorama` - для цветного вывода в терминале
- `plotly` - для работы с функциями из some_funcs.py

### Получение OAuth токена Яндекс.Метрики

1. Создайте приложение на https://oauth.yandex.ru/client/new
2. Укажите права доступа для чтения данных Яндекс.Метрики
3. Получите Client ID приложения
4. Перейдите по ссылке:
   ```
   https://oauth.yandex.ru/authorize?response_type=token&client_id=<ваш_client_id>
   ```
5. Скопируйте полученный токен

### Настройка конфигурации

#### Вариант 1: Файл конфигурации (рекомендуется)

Создайте файл `config.json`:

```bash
cp config.example.json config.json
```

Заполните параметры в `config.json`:

```json
{
  "ym_token": "ваш_токен_яндекс_метрики",
  "ym_counter_id": "123456",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "ch_host": "https://your-clickhouse-host:8443",
  "ch_user": "username",
  "ch_pass": "password",
  "ch_cacert": "YandexInternalRootCA.crt",
  "ch_database": "analytics",
  "ch_table": "ym_visits"
}
```

**Важно:** Добавьте `config.json` в `.gitignore`, чтобы не закоммитить секретные данные!

#### Вариант 2: Переменные окружения

Создайте файл `.env` или экспортируйте переменные:

```bash
export YM_TOKEN=ваш_токен
export YM_COUNTER_ID=123456
export YM_START_DATE=2024-01-01
export YM_END_DATE=2024-01-31
export CH_HOST=https://your-clickhouse-host:8443
export CH_USER=username
export CH_PASS=password
export CH_CACERT=YandexInternalRootCA.crt
export CH_DATABASE=analytics
export CH_TABLE=ym_visits
```

## Программа загрузки данных

### Описание

`load_ym_to_clickhouse.py` - программа для автоматической загрузки данных визитов из Яндекс.Метрики в ClickHouse.

### Функциональность

- Проверка доступности Logs API для указанного периода
- Создание запроса на выгрузку данных
- Ожидание обработки запроса (автоматическая проверка каждые 10 секунд)
- Скачивание данных по частям
- Автоматическое создание таблицы в ClickHouse
- Загрузка данных в ClickHouse
- Полное логирование всех операций
- Обработка ошибок на всех этапах

### Загружаемые поля

Программа загружает 40 полей визитов из Яндекс.Метрики:

- `visitID` - Уникальный ID визита
- `watchIDs` - ID просмотров
- `date` - Дата визита
- `isNewUser` - Признак нового пользователя
- `startURL` - Начальная страница
- `endURL` - Конечная страница
- `visitDuration` - Длительность визита в секундах
- `bounce` - Признак отказа
- `clientID` - ID клиента
- `goalsID` - ID достигнутых целей
- `goalsDateTime` - Время достижения целей
- `referer` - Реферер
- `deviceCategory` - Категория устройства
- `operatingSystemRoot` - Операционная система
- `DirectPlatform` - Площадка Яндекс.Директа
- `DirectConditionType` - Тип условия показа в Директе
- `UTMCampaign` - UTM метка кампании
- `UTMContent` - UTM метка содержания
- `UTMMedium` - UTM метка канала
- `UTMSource` - UTM метка источника
- `UTMTerm` - UTM метка ключевого слова
- `TrafficSource` - Источник трафика
- `pageViews` - Количество просмотров страниц
- `purchaseID` - ID покупок
- `purchaseDateTime` - Время покупок
- `purchaseRevenue` - Доход от покупок
- `purchaseCurrency` - Валюта покупок
- `purchaseProductQuantity` - Количество товаров в покупках
- `productsPurchaseID` - ID покупок товаров
- `productsID` - ID товаров
- `productsName` - Названия товаров
- `productsCategory` - Категории товаров
- `regionCity` - Город
- `impressionsURL` - URL показов товаров
- `impressionsDateTime` - Время показов товаров
- `impressionsProductID` - ID показанных товаров
- `AdvEngine` - Рекламная система
- `ReferalSource` - Реферальный источник
- `SearchEngineRoot` - Поисковая система
- `SearchPhrase` - Поисковая фраза

### Использование

#### С файлом конфигурации:

```bash
python load_ym_to_clickhouse.py --config config.json
```

#### С переменными окружения:

```bash
python load_ym_to_clickhouse.py
```

### Схема таблицы ClickHouse

Программа автоматически создает таблицу со следующей схемой:

```sql
CREATE TABLE database.table_name (
    visitID UInt64,
    watchIDs String,
    date Date,
    isNewUser UInt8,
    startURL String,
    endURL String,
    visitDuration UInt32,
    bounce UInt8,
    clientID UInt64,
    goalsID String,
    goalsDateTime String,
    referer String,
    deviceCategory String,
    operatingSystemRoot String,
    DirectPlatform String,
    DirectConditionType String,
    UTMCampaign String,
    UTMContent String,
    UTMMedium String,
    UTMSource String,
    UTMTerm String,
    TrafficSource String,
    pageViews UInt32,
    purchaseID String,
    purchaseDateTime String,
    purchaseRevenue String,
    purchaseCurrency String,
    purchaseProductQuantity String,
    productsPurchaseID String,
    productsID String,
    productsName String,
    productsCategory String,
    regionCity String,
    impressionsURL String,
    impressionsDateTime String,
    impressionsProductID String,
    AdvEngine String,
    ReferalSource String,
    SearchEngineRoot String,
    SearchPhrase String
) ENGINE = MergeTree()
ORDER BY (clientID, date)
SETTINGS index_granularity=8192
```

**Примечание:** Если таблица уже существует, она будет удалена и создана заново!

### Пример вывода

```
2024-02-15 19:30:00 - INFO - Starting Yandex Metrica to ClickHouse data load...
2024-02-15 19:30:00 - INFO - Configuration validated successfully
2024-02-15 19:30:01 - INFO - ClickHouse connection established
2024-02-15 19:30:01 - INFO - Checking Logs API availability...
2024-02-15 19:30:02 - INFO - Logs API available. Expected size: 15234567 bytes
2024-02-15 19:30:02 - INFO - Creating Logs API request...
2024-02-15 19:30:03 - INFO - Logs API request created with ID: 51234567
2024-02-15 19:30:03 - INFO - Waiting for request 51234567 to be processed...
2024-02-15 19:30:13 - INFO - Request status: processing
2024-02-15 19:31:23 - INFO - Request status: processed
2024-02-15 19:31:23 - INFO - Request processed successfully. Parts: 2
2024-02-15 19:31:23 - INFO - Downloading data from 2 parts...
2024-02-15 19:31:23 - INFO - Downloading part 0...
2024-02-15 19:31:45 - INFO - Part 0 downloaded: 50000 rows
2024-02-15 19:31:45 - INFO - Downloading part 1...
2024-02-15 19:32:07 - INFO - Part 1 downloaded: 50000 rows
2024-02-15 19:32:07 - INFO - Total rows downloaded: 100000
2024-02-15 19:32:07 - INFO - Creating ClickHouse table...
2024-02-15 19:32:08 - INFO - Dropped existing table (if existed): analytics.ym_visits
2024-02-15 19:32:08 - INFO - Created table: analytics.ym_visits
2024-02-15 19:32:08 - INFO - Uploading data to ClickHouse...
2024-02-15 19:32:25 - INFO - Successfully uploaded 100000 rows to analytics.ym_visits
2024-02-15 19:32:25 - INFO - Data load completed successfully!
```

## Программа визуализации данных

### Описание

`query_clickhouse.py` - программа для выполнения SQL запросов к ClickHouse с красивым форматированием результатов (как в Jupyter Notebook, но в терминале).

### Функциональность

- Выполнение произвольных SQL запросов
- Красивое форматирование таблиц с цветным выводом
- Автоматическое ограничение длинных строк
- Статистика по данным (типы колонок, количество null-значений, статистика по числовым колонкам)
- Интерактивный режим для последовательных запросов
- Поддержка различных форматов таблиц (grid, fancy_grid, simple, plain, html, latex)

### Режимы работы

#### 1. Запрос конкретной таблицы

```bash
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100
```

#### 2. Произвольный SQL запрос

```bash
python query_clickhouse.py --config config.json --query "SELECT date, COUNT(*) as visits FROM analytics.ym_visits GROUP BY date ORDER BY date LIMIT 100"
```

#### 3. С показом статистики

```bash
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100 --stats
```

#### 4. Интерактивный режим

```bash
python query_clickhouse.py --config config.json --interactive
```

В интерактивном режиме:
- Вводите SQL запросы и получайте результаты
- Команда `help` - показать справку
- Команда `exit` или `quit` - выход
- После каждого запроса можно посмотреть статистику (ответить 'y' на вопрос)

#### 5. Различные форматы вывода

```bash
# Красивая таблица с рамками (по умолчанию)
python query_clickhouse.py --config config.json --table analytics.ym_visits --format grid

# Более красивая таблица
python query_clickhouse.py --config config.json --table analytics.ym_visits --format fancy_grid

# Простая таблица
python query_clickhouse.py --config config.json --table analytics.ym_visits --format simple

# HTML формат (можно сохранить в файл)
python query_clickhouse.py --config config.json --table analytics.ym_visits --format html > result.html

# LaTeX формат (для вставки в научные работы)
python query_clickhouse.py --config config.json --table analytics.ym_visits --format latex
```

### Пример вывода

```
================================================================================
Query Results
================================================================================

Shape: 100 rows × 10 columns

+----+-------------+------------+---------------------+------------------+
|    |   visitID   |    date    |      startURL       | deviceCategory   |
+====+=============+============+=====================+==================+
|  0 | 12345678901 | 2024-01-15 | https://example...  | desktop          |
|  1 | 12345678902 | 2024-01-15 | https://example...  | mobile           |
|  2 | 12345678903 | 2024-01-15 | https://example...  | tablet           |
...
+----+-------------+------------+---------------------+------------------+

================================================================================
```

## Примеры использования

### Пример 1: Базовая загрузка данных за месяц

```bash
# Настройка конфигурации
cat > config.json << EOF
{
  "ym_token": "AQAAAAAxxxxx",
  "ym_counter_id": "12345678",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "ch_host": "https://rc1b-xxx.mdb.yandexcloud.net:8443",
  "ch_user": "user",
  "ch_pass": "password",
  "ch_cacert": "YandexInternalRootCA.crt",
  "ch_database": "analytics",
  "ch_table": "ym_visits_jan"
}
EOF

# Загрузка данных
python load_ym_to_clickhouse.py --config config.json
```

### Пример 2: Просмотр последних визитов

```bash
python query_clickhouse.py --config config.json \
  --query "SELECT date, clientID, startURL, deviceCategory FROM analytics.ym_visits_jan ORDER BY date DESC LIMIT 50"
```

### Пример 3: Анализ источников трафика

```bash
python query_clickhouse.py --config config.json \
  --query "SELECT TrafficSource, COUNT(*) as visits, AVG(visitDuration) as avg_duration FROM analytics.ym_visits_jan GROUP BY TrafficSource ORDER BY visits DESC LIMIT 20" \
  --stats
```

### Пример 4: Анализ конверсий

```bash
python query_clickhouse.py --config config.json \
  --query "SELECT date, COUNT(*) as total_visits, SUM(CASE WHEN purchaseID != '' THEN 1 ELSE 0 END) as purchases, SUM(CASE WHEN purchaseID != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as conversion_rate FROM analytics.ym_visits_jan GROUP BY date ORDER BY date" \
  --format fancy_grid
```

### Пример 5: Интерактивная работа

```bash
python query_clickhouse.py --config config.json --interactive

# В интерактивном режиме:
SQL> SELECT COUNT(*) as total_visits FROM analytics.ym_visits_jan
SQL> SELECT deviceCategory, COUNT(*) as visits FROM analytics.ym_visits_jan GROUP BY deviceCategory
SQL> exit
```

### Пример 6: Экспорт результатов в HTML

```bash
python query_clickhouse.py --config config.json \
  --query "SELECT date, TrafficSource, COUNT(*) as visits FROM analytics.ym_visits_jan GROUP BY date, TrafficSource ORDER BY date, visits DESC LIMIT 1000" \
  --format html > traffic_report.html
```

### Пример 7: Загрузка данных через переменные окружения

```bash
# Создайте файл .env
cat > .env << EOF
YM_TOKEN=AQAAAAAxxxxx
YM_COUNTER_ID=12345678
YM_START_DATE=2024-01-01
YM_END_DATE=2024-01-31
CH_HOST=https://rc1b-xxx.mdb.yandexcloud.net:8443
CH_USER=user
CH_PASS=password
CH_DATABASE=analytics
CH_TABLE=ym_visits
EOF

# Загрузите переменные
source .env

# Запустите программу
python load_ym_to_clickhouse.py
```

## Обработка ошибок

Программы включают полную обработку ошибок:

### Ошибки при загрузке данных

1. **Неверный токен или нет прав доступа**
   ```
   ERROR - Failed to check Logs API availability: 403 Client Error
   ```
   Решение: Проверьте токен и права доступа к счетчику

2. **Счетчик не найден**
   ```
   ERROR - Failed to check Logs API availability: 404 Not Found
   ```
   Решение: Проверьте правильность counter_id

3. **Данные не доступны для выгрузки**
   ```
   ERROR - Logs API request is not possible for the specified parameters
   ```
   Решение: Проверьте период дат, возможно данные еще не готовы или период слишком большой

4. **Таймаут обработки запроса**
   ```
   ERROR - Request processing timeout after 30 minutes
   ```
   Решение: Уменьшите период выгрузки или повторите позже

5. **Ошибка подключения к ClickHouse**
   ```
   ERROR - Failed to connect to ClickHouse: ...
   ```
   Решение: Проверьте параметры подключения (host, user, password, сертификат)

### Ошибки при запросах к ClickHouse

1. **Таблица не найдена**
   ```
   ERROR - Query execution failed: Table analytics.ym_visits doesn't exist
   ```
   Решение: Сначала загрузите данные с помощью load_ym_to_clickhouse.py

2. **Ошибка в SQL запросе**
   ```
   ERROR - Query execution failed: Syntax error: ...
   ```
   Решение: Проверьте синтаксис SQL запроса

3. **Нет прав доступа**
   ```
   ERROR - Query execution failed: Access denied
   ```
   Решение: Проверьте права пользователя в ClickHouse

## FAQ

### Q: Можно ли загружать данные за большой период (например, год)?

A: Да, но рекомендуется разбивать на периоды по 1-3 месяца. Большие периоды могут занять много времени на обработку в Logs API.

### Q: Можно ли запустить несколько загрузок параллельно?

A: Да, но учитывайте лимиты Logs API на количество одновременных запросов для одного счетчика.

### Q: Как обновить данные в существующей таблице?

A: Программа пересоздает таблицу при каждом запуске. Для добавления данных нужно модифицировать код, убрав DROP TABLE.

### Q: Поддерживаются ли данные типа "хиты"?

A: Текущая версия загружает только визиты. Для хитов нужно изменить параметр source='hits' и список полей.

### Q: Можно ли использовать программы в Windows?

A: Да, программы кроссплатформенные. Убедитесь, что установлен Python 3.8+ и все зависимости.

### Q: Где хранить конфигурацию с секретными данными?

A: Рекомендуется использовать файл config.json и добавить его в .gitignore. Для production можно использовать переменные окружения или менеджеры секретов.

### Q: Можно ли использовать программы в автоматическом режиме (cron)?

A: Да, программы полностью автоматические. Пример для cron:
```bash
0 2 * * * cd /path/to/project && /usr/bin/python3 load_ym_to_clickhouse.py --config config.json >> /var/log/ym_load.log 2>&1
```

### Q: Как посмотреть только определенные колонки?

A: Используйте SQL запрос с указанием нужных колонок:
```bash
python query_clickhouse.py --config config.json \
  --query "SELECT date, clientID, startURL FROM analytics.ym_visits LIMIT 100"
```

### Q: Можно ли экспортировать результаты в CSV?

A: Да, используйте перенаправление вывода с форматом simple:
```bash
python query_clickhouse.py --config config.json --query "..." --format simple > output.csv
```

### Q: Как обрабатываются NULL значения?

A: NULL значения отображаются как пустые строки в таблице. В статистике показывается количество NULL значений по каждой колонке.

## Поддержка

При возникновении проблем:

1. Проверьте логи программы (уровень INFO)
2. Убедитесь в правильности конфигурации
3. Проверьте доступность ClickHouse и Яндекс.Метрики API
4. Создайте issue в репозитории с описанием проблемы и логами

## Лицензия

См. LICENSE файл в репозитории.
