# Yandex Metrica to ClickHouse

Автоматизированная загрузка данных из Яндекс.Метрики в ClickHouse с красивым отображением результатов.

## Ключевые возможности

✅ **Автоматическое определение полей** - программа сама определяет, какие поля доступны для вашего счетчика
✅ **Универсальность** - работает с любым счетчиком без ручной настройки полей
✅ **Полная обработка ошибок** - детальные сообщения об ошибках с подсказками
✅ **Красивый вывод данных** - форматированные таблицы как в Jupyter Notebook
✅ **Кросс-платформенность** - работает на macOS, Linux, Windows

## Новые возможности

Репозиторий теперь включает два готовых Python-скрипта для работы на macOS (и других ОС):

1. **load_ym_to_clickhouse.py** - загрузка данных визитов из Яндекс.Метрики в ClickHouse
2. **query_clickhouse.py** - выполнение запросов к ClickHouse с красивым форматированием (как в Jupyter Notebook)

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка конфигурации

Создайте файл `config.json` на основе `config.example.json`:

```bash
cp config.example.json config.json
```

Заполните `config.json` своими данными:
- `ym_token` - OAuth токен Яндекс.Метрики
- `ym_counter_id` - ID счетчика
- `start_date` / `end_date` - период выгрузки
- `ch_host` / `ch_user` / `ch_pass` - параметры подключения к ClickHouse
- `ch_database` / `ch_table` - имя базы и таблицы

### 2.1. Диагностика проблем (если возникают ошибки)

Если при загрузке данных возникает ошибка 400 Bad Request или другие проблемы, запустите скрипт диагностики:

```bash
python troubleshoot.py --config config.json
```

Этот скрипт проверит:
- Доступность API Яндекс.Метрики
- Правильность токена и прав доступа
- Доступность счетчика
- Корректность дат
- Доступность всех полей для вашего счетчика

### 3. Загрузка данных в ClickHouse

```bash
python load_ym_to_clickhouse.py --config config.json
```

Или через переменные окружения:

```bash
export YM_TOKEN=your_token
export YM_COUNTER_ID=123456
export YM_START_DATE=2024-01-01
export YM_END_DATE=2024-01-31
export CH_HOST=https://clickhouse.example.com:8443
export CH_USER=user
export CH_PASS=password
export CH_DATABASE=analytics
export CH_TABLE=ym_visits

python load_ym_to_clickhouse.py
```

### 4. Просмотр данных

Простой запрос первых 100 строк:

```bash
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100
```

С статистикой:

```bash
python query_clickhouse.py --config config.json --table analytics.ym_visits --limit 100 --stats
```

Интерактивный режим:

```bash
python query_clickhouse.py --config config.json --interactive
```

Произвольный запрос:

```bash
python query_clickhouse.py --config config.json --query "SELECT date, COUNT(*) as visits FROM analytics.ym_visits GROUP BY date ORDER BY date LIMIT 100"
```

## Загружаемые параметры из Яндекс.Метрики

Программа **автоматически определяет** доступные поля для вашего счетчика и загружает только их.

Начальный список включает 38 полей для типа выгрузки "визиты":

**Автоматическое определение полей:** Программа тестирует все поля и исключает недоступные (например, поля Яндекс.Директ, неактивные поля электронной коммерции и т.д.). Вам не нужно вручную редактировать список!

- ym:s:visitID - ID визита
- ym:s:watchIDs - ID просмотров
- ym:s:date - Дата
- ym:s:isNewUser - Новый пользователь
- ym:s:startURL - Начальная страница
- ym:s:endURL - Конечная страница
- ym:s:visitDuration - Длительность визита
- ym:s:bounce - Отказ
- ym:s:clientID - ID клиента
- ym:s:goalsID - ID целей
- ym:s:goalsDateTime - Время достижения целей
- ym:s:referer - Реферер
- ym:s:deviceCategory - Категория устройства
- ym:s:operatingSystemRoot - ОС
- ym:s:UTMCampaign - UTM кампания
- ym:s:UTMContent - UTM содержание
- ym:s:UTMMedium - UTM канал
- ym:s:UTMSource - UTM источник
- ym:s:UTMTerm - UTM термин
- ym:s:TrafficSource - Источник трафика
- ym:s:pageViews - Просмотры страниц
- ym:s:purchaseID - ID покупки
- ym:s:purchaseDateTime - Время покупки
- ym:s:purchaseRevenue - Доход от покупки
- ym:s:purchaseCurrency - Валюта покупки
- ym:s:purchaseProductQuantity - Количество товаров
- ym:s:productsPurchaseID - ID покупок товаров
- ym:s:productsID - ID товаров
- ym:s:productsName - Названия товаров
- ym:s:productsCategory - Категории товаров
- ym:s:regionCity - Город
- ym:s:impressionsURL - URL показов
- ym:s:impressionsDateTime - Время показов
- ym:s:impressionsProductID - ID товаров в показах
- ym:s:AdvEngine - Рекламная система
- ym:s:ReferalSource - Реферальный источник
- ym:s:SearchEngineRoot - Поисковая система
- ym:s:SearchPhrase - Поисковая фраза

## Обработка ошибок

Программы включают полную обработку ошибок:
- Проверка доступности Logs API
- Таймауты при ожидании обработки запросов
- Валидация конфигурации
- Обработка сетевых ошибок
- Логирование всех операций

---

# Получение данных через `Logs API`
`Logs API` позволяет выгрузить сырые данные со счетчика.

Документация по `Logs API` - https://yandex.ru/dev/metrika/doc/api2/logs/intro.html

Данные для этого кейса также доступны на Яндекс.Диске - https://disk.yandex.ru/d/sUmQmh_MnQWL4g?w=1

## Получаем токен
Для работы с API необходимо получить свой токен - https://yandex.ru/dev/oauth/doc/dg/tasks/get-oauth-token.html

Создаем приложение тут (указываем права для чтения в Яндекс.Метрике) - https://oauth.yandex.ru/client/new

Переходим по ссылке вида - `https://oauth.yandex.ru/authorize?response_type=token&client_id=<идентификатор приложения>`

Полученный токен можно сохранить в домашнюю директорию в файл `.yatoken.txt`

# Получение данных тестового счетчика с `Яндекс.Диска`
Для показательного очного семинара/воркшопа, или, если не получается по каким-то причинам получить данные из `LogsAPI`, были заготовлдены CSV с выгрудженными данными из `LogsAPI` тестового счетчика.

Хиты: https://disk.yandex.ru/d/MoSoXW6VzAP0bQ
Визиты: https://disk.yandex.ru/d/ywvr6YM3B95i0A

Нужно эти файлы скачать и положить в папочку репозитория для дальнейшей работы.

В файле `some_funcs` есть метод, `get_file_from_yadisk(file_link, file_name)` который копирует ровно 1 файл по ссылке из Яндекс.Диска `file_link` и сохраняет его на диск по пути `file_name`

Ожидается, что, выполняя ячейки ниже, вы будете находится в директории с репозиторием (`/yandex_metrika_cloud_case`)

# Загрузка данных в `ClickHouse`

## Подключение и настройка
https://cloud.yandex.ru/docs/managed-clickhouse/
(см. слайды)

##  Функции для интеграции с ClickHouse

В файле `some_funcs` есть класс `simple_ch_client` для работы с ClickHouse

Сначала надо создать экземпляр класса, инициализировав его начальными параметрами - хост, пользователь, пароль и путь к сертификату
`simple_ch_client(CH_HOST, CH_USER, CH_PASS, cacert)`

В классе есть 4 метода:
* `.get_version()` - получает текущую версию ClickHouse. Хороший способ проверить, что указанные при инициализации параметры работают
* `.get_clickhouse_data(query)` - выполняет запрос `query` и возвращает результат в текстовом формате
* `.get_clickhouse_df(query)` - выполняет запрос `query` и возвращает результат в виде DataFrame
* `.upload(table, content)` - загружает таблицу `content`, которая подается в текстовом формате в таблицу ClickHouse'а с именем `table`


## Проверяем ClickHouse
Используя заговленные выше переменные проверим доступ до сервера (как в документации https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect#connection-string)
Этот метод реализован в методе `.get_version()` класса для работы с ClickHouse
При успешном подключении не произойдет никакой ошибки при выполнении этого метода, и он сам вернет версию сервера ClickHouse (например `21.3.2.5`)
