# RomaGram Business — Railway + PostgreSQL

RomaGram — Telegram Bot API бот з підтримкою Telegram Business.

## Що є

- кілька підключених Telegram Business-акаунтів через одного бота;
- `Roma_1`, `Roma_2`, `Roma_3` та збереження username кожного акаунта;
- тільки приватні чати;
- збереження повідомлень, відправника, отримувача, типу, caption, Telegram file_id та raw JSON;
- окрема інформація про кожен Business-акаунт;
- лог у `HUB_CHAT_ID` про вхідні та вихідні повідомлення;
- відстеження `deleted_business_messages`;
- після видалення — повідомлення про видалення та копія доступного медіа;
- PostgreSQL замість локальних SQLite БД;
- готовність до запуску як Railway service через Dockerfile.

## Railway

1. Створи новий Railway Project.
2. Додай PostgreSQL через `+ New` → Database → PostgreSQL.
3. Додай цей репозиторій як окремий service через GitHub.
4. У Variables сервісу бота додай:
   - `BOT_TOKEN` — токен бота;
   - `DATABASE_URL` — reference variable на `Postgres.DATABASE_URL`;
   - `ROMAGRAM_HUB_CHAT_ID=8215352323`;
   - `LOG_LEVEL=INFO`.
5. Railway побачить `Dockerfile` і запустить `python bot.py`.
6. Перевір логи сервісу: має з'явитися `RomaGram запущено. Очікування Business updates...`.

PostgreSQL таблиці створюються автоматично при старті бота.

## Важливо

`DATABASE_URL` не треба записувати в код. Не додавай `.env` або токени в GitHub.

Якщо реальний токен бота вже десь публікувався, перед production-розгортанням створи новий токен через BotFather.

## Локальний запуск

Потрібні Python 3.13+, PostgreSQL та змінні з `.env.example`.

```bash
pip install -r requirements.txt
python bot.py
```

## Структура PostgreSQL

- `accounts` — підключені Business-акаунти;
- `messages` — архів повідомлень;
- `hub_logs` — загальний журнал;
- `pending_deletions` — видалення, що прийшли раніше за повідомлення;
- `settings` — налаштування Business-підключень.
