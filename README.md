# DeepSeek CLI

Небольшой интерактивный клиент DeepSeek на Rust. Ответ печатается по мере
генерации, а история текущего диалога хранится только в памяти процесса.

## Сборка

```bash
cargo build --release
```

Готовый исполняемый файл: `target/release/deepseek-cli`. При желании его можно
установить в Cargo bin-каталог:

```bash
cargo install --path .
```

## Настройка

Создайте локальный конфиг из примера:

```bash
cp deepseek.example.toml deepseek.toml
```

Вставьте API-ключ в `api_key`. Файл `deepseek.toml` исключён из Git, однако его
всё равно следует считать секретным и не пересылать другим людям.

Ключ также можно передать переменной окружения; она имеет приоритет над файлом:

```bash
DEEPSEEK_API_KEY='your-key' deepseek-cli
```

Доступные параметры:

```toml
api_key = "replace-with-your-key"
base_url = "https://api.deepseek.com"
model = "deepseek-v4-flash"
system_prompt = "You are a helpful assistant."
temperature = 1.0
max_tokens = 4096
timeout_seconds = 120
```

Кроме ключа, все поля необязательны и принимают показанные значения по
умолчанию. Другой файл можно выбрать явно:

```bash
deepseek-cli --config /path/to/config.toml
```

## Использование

```text
$ deepseek-cli
you> Объясни ownership в Rust одним абзацем
assistant> ...ответ появляется постепенно...
you> /exit
```

Команды:

- `/clear` — очистить историю текущего сеанса;
- `/exit` или `/quit` — выйти;
- Ctrl+D — выйти.

Если запрос оборвался или API вернул ошибку, незавершённая пара сообщений не
добавляется в историю.
