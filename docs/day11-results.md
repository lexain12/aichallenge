# День 11: сценарий проверки слоёв памяти

Этот документ — воспроизводимый сценарий записи, а не протокол уже выполненного
live-run. Здесь намеренно нет придуманных ответов модели, token usage и времени:
их нужно записать только после реального запуска с DeepSeek.

## Что проверяем

```text
conversation = dialog ID; task = user ID + task ID; user = user ID
```

- Conversation живёт в одном диалоге. Raw-сообщения сохраняются в SQLite, но
  старая часть контекста может быть заменена cumulative summary в запросе.
- Working memory адресуется парой `user ID + task ID`, переживает новые диалоги
  этой задачи и имеет `CompactionPolicy::Exclude`.
- Long-term memory адресуется только `user ID`, переживает новые задачи и
  диалоги этого пользователя и также имеет `CompactionPolicy::Exclude`.

Durable-записи меняются только явными `/remember task|user` и
`/forget task|user`. `/memory` и эти команды локальны: они не вызывают модель.

## Подготовка

Используйте отдельную пустую базу и отдельный debug-log:

```bash
cargo run -- --help
```

В `deepseek.toml` оставьте реальный ключ только локально и временно задайте:

```toml
[context]
strategy = "summary"
keep_last_messages = 2
compact_after_prompt_tokens = 1
summary_max_tokens = 512
facts_max_tokens = 512

[debug]
log_path = "/tmp/day11-debug.jsonl"
log_payloads = false
```

Для повторного прогона выберите новые пути либо заранее переименуйте старые
файлы. Ниже используются:

```bash
DB=/tmp/day11-memory.sqlite3
DEBUG=/tmp/day11-debug.jsonl
```

Низкий порог нужен только для демонстрации summary compaction. Он не является
рекомендацией для обычной работы.

## 1. Записываем один user fact и два task fact

Запустите `alice/telegram-bot`:

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task telegram-bot
```

В терминале выполните:

```text
/remember user response_language Russian
/remember task stack Rust
/remember task database SQLite
```

SQLite-проверка записей:

```bash
sqlite3 -header -column "$DB" \
  "SELECT scope_type, user_id, quote(task_id) AS task_id, key, value, updated_at FROM memory_entries ORDER BY scope_type, user_id, task_id, key;"
```

В таблице должна быть user-строка для `alice` с внутренним пустым `task_id` и
две task-строки для пары `alice/telegram-bot`.

## 2. Показываем Conversation, Working и Long-term

В той же сессии выполните:

```text
/memory
```

Проверяемые признаки:

```text
Conversation · dialog: new · strategy: summary · messages: 0 · summary boundary: 0 · sticky facts: 0
Working · database = SQLite
Working · stack = Rust
Long-term · response_language = Russian
```

До первого обычного сообщения dialog ещё имеет значение `new`: durable memory
адресуется независимо от dialog ID. Внутри каждого слоя ключи выводятся в
детерминированном порядке.

## 3. Проверяем ordinary request и безопасный debug-log

В той же сессии отправьте вопрос, но не переносите ответ модели в этот документ:

```text
Назови активный язык ответа, стек и базу данных из контекста.
```

После завершения ответа в другом терминале выполните:

```bash
jq -s -e '
  ((map(select(.event == "request_prepared" and .kind == "chat")) | last)
   // error("no chat request_prepared record"))
  | {kind, system_block_names, system_blocks, message_count, message_metadata,
     payload_logged: has("messages")}
' "$DEBUG"
```

Обычный ответ имеет точное значение `kind = "chat"`. Команда завершается с
ошибкой, если такой `request_prepared` отсутствует, поэтому пустой лог нельзя
ошибочно принять за успешную проверку.

В `system_block_names` блок `user_memory` должен идти раньше `task_memory`.
Структурные метаданные для них должны иметь точные значения:

```json
{"name":"user_memory","scope":"user","compaction":"exclude"}
{"name":"task_memory","scope":"task","compaction":"exclude"}
```

При `log_payloads = false` поле `messages` отсутствует, поэтому значения
`Russian`, `Rust` и `SQLite` в JSONL не записываются. Проверка этого свойства:

```bash
jq -s -e '[.[] | select(.event == "request_prepared") | has("messages")] | all(. == false)' "$DEBUG"
```

## 4. Показываем исключение durable memory из compaction

Отправьте ещё один обычный вопрос, чтобы после накопления raw-сообщений появился
eligible prefix для summary:

```text
Кратко повтори только названия технологий.
```

Не фиксируйте ответ и токены как заранее известные. Убедитесь, что в логе есть
реальный compaction request:

```bash
jq -c 'select(.event == "request_prepared" and .kind == "compaction") | {kind, system_block_names, system_blocks, message_count, message_metadata, payload_logged: has("messages")}' "$DEBUG" | tail -n 1
```

Первый такой запрос содержит служебный `summary_compactor`; последующие могут
также содержать conversation-блок `summary` с policy `include`. Ни один из них
не должен содержать `user_memory` или `task_memory`. Машинная проверка последней
compaction-записи:

```bash
jq -s -e '
  map(select(.event == "request_prepared" and .kind == "compaction"))
  | last
  | .system_block_names as $names
  | (($names | index("user_memory")) == null
     and ($names | index("task_memory")) == null)
' "$DEBUG"
```

Команда должна завершиться с кодом 0. Это проверяет executable policy, а не
поиск чувствительных значений в payload: payload намеренно отключён.

## 5. Новый диалог той же задачи сохраняет оба durable-слоя

Завершите первую сессию через `/exit`, затем снова запустите тот же scope без
`--resume`:

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task telegram-bot
```

Сразу выполните `/memory`. Ожидается `Conversation · dialog: new` с нулём
сообщений, обе Working-записи и Long-term-запись. Старые raw-сообщения не
воспроизводятся и не входят в новый conversation; они остаются в SQLite внутри
старого dialog ID.

Чтобы создать второй dialog ID и проверить инъекцию, отправьте один новый
вопрос, например `Какие настройки памяти активны?`, дождитесь ответа и завершите
сессию. Затем выполните:

```bash
sqlite3 -header -column "$DB" \
  "SELECT d.id, s.user_id, s.task_id, count(m.id) AS messages FROM dialogs d JOIN dialog_scopes s ON s.dialog_id = d.id LEFT JOIN messages m ON m.dialog_id = d.id GROUP BY d.id, s.user_id, s.task_id ORDER BY d.id;"
```

У двух диалогов один scope, но разные ID и независимые наборы raw-сообщений.

## 6. Другая задача Alice получает только long-term memory

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task another-task
```

Выполните `/memory`:

```text
Working · empty
Long-term · response_language = Russian
```

Отправьте `Какая память доступна в этой задаче?` и дождитесь ответа. Последняя
ordinary-запись debug-log должна содержать `user_memory`, но не `task_memory`:

```bash
jq -s -e '
  map(select(.event == "request_prepared" and .kind == "chat"))
  | last
  | .system_block_names as $names
  | (($names | index("user_memory")) != null
     and ($names | index("task_memory")) == null)
' "$DEBUG"
```

## 7. Другой пользователь не получает память Alice

```bash
cargo run -- --config deepseek.toml --db "$DB" --user bob --task telegram-bot
```

`/memory` должен показать:

```text
Working · empty
Long-term · empty
```

Отправьте `Какая память доступна этому пользователю?` и дождитесь ответа.
Последняя ordinary-запись не должна содержать ни одного durable-блока:

```bash
jq -s -e '
  map(select(.event == "request_prepared" and .kind == "chat"))
  | last
  | .system_block_names as $names
  | (($names | index("user_memory")) == null
     and ($names | index("task_memory")) == null)
' "$DEBUG"
```

## 8. Конфликт scope при resume останавливается локально

Найдите ID первого диалога Alice:

```bash
cargo run -- --config deepseek.toml --db "$DB" --list-dialogs
```

Подставьте его вместо `<ALICE_DIALOG_ID>`:

```bash
cargo run -- --config deepseek.toml --db "$DB" --resume <ALICE_DIALOG_ID> --user bob
```

Ожидаемая локальная ошибка определяется самим CLI:

```text
error: dialog belongs to user 'alice', not requested user 'bob'
```

Процесс завершается с ненулевым кодом до API-запроса. Для дополнительной
проверки убедитесь, что после команды в debug-log не появилась новая запись
`request_prepared`.

## Дополнительная SQLite-проверка изоляции

```bash
sqlite3 -header -column "$DB" \
  "SELECT scope_type, user_id, quote(task_id) AS task_id, key, value FROM memory_entries ORDER BY user_id, scope_type, task_id, key;"

sqlite3 -header -column "$DB" \
  "SELECT d.id, s.user_id, s.task_id, d.title, count(m.id) AS messages FROM dialogs d JOIN dialog_scopes s ON s.dialog_id = d.id LEFT JOIN messages m ON m.dialog_id = d.id GROUP BY d.id, s.user_id, s.task_id, d.title ORDER BY d.id;"
```

В `memory_entries` должны остаться только явно созданные записи Alice:
long-term `response_language` и working `stack`/`database` для
`alice/telegram-bot`. Новые task/user scopes сами по себе строк памяти не
создают.

## Что Day 11 оставляет следующим дням

Новые context provider-ы могут возвращать policy-bearing `SystemBlock` через
тот же интерфейс. Это подготовленная точка расширения для user profile (Day 12),
task state (Day 13), invariants (Day 14) и transition policy (Day 15). Сценарий
выше не проверяет эти функции, потому что в Day 11 они ещё не реализованы.
