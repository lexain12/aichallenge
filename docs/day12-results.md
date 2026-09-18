# День 12: проверка персонализации по Markdown-профилю

Этот сценарий показывает код и поведение, которые нужны для задания Day 12.
Ответы реальной модели не фиксируются заранее: автоматические тесты проверяют
точные request payload, а видео показывает фактическую персонализацию.

## Что реализовано

```text
user ID → один Markdown-профиль → system block user_profile
```

- профиль хранится отдельно от generic long-term и working memory;
- SQLite является источником истины, а `.md` используется только для импорта;
- профиль автоматически подключается к каждому persistent-запросу пользователя;
- другой пользователь и in-memory Agent профиль не получают;
- текущий запрос и task-specific контекст приоритетнее мягких предпочтений;
- блок имеет `scope = user` и `compaction = exclude`;
- обычный диалог не изменяет профиль автоматически.

## Подготовка

Для чистого прогона используйте отдельную базу и debug-log:

```bash
mkdir -p /tmp/day12-demo
DB=/tmp/day12-demo/profiles.sqlite3
DEBUG=/tmp/day12-demo/debug.jsonl
```

В локальном `deepseek.toml` укажите реальный ключ и безопасный debug:

```toml
[context]
strategy = "summary"
keep_last_messages = 4

[debug]
log_path = "/tmp/day12-demo/debug.jsonl"
log_payloads = false
```

Создайте два произвольных Markdown-документа. Структура не является схемой —
это обычный текст:

```bash
printf '%s\n' \
  '# Alice' \
  '' \
  'Общайся кратко и по делу.' \
  'Для мобильных приложений предпочитай Android, Kotlin и Compose.' \
  'Предлагай минимально достаточную архитектуру.' \
  > '/tmp/day12-demo/Alice Profile.md'

printf '%s\n' \
  '# Bob' \
  '' \
  'Объясняй причины решений и сравнивай альтернативы.' \
  'Для мобильных приложений предпочитай Flutter и Dart.' \
  'Группируй код по пользовательским функциям.' \
  > '/tmp/day12-demo/Bob Profile.md'
```

## 1. Импортируем профиль Alice

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task mobile-app
```

В REPL:

```text
/profile import /tmp/day12-demo/Alice Profile.md
/profile
Спроектируй небольшое мобильное приложение для учёта привычек.
/exit
```

Проверяемые признаки:

- import сообщает `Imported profile · user: alice`;
- `/profile` печатает весь исходный Markdown;
- ответ применяет профиль без повторения предпочтений в текущем вопросе;
- профиль действует до создания dialog ID и не зависит от task ID.

## 2. Импортируем профиль Bob и повторяем тот же запрос

```bash
cargo run -- --config deepseek.toml --db "$DB" --user bob --task mobile-app
```

```text
/profile import /tmp/day12-demo/Bob Profile.md
/profile
Спроектируй небольшое мобильное приложение для учёта привычек.
/exit
```

Второй вопрос дословно совпадает с первым. Различие ответа должно следовать из
профиля: Alice получает краткое Android/Kotlin/Compose-направление, Bob — более
объяснительное Flutter/Dart-направление. Конкретные формулировки модели могут
меняться, поэтому корректность инъекции дополнительно закреплена integration-
тестом `profiles_persist_across_tasks_and_personalize_the_same_prompt`.

## 3. Проверяем SQLite и изоляцию пользователей

```bash
sqlite3 -header -column "$DB" \
  "SELECT user_id, content_markdown, updated_at FROM user_profiles ORDER BY user_id;"
```

Ожидаются ровно две строки: одна для `alice`, одна для `bob`. Профиль не
дублируется на каждый task или dialog.

Проверка количества:

```bash
sqlite3 "$DB" \
  "SELECT CASE WHEN count(*) = 2 AND count(DISTINCT user_id) = 2 THEN 1 ELSE 0 END FROM user_profiles;"
```

Команда должна вывести `1`.

## 4. Проверяем автоматическое подключение в другой задаче

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task another-task
```

```text
/profile
Предложи архитектуру мобильного приложения для заметок.
/exit
```

Профиль Alice выводится и применяется без повторного импорта: его адрес — только
`user ID`, тогда как working memory адресуется `user ID + task ID`.

## 5. Проверяем безопасный debug-log

После ordinary-запросов выполните:

```bash
jq -s -e '
  map(select(.event == "request_prepared" and .kind == "chat"))
  | (last // error("chat request_prepared record missing"))
  | .system_blocks as $blocks
  | (($blocks | map(select(.name == "user_profile"
                         and .scope == "user"
                         and .compaction == "exclude")) | length) == 1
     and (has("messages") | not))
' "$DEBUG"
```

Команда должна завершиться с кодом 0. Пустой лог и отсутствие ordinary-записи
дают ошибку. При `log_payloads = false` Markdown отсутствует:

```bash
if grep -q 'Alice\|Bob\|Android\|Flutter' "$DEBUG"; then
  echo 'profile content leaked into safe debug log' >&2
  exit 1
fi
```

`user_profile` может присутствовать в `system_block_names` обычного запроса, но
не должен присутствовать в compaction request:

```bash
jq -s -e '
  map(select(.event == "request_prepared" and .kind == "compaction"))
  | if length == 0 then true
    else all(.system_block_names | index("user_profile") == null) end
' "$DEBUG"
```

Здесь отсутствие compaction допустимо: низкий порог не является частью Day 12.
Автоматический Agent-тест принудительно запускает compaction и проверяет
исключение содержимого профиля из payload.

## 6. Проверяем замену и удаление

```bash
cargo run -- --config deepseek.toml --db "$DB" --user alice --task mobile-app
```

```text
/profile set Отвечай одним коротким абзацем и предпочитай Kotlin.
/profile
Предложи экран настроек приложения.
/profile clear
/profile
/exit
```

`set` полностью заменяет документ, поэтому следующий запрос получает только
новую версию. После `clear` выводится `Profile · user: alice · empty`, а
следующие запросы не содержат блока `user_profile`. Удаление профиля не стирает
conversation history и durable memory.

## 7. Проверяем ошибки импорта

```text
/profile set Эта версия должна сохраниться.
/profile import /tmp/day12-demo/missing.md
/profile
```

Ошибка печатается локально, REPL продолжает работу, а `/profile` показывает
прежнюю версию. Пустой и не-UTF-8 файл ведут себя так же. Ни одна profile-команда
не вызывает модель и не создаёт строку в `dialogs`.

## Автоматическая проверка кода

```bash
cargo test --test profile
cargo test --test agent profile
cargo test --test chat --test cli profile
```

Ключевые тесты проверяют реальные SQLite-записи и захваченные HTTP payload, а не
наличие строк в исходном коде.

## Сценарий видео, 60–90 секунд

1. Показать два Markdown-файла рядом: Alice и Bob.
2. Запустить Alice, выполнить `/profile import`, затем `/profile`.
3. Задать вопрос про приложение привычек и показать Android/Kotlin-направление.
4. Запустить Bob, импортировать его профиль и задать тот же вопрос.
5. Показать отличающийся Flutter/Dart-ответ.
6. Выполнить SQLite `SELECT`, показать две строки по `user_id`.
7. Показать debug-запись с `user_profile/user/exclude` без Markdown payload.
8. Завершить фразой: профиль — отдельный user-scoped provider поверх memory
   layers Day 11; task state и жёсткие invariants ещё не реализованы.

Не показывайте API-ключ и не включайте `debug.log_payloads = true` во время
записи.
