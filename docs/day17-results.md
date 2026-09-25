# Day 17 Results

Проверка от 2026-09-25. Здесь сохраняются только команды, статусы и безопасные
результаты; содержимое и названия чатов, Telegram ID, credentials и сам маркер
не записываются.

## Deterministic verification

До первого обращения к живому Telegram выполнены:

| Команда | Exit status | Результат |
| --- | --- | --- |
| `UV_CACHE_DIR=/private/tmp/aichallenge-task11-uv.331vOO uv run --offline --no-sync --project telegram_mcp pytest telegram_mcp/tests -q` | 0 | 37 passed |
| `cargo fmt --check` | 0 | Форматирование соответствует rustfmt |
| `cargo clippy --all-targets --all-features -- -D warnings` | 0 | Без предупреждений |
| `cargo test --all-targets --all-features` | 0 | 526 passed, 2 live-теста ignored |
| `git diff --check` | 0 | Ошибок whitespace нет |

Исходная команда `uv run --project telegram_mcp pytest telegram_mcp/tests -q`
завершилась с кодом 2: sandbox не разрешил доступ к стандартному кэшу `uv`.
Повтор с временным кэшем и `--offline` завершился с кодом 101 из-за panic
`system-configuration` в `uv` на macOS. Добавление `--no-sync` использовало уже
установленное окружение проекта и дало 37/37; ошибки тестов Python не было.

`tests/day17_live.rs` дополнительно проверяет fail-closed границу отправки:
другой адрес или текст, другой/неизвестный write-tool, неверные JSON arguments,
пропущенный/несовпадающий маршрут, отсутствующую read-only классификацию,
неправильную read-only аннотацию send, повтор после таймаута и новый call ID.
Маршрут передаётся от inner executor без разбора alias. Проверка маркера считает
только точное поле `text`, а не подстроку или соседние метаданные.
Эти проверки и явный opt-in входят в 12 детерминированных тестов нового файла.

### Исправления после ревью P1 / P2

Ошибка `delivery_unknown` теперь передаётся Python через строгий общий конверт
`{"mcp_error":{"version":1,"code":"delivery_unknown"}}`. Общая синтетическая
фикстура проверяется против настоящего in-memory Python MCP SDK и используется
в Rust-тесте цепочки HTTP Registry → Agent → SQLite. Регрессия подтверждает один
dispatch, безопасный `delivery_unknown` для модели и статус `uncertain` в аудите.
Неизвестные схемы/коды и произвольные тела ошибок превращаются в `mcp_tool_error`;
read-инструмент не может объявить неопределённость write.

Добавлены четыре теста последовательности приёмки. Успех требует чтения самой
моделью после разрешённой отправки: авторитетный маршрут `telegram/read_chat`,
ровно `chat="me", limit=100`, успешный результат и ровно один точный маркер.
Предварительное чтение, другая цель/лимит/маршрут, ошибочный или неподходящий
результат не засчитываются. Независимое чтение используется только для
диагностики. В `day17_live.rs` теперь 16 детерминированных тестов и 2 ignored.

Повторная проверка после исправлений:

| Команда | Exit status | Результат |
| --- | --- | --- |
| `UV_CACHE_DIR=/private/tmp/aichallenge-task11-uv.331vOO uv run --offline --no-sync --project telegram_mcp pytest telegram_mcp/tests -q` | 0 | 38 passed |
| `cargo fmt --check` | 0 | Чисто |
| `cargo clippy --all-targets --all-features -- -D warnings` | 0 | Без предупреждений |
| `cargo test --all-targets --all-features --quiet` с доступом хоста | 0 | 534 passed, 2 live-теста ignored |
| `git diff --check` | 0 | Чисто |

Первый повторный Rust-запуск в sandbox завершился с кодом 101: два существующих
workflow unit-теста не смогли открыть порт Wiremock (`Operation not permitted`).
Повтор с разрешённым доступом хоста прошёл полностью. При исправлении P1/P2 не
запускались Telegram-сервер, live-тесты или повторная проверка credentials.

После финальной fix wave детерминированная проверка была повторена ещё раз:
**43 Python-теста и 538 Rust-тестов прошли**, 2 live-теста остались ignored;
`cargo fmt --check`, строгий Clippy и `git diff --check` завершились с кодом 0.

## Live Telegram MCP

**PASS.** После финальных исправлений read-only проверка выполнена повторно.
Проверка наличия переменных показала, что три Telegram-переменные
присутствуют; значения не выводились. Сервер запущен отдельно командой:

```bash
UV_CACHE_DIR=/private/tmp/aichallenge-task11-uv.331vOO uv run --offline --no-sync --project telegram_mcp telegram-mcp > /dev/null 2>&1
```

Запуск в sandbox завершился с кодом 3 (`Operation not permitted`). После
разрешённого запуска с доступом хоста сервер слушал только loopback.
stdout/stderr сервера были подавлены без сохранения в файл.

```bash
RUN_LIVE_TELEGRAM_TESTS=1 cargo test --test day17_live rust_registry_lists_and_reads_saved_messages -- --ignored --nocapture
```

Exit status: **0**, 1 passed. Настоящий Rust Registry выполнил initialize и
discovery. Обнаружены ровно `telegram__list_chats`, `telegram__read_chat`,
`telegram__send_message` с ожидаемой read-only классификацией.
`list_chats(limit=5)` и `read_chat(chat="me", limit=5)` завершились успешно;
структура результата чтения проверена без вывода payload.

После обеих live-проверок сервер остановлен через Ctrl+C (exit 130);
дополнительная проверка TCP подтвердила, что локальный порт 8000 больше не
слушает.

## Live DeepSeek tool loop

**PASS.** После появления `DEEPSEEK_API_KEY` в environment команда выполнена
ровно один раз; значение ключа не выводилось, не передавалось аргументом и не
сохранялось:

```bash
RUN_LIVE_TELEGRAM_TESTS=1 cargo test --test day17_live deepseek_agent_sends_only_to_saved_messages -- --ignored --nocapture
```

Exit status: **0**, 1 passed. DeepSeek последовательно вызвал
`telegram__list_chats`, `telegram__read_chat`, `telegram__send_message` и
повторный `telegram__read_chat`; все четыре вызова завершились успешно.
Выполнена ровно одна попытка записи, автоматического повтора не было.
Независимая проверка нашла уникальный маркер ровно один раз среди последних
100 Saved Messages, а observer подтвердил успешное модельное post-send чтение.
Маркер не удалялся; его текст не записан в этот документ.

## Limitations

Живой тест использует Agent в памяти и не сохраняет real Telegram payload или
ответ модели в SQLite/debug-файл. Аудит, persistence и recovery проверяются
детерминированными тестами; live-проверки восстановления SQLite здесь нет.
Защитная обёртка относится к приёмочному тесту. Обычный CLI не ограничивает
пользователя «Избранным» и не запрашивает подтверждение перед разрешённой моделью
отправкой. Лимит одной попытки действует в рамках одного запуска send-теста.
Live-тест не проверяет SQLite persistence/restart с реальными Telegram-данными;
эти пути покрыты синтетическими тестами. Telegram, DeepSeek и сеть остаются
внешними зависимостями. После успешной единственной отправки live send-тест
повторно не запускался.

Финальное re-review отдельно обнаружило незакрытую privacy-границу embedding:
заранее настроенный handler на дочернем logger MCP SDK может получить сырой
`ToolError` до родительского санитайзера. Обычный standalone запуск, использованный
выше, этот handler не устанавливает, но ветка не считается merge-ready до
исправления данного случая.
