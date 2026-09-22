# Day 13: workflow state machine — результаты

Проверено 22 сентября 2026 года в worktree Day 13. В persistent CLI задача
проходит проверяемые кодом этапы, сохраняется в SQLite и восстанавливается
после отмены или перезапуска. В Task 12 добавлены четыре приёмочных сценария
и проверка совместимости сохранённого JSON. Изменения production-кода здесь
ограничены структурным исправлением Clippy; новых правил переходов не добавлено.

## Приёмочные сценарии

| Тест | Что проверяет |
| --- | --- |
| `workflow_acceptance_complete_lifecycle_is_durable_ordered_and_stage_isolated` | CLI и настоящий SQLite: human start → planning answer → human PlanningCompleted → execution answer → controller Continue → execution answer → controller ExecutionCompleted → validation answer → controller ValidationFailed → repair answer → human ExecutionCompleted → validation answer → controller ValidationPassed → done |
| `acceptance_forbidden_phase_skips_preserve_the_exact_committed_state` | Planning→validation, execution→done и done→execution отклоняются без обычного HTTP, handoff, checker, новой стадии или изменения всей сохранённой task projection, включая version |
| `acceptance_restart_recovers_once_then_human_continues_the_stored_execution_step` | Оборванный после durable answer checker получает одну recovery-попытку; повторный recovery ничего не делает; controller input не создаётся; человеческий `continue` использует сохранённые план, шаг и checkpoint |
| `workflow_acceptance_two_tasks_keep_only_current_stage_and_projected_checkpoint` | Две последовательные задачи, реальные CLI profile/memory команды и sticky facts: стабильный порядок system-блоков, только protocol текущей стадии и принятая handoff-проекция |
| `transition_processing_result_preserves_existing_json_shape` | Старый `decision=transition` audit JSON декодируется типизированно и сериализуется без изменения формы после boxing большого payload |

Полный lifecycle выполняет 19 HTTP-запросов к локальному wiremock, из них
6 обычных ответов. Стадии строго идут с sequence 1…6:
`planning, execution, validation, execution, validation, done`. Повторный
execution имеет новый ID. Пять переходов упорядочены; первые три полные записи
ledger остаются побайтно теми же после последнего человеческого перехода.
Итоговая version равна 8. Повторный запуск CLI показывает все полные ответы,
но скрывает controller continuation и текст synthetic transition.

Leak-сценарий использует отдельные markers для raw-ответов первой задачи,
её full handoff, старого planning второй задачи, controller, профиля и обоих
durable memory-слоёв. Финальный execution-запрос содержит base → profile →
user memory → memory-task → current workflow state → current-stage facts →
current-stage protocol. Проверены summary/decisions/open_issues принятого
checkpoint; поля полного handoff (`completed_step_ids`, `plan_changes`) не
расширяются в prompt. Controller остаётся в protocol, но отсутствует во всех
четырёх facts-запросах. Старые raw-сообщения и старые facts в запрос не попадают.

## Команды и фактические результаты

```bash
cargo test --test cli workflow_acceptance -- --nocapture
cargo test --test workflow_engine acceptance_forbidden -- --nocapture
cargo test --test agent acceptance_restart -- --nocapture
cargo test --test workflow_store transition_processing_result_preserves -- --nocapture
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
git diff --check
```

Focused-проверки: соответственно 2, 1, 1 и 1 тест, все PASS.
Полный suite: **365 passed, 0 failed** (19 unit и 346 integration).
Из них Agent — 58, CLI — 31, workflow engine — 53, workflow store — 77.
Форматирование и strict Clippy проходят без warnings; `git diff --check` чист.

Четыре сценария сразу прошли на реализованном поведении. Чувствительность
подтверждена временными регрессиями и последующим восстановлением кода:
открытие controller в replay ломает lifecycle; отключение фильтра controller
ломает facts/lifecycle второй задачи; разрешение planning→validation вызывает
запрещённый handoff; отключение recovery-coercion пытается диспетчеризовать
controller и ломает restart-тест. Все эти изменения удалены, повторный
focused-прогон зелёный. Это mutation RED/GREEN существующего поведения,
а не заявление, что четыре новых функциональных дефекта были найдены.

Первый полный прогон обнаружил существующую гонку в
`interrupt_discards_partial_work_pauses_exactly_once_and_never_synthesizes_a_task`:
`Child::wait` закрывал stdin, и EOF иногда выигрывал у SIGINT. Это подтверждено
локальным исходником Rust std (`drop(self.stdin.take())` в `Child::wait`). Тест
теперь держит stdin открытым через уже имеющийся bounded wait helper; отдельная
проверка и следующий полный прогон прошли. Логика CLI не менялась.

Clippy сначала обнаружил крупные `PauseOutcome::Paused` и
`ProcessingResult::Transition`, а после их уменьшения —
`ProcessingOutcome::Controller`. Их payload помещён в `Box`; audit JSON
сохранил прежнюю форму. Вложенная проверка source fingerprint объединена,
test mutex guard ограничен лексическим блоком до `.await`. Новых `allow`
атрибутов нет.

## Архитектурные проверки

Выполнены прямые аудиты:

```bash
rg -n "set_phase|idempotency_key" src tests
rg -n "ProtocolSource::Controller|source = 'controller'|source='controller'" src
rg -n "dialog_context|dialog_facts" src/workflow_context.rs src/workflow_engine.rs
```

Первый и третий поиск не нашли совпадений (exit 1 означает отсутствие
совпадений). Публичного setter фазы и пользовательского idempotency key нет;
managed context не читает dialog-wide summary/facts. Второй поиск показывает
явный facts-фильтр, decoding provenance, проверку происхождения при replay
controller и ограничение SQL-схемы. Отдельно проверен human-only фильтр
`DialogStore::load`; durable memory и профиль меняются явными командами.

Человеческий transition проходит strict `HumanIntentDto` →
`WorkflowInputHandler` → `StateMachine::authorize` → handoff →
`commit_stage_change`. Controller проходит strict `ControllerDecisionDto` →
проверку patch и reducer → pipeline/budget/recovery guards → тот же handler.
В обоих случаях SQLite `IMMEDIATE`-транзакция заново вычисляет authorization
по event и сравнивает её с переданной; затем атомарно сохраняет input,
новую стадию, ledger, projection, version и processing result. Произвольная
`to_phase` из модели не принимается. Task identity, version и processing
attempt ограждают commit от устаревших результатов и гонок.

Проверка накопленного изменения от исходного `4271357` охватила взаимодействие
конфигурации/model adapter, reducer/parser, workflow store/migrations/replay,
Agent/context/facts, CLI cancellation/status и redacted diagnostics; это
не только проверка файлов Task 12. Сохранены правила одного unfinished task,
human-only restart/replan/new-task, stage isolation и append-only переходов.

## Примеры для оператора и ограничения

```text
cargo run -- --user alice --task parser-project
Спроектируй парсер и составь план с критериями проверки.
/task
План принят, переходи к выполнению.
```

Во время работы нажать Ctrl+C. Затем:

```text
cargo run -- --resume-last
/task
continue
```

Пауза сохраняет phase/stage ID. Restore может завершить advisory processing,
но не активирует задачу и не делает обычные автономные вызовы. `continue`
сохраняет план; явный replan создаёт новый planning той же задачи. После
`done` только человеческое сообщение может начать следующую задачу.
`--task` всё ещё адресует память проекта, `/task` показывает workflow state.
Все десять ключей `[workflow]`, точные defaults и бюджет описаны в
[README](../README.md#настройки-workflow).

Намеренные границы текущей реализации:

- Проверки используют локальные заданные ответы моделей. Реальный DeepSeek API
  и качество его интерпретации здесь не проверялись.
- Checker advisory: полный ответ уже показан и сохранён; ошибочное или
  невалидное предложение не отменяет этот ответ, но не меняет phase.
- Evidence проверяется структурно; CLI не запускает инструменты, реальные
  тесты или независимого валидатора истинности model-provided evidence.
- Бюджет 8 автономных ответов и 20 000 tokens ограничивает следующие вызовы;
  текущий вызов и обязательный checker могут превысить границу. Missing usage
  останавливает автономию. Версионный fingerprint не ловит семантический цикл
  через разные versions; для него остаётся turn limit.
- Максимум две processing-попытки; background worker, blocking validation,
  approval UI и дополнительные transition policies не реализованы.
- Сохранённые ответы и audit остаются в SQLite; stage isolation относится к
  сборке контекста, а не удалению данных. Полные payload-логи включаются явно.
