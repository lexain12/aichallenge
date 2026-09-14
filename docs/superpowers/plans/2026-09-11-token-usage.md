# Day 8: API token statistics

Approved design: show input (system + history + new question), output and total
for each API request. Use provider usage only; no tokenizer or cumulative sum.
Show optional reasoning tokens as a subset of output. Missing usage is unknown,
not zero. A single rolling footer below streaming output and above input shows waiting or latest statistics. Piped output emits a single final summary.

- [x] Extend TokenUsage with optional completion details; retain the latest usage
  event per call, reset on new attempts/clear, and never add repeated metadata.
- [x] Attach usage to completed assistant messages. Add message_usage table to
  existing SQLite databases without modifying old messages. Write answer and
  usage in one transaction; never send usage metadata in API message payloads.
- [x] Render one terminal status line for the current conversation; remove prior
  footer on input and omit per-message footers during replay. Show no-data on missing usage; show provider usage for failed calls
  when received, without persisting a failed answer.
- [x] Test old-schema compatibility, atomic rollback, duplicate/missing usage,
  exact values, wire payload exclusion and cross-process resume. Run all tests,
  clippy with denied warnings, rustfmt, diff check, and request review.

Validation: 47 tests passed. Clippy with warnings denied, rustfmt check and git diff check passed. Independent review found no concrete bugs.

Footer follow-up: real PTY checks using a terminal emulator passed at widths 80, 40 and 20, including wrapped input, tabs with NO_COLOR and input taller than the viewport. Old footers that have already scrolled outside the viewport are left in scrollback to avoid deleting unrelated terminal content.
