use std::io::{self, IsTerminal, Write};

use crate::agent::WorkflowStatus;
use crate::client::TokenUsage;
use crate::context::{ContextStats, UsageTotals};
use crate::memory::{DurableMemoryScope, MemorySnapshot, RequestScope};
use crate::profile::UserProfile;
use base64::{Engine, engine::general_purpose::STANDARD};
use terminal_size::{Height, Width, terminal_size};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const RESET: &str = "\x1b[0m";
const USER_STYLE: &str = "\x1b[48;2;20;45;80m\x1b[38;2;235;245;255m";
const ASSISTANT_STYLE: &str = "\x1b[48;2;15;65;45m\x1b[38;2;235;255;245m";
const SYSTEM_STYLE: &str = "\x1b[48;2;55;55;55m\x1b[38;2;245;245;245m";
const ERROR_STYLE: &str = "\x1b[48;2;90;25;25m\x1b[38;2;255;235;235m";
const HORSE_GIF: &[u8] = include_bytes!("../assets/horse-loader-dark.gif");

#[derive(Clone, Copy)]
pub enum BlockStyle {
    User,
    Assistant,
    System,
    Error,
}

impl BlockStyle {
    fn ansi(self) -> &'static str {
        match self {
            Self::User => USER_STYLE,
            Self::Assistant => ASSISTANT_STYLE,
            Self::System => SYSTEM_STYLE,
            Self::Error => ERROR_STYLE,
        }
    }
}

/// Terminal-aware rendering that keeps pipes free from ANSI escape sequences.
#[derive(Clone, Copy)]
pub struct TerminalUi {
    styled: bool,
    interactive: bool,
    inline_images: bool,
}

impl TerminalUi {
    pub fn write_workflow_status<W: Write>(
        &self,
        writer: &mut W,
        status: Option<&WorkflowStatus>,
    ) -> io::Result<()> {
        let Some(status) = status else {
            return self.write_block(
                writer,
                BlockStyle::System,
                "No workflow task in this dialog.",
            );
        };
        let current_step = status.current_step_id.as_deref().unwrap_or("none");
        let expected_action = status.expected_action.as_deref().unwrap_or("none");
        let processing = status.processing.map_or("none", processing_status_name);
        let goal_label = if status.phase == crate::workflow::TaskPhase::GoalDefinition {
            "working"
        } else {
            "approved"
        };
        let proposal = status
            .goal_proposal_message_id
            .map_or_else(|| "none".to_owned(), |id| id.to_string());
        self.write_block(
            writer,
            BlockStyle::System,
            &format!(
                "Workflow task #{} · ID: {}\nPhase: {} · status: {}\nGoal ({goal_label}, revision {}): {}\nCurrent proposal message: {}\nPlan revision: {} · current step: {}\nExpected action: {}\nStage sequence: {} · processing: {}",
                status.ordinal,
                status.task_id.0,
                task_phase_name(status.phase),
                task_status_name(status.status),
                status.goal_revision,
                status.goal,
                proposal,
                status.plan_revision,
                current_step,
                expected_action,
                status.stage_sequence,
                processing,
            ),
        )
    }

    pub fn write_profile<W: Write>(
        &self,
        writer: &mut W,
        user_id: &str,
        profile: Option<&UserProfile>,
    ) -> io::Result<()> {
        let message = profile.map_or_else(
            || format!("Profile · user: {user_id} · empty"),
            |profile| format!("Profile · user: {user_id}\n{}", profile.content_markdown()),
        );
        self.write_block(writer, BlockStyle::System, &message)
    }

    pub fn write_memory<W: Write>(
        &self,
        writer: &mut W,
        scope: &RequestScope,
        stats: ContextStats,
        snapshot: &MemorySnapshot,
        filter: Option<DurableMemoryScope>,
    ) -> io::Result<()> {
        if filter.is_none() {
            let dialog = scope
                .dialog_id()
                .map_or_else(|| "new".to_owned(), |id| format!("#{id}"));
            let active_stage = stats.stage_message_count.map_or_else(String::new, |count| {
                format!(" · current stage messages: {count}")
            });
            self.write_block(writer, BlockStyle::System, &format!(
                "Conversation · dialog: {dialog} · strategy: {} · messages: {}{active_stage} · summary boundary: {} · sticky facts: {}",
                stats.strategy.as_str(), stats.full_message_count, stats.covered_message_count, stats.facts_count,
            ))?;
        }
        for (layer, entries) in [
            (DurableMemoryScope::Task, snapshot.task_entries()),
            (DurableMemoryScope::User, snapshot.user_entries()),
        ] {
            if filter.is_some_and(|selected| selected != layer) {
                continue;
            }
            if entries.is_empty() {
                self.write_block(
                    writer,
                    BlockStyle::System,
                    &format!("{} · empty", layer.label()),
                )?;
            } else {
                for (key, value) in entries {
                    self.write_block(
                        writer,
                        BlockStyle::System,
                        &format!("{} · {key} = {value}", layer.label()),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Compact, dim metadata line; pipes and NO_COLOR receive plain text.
    pub fn write_status<W: Write>(&self, writer: &mut W, text: &str) -> io::Result<()> {
        let mut block = FullWidthBlock::new(writer, self.styled, current_width(), "\x1b[2m", "")?;
        let text = if self.interactive {
            fit_line(text, current_width().saturating_sub(1))
        } else {
            text.to_owned()
        };
        block.write_text(&text)?;
        block.finish()
    }

    pub fn is_interactive(&self) -> bool {
        self.interactive
    }

    /// Delete the footer above the submitted input, preserving the input itself.
    pub fn erase_usage_before_input<W: Write>(
        &self,
        writer: &mut W,
        input: &str,
    ) -> io::Result<()> {
        if self.interactive {
            let rows = input_rows(input, current_width());
            // Avoid deleting an unrelated row if the footer scrolled offscreen.
            if rows + 1 >= current_height() {
                return Ok(());
            }
            write!(writer, "{RESET}\x1b[{}A\r\x1b[1M\x1b[{rows}B\r", rows + 1)?;
            writer.flush()?;
        }
        Ok(())
    }

    pub fn start_response<'a, W: Write>(
        &self,
        writer: &'a mut W,
    ) -> io::Result<FullWidthBlock<'a, W>> {
        let mut block = self.start_block(writer, BlockStyle::Assistant, "")?;
        block.live_status = self.interactive;
        block.waiting_for_text = true;
        block.inline_images = self.inline_images;
        // Enable cursor tracking before the prefix, including with NO_COLOR.
        block.write_content("assistant> ")?;
        block.flush_pending_grapheme()?;
        block.draw_live_status()?;
        block.writer.flush()?;
        Ok(block)
    }

    pub fn write_usage<W: Write>(
        &self,
        writer: &mut W,
        usage: Option<TokenUsage>,
    ) -> io::Result<()> {
        let text = match usage {
            None => "Токены · нет данных API".to_owned(),
            Some(usage) => {
                let mut text = format!(
                    "Токены · Вход: {} · Выход: {} · Всего: {}",
                    usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                );
                if let Some(reasoning) = usage
                    .completion_tokens_details
                    .and_then(|details| details.reasoning_tokens)
                {
                    text.push_str(&format!(" · Рассуждения: {reasoning}"));
                }
                text
            }
        };
        self.write_status(writer, &text)
    }

    pub fn write_context_stats<W: Write>(
        &self,
        writer: &mut W,
        stats: ContextStats,
    ) -> io::Result<()> {
        self.write_block(
            writer,
            BlockStyle::System,
            &format!("Стратегия · {}", stats.strategy.as_str()),
        )?;
        let history_label = match stats.stage_message_count {
            Some(count) => format!(
                "транскрипт: {} · текущий этап: {count}",
                stats.full_message_count
            ),
            None => format!("полная история: {}", stats.full_message_count),
        };
        let context = match stats.strategy {
            crate::config::ContextStrategy::Summary => format!(
                "Контекст · {history_label} · покрыто summary: {} · дословно: {}",
                stats.covered_message_count, stats.raw_message_count
            ),
            crate::config::ContextStrategy::SlidingWindow => format!(
                "Контекст · {history_label} · в запросе: {}",
                stats.selected_message_count
            ),
            crate::config::ContextStrategy::StickyFacts => format!(
                "Контекст · {history_label} · в запросе: {} · facts: {} · facts до: {}",
                stats.selected_message_count, stats.facts_count, stats.facts_covered_message_count
            ),
            crate::config::ContextStrategy::Branching => match stats.branch_group_id {
                Some(group) => format!(
                    "Контекст · {history_label} · в запросе: {} · диалог: #{} · группа: #{}",
                    stats.selected_message_count,
                    stats.dialog_id.unwrap_or_default(),
                    group
                ),
                None => format!(
                    "Контекст · {history_label} · в запросе: {} · диалог: #{} · группа не создана",
                    stats.selected_message_count,
                    stats.dialog_id.unwrap_or_default()
                ),
            },
        };
        self.write_block(writer, BlockStyle::System, &context)?;
        let (answer_label, summary_label, facts_label, api_label) =
            if stats.stage_message_count.is_some() {
                (
                    "Ответы этапа",
                    "Summary этапа",
                    "Facts этапа",
                    "API этапа всего",
                )
            } else {
                ("Ответы", "Summary", "Facts", "API всего")
            };
        self.write_block(
            writer,
            BlockStyle::System,
            &format_usage_totals(answer_label, stats.ordinary_usage),
        )?;
        self.write_block(
            writer,
            BlockStyle::System,
            &format_usage_totals(summary_label, stats.compaction_usage),
        )?;
        self.write_block(
            writer,
            BlockStyle::System,
            &format_usage_totals(facts_label, stats.facts_usage),
        )?;
        self.write_block(
            writer,
            BlockStyle::System,
            &format!(
                "{api_label} · {}",
                stats
                    .ordinary_usage
                    .total_tokens()
                    .saturating_add(stats.compaction_usage.total_tokens())
                    .saturating_add(stats.facts_usage.total_tokens())
            ),
        )
    }

    pub fn stdout() -> Self {
        Self::new(io::stdout().is_terminal() && io::stdin().is_terminal())
    }

    pub fn stderr() -> Self {
        Self::new(io::stderr().is_terminal())
    }

    fn new(is_terminal: bool) -> Self {
        let terminal_supports_color = !matches!(std::env::var("TERM").as_deref(), Ok("dumb"));
        let color_allowed = std::env::var_os("NO_COLOR").is_none() && terminal_supports_color;
        Self {
            styled: is_terminal && color_allowed,
            interactive: is_terminal && terminal_supports_color,
            inline_images: is_terminal
                && terminal_supports_color
                && std::env::var("TERM_PROGRAM").as_deref() == Ok("iTerm.app")
                && std::env::var_os("TMUX").is_none()
                && std::env::var_os("STY").is_none(),
        }
    }

    pub fn write_input_prompt<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if self.styled {
            write!(writer, "{USER_STYLE}")?;
        }
        write!(writer, "you> ")?;
        writer.flush()
    }

    /// Replaces the terminal-echoed input with a full-width colored block.
    pub fn complete_input<W: Write>(&self, writer: &mut W, input: &str) -> io::Result<()> {
        if !self.styled {
            return Ok(());
        }

        let width = current_width();
        let occupied_rows = input_rows(input, width);
        if occupied_rows >= current_height() {
            write!(writer, "{RESET}")?;
            return writer.flush();
        }
        write!(writer, "{RESET}\x1b[{occupied_rows}A\r")?;

        let mut block = FullWidthBlock::new(writer, true, width, USER_STYLE, "")?;
        block.write_text("you> ")?;
        block.write_text(input)?;
        block.finish()
    }

    pub fn finish_empty_prompt<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if self.styled {
            write!(writer, "{RESET}\r\n")
        } else {
            writeln!(writer)
        }
    }

    /// Reset a response block whose owning future was cancelled and dropped.
    pub fn finish_interrupted_response<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if self.styled {
            write!(writer, "{RESET}\r\n")?;
        } else {
            writeln!(writer)?;
        }
        writer.flush()
    }

    pub fn start_block<'a, W: Write>(
        &self,
        writer: &'a mut W,
        style: BlockStyle,
        prefix: &str,
    ) -> io::Result<FullWidthBlock<'a, W>> {
        FullWidthBlock::new(writer, self.styled, current_width(), style.ansi(), prefix)
    }

    pub fn write_block<W: Write>(
        &self,
        writer: &mut W,
        style: BlockStyle,
        text: &str,
    ) -> io::Result<()> {
        let mut block = self.start_block(writer, style, "")?;
        block.write_text(text)?;
        block.finish()
    }
}

fn task_phase_name(phase: crate::workflow::TaskPhase) -> &'static str {
    match phase {
        crate::workflow::TaskPhase::GoalDefinition => "goal_definition",
        crate::workflow::TaskPhase::Planning => "planning",
        crate::workflow::TaskPhase::Execution => "execution",
        crate::workflow::TaskPhase::Validation => "validation",
        crate::workflow::TaskPhase::Done => "done",
    }
}

fn task_status_name(status: crate::workflow::TaskStatus) -> &'static str {
    match status {
        crate::workflow::TaskStatus::Active => "active",
        crate::workflow::TaskStatus::Paused => "paused",
    }
}

fn processing_status_name(status: crate::workflow_store::ProcessingStatus) -> &'static str {
    match status {
        crate::workflow_store::ProcessingStatus::Pending => "pending",
        crate::workflow_store::ProcessingStatus::Processing => "processing",
        crate::workflow_store::ProcessingStatus::Completed => "completed",
        crate::workflow_store::ProcessingStatus::Failed => "failed",
    }
}

fn format_usage_totals(label: &str, usage: UsageTotals) -> String {
    let mut text = format!(
        "{label} · вход: {} · выход: {} · всего: {}",
        usage.prompt_tokens(),
        usage.completion_tokens(),
        usage.total_tokens()
    );
    if usage.missing_usage_count() > 0 {
        text.push_str(&format!(
            " · без данных API: {}",
            usage.missing_usage_count()
        ));
    }
    text
}

pub struct FullWidthBlock<'a, W: Write> {
    writer: &'a mut W,
    styled: bool,
    width: usize,
    column: usize,
    pending_grapheme: String,
    ansi_style: &'static str,
    last_was_newline: bool,
    live_status: bool,
    status_visible: bool,
    waiting_for_text: bool,
    inline_images: bool,
    closed: bool,
}

impl<'a, W: Write> FullWidthBlock<'a, W> {
    fn new(
        writer: &'a mut W,
        styled: bool,
        width: usize,
        ansi_style: &'static str,
        prefix: &str,
    ) -> io::Result<Self> {
        if styled {
            write!(writer, "{ansi_style}")?;
        }
        let mut block = Self {
            writer,
            styled,
            width,
            column: 0,
            pending_grapheme: String::new(),
            ansi_style: if styled { ansi_style } else { "" },
            last_was_newline: false,
            live_status: false,
            status_visible: false,
            waiting_for_text: false,
            inline_images: false,
            closed: false,
        };
        block.write_text(prefix)?;
        block.flush_pending_grapheme()?;
        block.writer.flush()?;
        Ok(block)
    }

    pub fn write_text(&mut self, text: &str) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::other(
                "cannot write to a finished terminal block",
            ));
        }
        if text.is_empty() {
            return Ok(());
        }
        self.hide_live_status()?;
        self.waiting_for_text = false;
        self.write_content(text)?;
        self.draw_live_status()?;
        self.writer.flush()
    }

    /// Emit metadata on stderr without overwriting the pending response/loader
    /// when both streams share a terminal. Pipes keep the two streams separate.
    pub fn write_status<E: Write>(
        &mut self,
        ui: TerminalUi,
        writer: &mut E,
        text: &str,
    ) -> io::Result<()> {
        let restore_prefix = self.live_status && self.waiting_for_text && !self.closed;
        if restore_prefix {
            self.hide_live_status()?;
            write!(self.writer, "\r\x1b[2K{RESET}")?;
            self.writer.flush()?;
        }
        ui.write_block(writer, BlockStyle::System, text)?;
        if restore_prefix {
            self.column = 0;
            self.last_was_newline = false;
            write!(self.writer, "{}", self.ansi_style)?;
            self.write_content("assistant> ")?;
            self.flush_pending_grapheme()?;
            self.draw_live_status()?;
            self.writer.flush()?;
        }
        Ok(())
    }

    fn write_content(&mut self, text: &str) -> io::Result<()> {
        if !self.styled && !self.live_status {
            for character in text.chars() {
                match character {
                    '\n' => {
                        writeln!(self.writer)?;
                        self.last_was_newline = true;
                    }
                    '\r' => {}
                    '\t' => {
                        write!(self.writer, "\t")?;
                        self.last_was_newline = false;
                    }
                    character if character.is_control() => {}
                    character => {
                        write!(self.writer, "{character}")?;
                        self.last_was_newline = false;
                    }
                }
            }
            return Ok(());
        }

        for character in text.chars() {
            match character {
                '\n' => {
                    self.flush_pending_grapheme()?;
                    self.finish_line(true)?;
                    self.last_was_newline = true;
                }
                '\r' => {}
                '\t' => {
                    self.flush_pending_grapheme()?;
                    let spaces = 4 - self.column % 4;
                    for _ in 0..spaces {
                        self.write_grapheme(" ")?;
                    }
                }
                character if character.is_control() => {}
                character => self.queue_character(character)?,
            }
        }
        Ok(())
    }

    fn hide_live_status(&mut self) -> io::Result<()> {
        if self.status_visible {
            write!(
                self.writer,
                "\x1b[1B\r\x1b[2K\x1b[1A\x1b[{}G{}",
                self.column.min(self.width.saturating_sub(1)) + 1,
                self.ansi_style
            )?;
            self.status_visible = false;
        }
        Ok(())
    }

    fn draw_live_status(&mut self) -> io::Result<()> {
        if self.live_status && self.waiting_for_text {
            // Leave room for the four-cell image, a gap, the label and the
            // terminal's last column (which could trigger automatic wrapping).
            let show_image = self.inline_images && self.width >= 12;
            let text = fit_line("Думаю…", self.width.saturating_sub(1));
            let dim = if self.styled { "\x1b[2m" } else { "" };
            // Relative positioning also works when the newline scrolls the screen.
            write!(self.writer, "{RESET}\r\n\x1b[2K{dim}{text}{RESET}")?;
            if show_image {
                // iTerm2 animates the GIF itself. Restore the cursor explicitly
                // so returning to the response row is independent of image cursor behavior.
                write!(
                    self.writer,
                    " \x1b7\x1b]1337;File=inline=1;width=4;height=1;preserveAspectRatio=1:{}\x07\x1b8",
                    STANDARD.encode(HORSE_GIF),
                )?;
            }
            write!(
                self.writer,
                "\x1b[1A\x1b[{}G{}",
                self.column.min(self.width.saturating_sub(1)) + 1,
                self.ansi_style
            )?;
            self.status_visible = true;
            self.writer.flush()?;
        }
        Ok(())
    }

    fn queue_character(&mut self, character: char) -> io::Result<()> {
        self.pending_grapheme.push(character);
        let last_grapheme_start = self
            .pending_grapheme
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(index, _)| index);

        if last_grapheme_start > 0 {
            let completed = self.pending_grapheme[..last_grapheme_start].to_owned();
            let pending = self.pending_grapheme[last_grapheme_start..].to_owned();
            self.pending_grapheme = pending;
            for grapheme in completed.graphemes(true) {
                self.write_grapheme(grapheme)?;
            }
        }
        Ok(())
    }

    fn flush_pending_grapheme(&mut self) -> io::Result<()> {
        let pending = std::mem::take(&mut self.pending_grapheme);
        for grapheme in pending.graphemes(true) {
            self.write_grapheme(grapheme)?;
        }
        Ok(())
    }

    fn write_grapheme(&mut self, grapheme: &str) -> io::Result<()> {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if grapheme_width > 0 && self.column > 0 && self.column + grapheme_width > self.width {
            self.finish_line(true)?;
        }

        write!(self.writer, "{grapheme}")?;
        self.column = (self.column + grapheme_width).min(self.width);
        self.last_was_newline = false;
        Ok(())
    }

    fn finish_line(&mut self, start_next: bool) -> io::Result<()> {
        if self.styled {
            for _ in self.column..self.width {
                write!(self.writer, " ")?;
            }
            write!(self.writer, "{RESET}\r\n")?;
            if start_next {
                write!(self.writer, "{}", self.ansi_style)?;
            }
        } else {
            writeln!(self.writer)?;
        }
        self.column = 0;
        Ok(())
    }

    pub fn finish_current(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.hide_live_status()?;
        self.live_status = false;
        self.flush_pending_grapheme()?;
        if self.last_was_newline {
            if self.styled {
                write!(self.writer, "{RESET}")?;
            }
        } else {
            self.finish_line(false)?;
        }
        self.writer.flush()?;
        self.closed = true;
        Ok(())
    }

    pub fn start_next_response(&mut self, status: &str, live_status: bool) -> io::Result<()> {
        self.finish_current()?;
        let status = fit_line(status, self.width.saturating_sub(1));
        if self.styled {
            write!(self.writer, "\x1b[2m{status}{RESET}\r\n{ASSISTANT_STYLE}")?;
        } else {
            writeln!(self.writer, "{status}")?;
        }
        self.column = 0;
        self.last_was_newline = false;
        self.live_status = live_status;
        self.status_visible = false;
        self.waiting_for_text = true;
        self.closed = false;
        self.write_content("assistant> ")?;
        self.flush_pending_grapheme()?;
        self.draw_live_status()?;
        self.writer.flush()
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.finish_current()
    }
}

fn current_width() -> usize {
    terminal_size()
        .map(|(Width(width), _)| usize::from(width).max(1))
        .unwrap_or(80)
}

fn fit_line(text: &str, width: usize) -> String {
    if UnicodeWidthStr::width(text) <= width {
        return text.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    let mut result = String::new();
    let mut columns = 0;
    for grapheme in text.graphemes(true) {
        let size = UnicodeWidthStr::width(grapheme);
        if columns + size > width - 1 {
            break;
        }
        result.push_str(grapheme);
        columns += size;
    }
    result.push('…');
    result
}

fn current_height() -> usize {
    terminal_size()
        .map(|(_, Height(height))| usize::from(height).max(1))
        .unwrap_or(24)
}

fn input_rows(input: &str, width: usize) -> usize {
    let mut rows = 1;
    let mut column = 0;
    for grapheme in format!("you> {input}").graphemes(true) {
        if grapheme == "\t" {
            column = ((column / 8 + 1) * 8).min(width.saturating_sub(1));
        } else if !grapheme.chars().any(char::is_control) {
            let size = UnicodeWidthStr::width(grapheme);
            if column + size > width {
                rows += 1;
                column = 0;
            }
            column += size;
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::STANDARD};

    use crate::client::TokenUsage;
    use crate::config::ContextStrategy;
    use crate::context::{ContextStats, UsageTotals};
    use crate::memory::{DurableMemoryScope, MemorySnapshot, RequestScope};
    use crate::profile::UserProfile;

    use super::{FullWidthBlock, TerminalUi, fit_line, input_rows};

    #[test]
    fn memory_report_separates_layers_and_filters_without_conversation_content() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: false,
        };
        let scope = RequestScope::new("alice", "bot")
            .unwrap()
            .with_dialog_id(Some(42));
        let snapshot = MemorySnapshot::new(
            scope.clone(),
            [("language".into(), "Russian".into())].into(),
            [
                ("stack".into(), "Rust".into()),
                ("database".into(), "SQLite".into()),
            ]
            .into(),
        );
        let stats = ContextStats {
            strategy: ContextStrategy::StickyFacts,
            full_message_count: 8,
            covered_message_count: 2,
            facts_count: 3,
            ..ContextStats::default()
        };
        for (filter, expected) in [
            (
                None,
                "Conversation · dialog: #42 · strategy: sticky_facts · messages: 8 · summary boundary: 2 · sticky facts: 3\nWorking · database = SQLite\nWorking · stack = Rust\nLong-term · language = Russian\n",
            ),
            (
                Some(DurableMemoryScope::User),
                "Long-term · language = Russian\n",
            ),
            (
                Some(DurableMemoryScope::Task),
                "Working · database = SQLite\nWorking · stack = Rust\n",
            ),
        ] {
            let mut output = Vec::new();
            ui.write_memory(&mut output, &scope, stats, &snapshot, filter)
                .unwrap();
            assert_eq!(String::from_utf8(output).unwrap(), expected);
        }
        let mut output = Vec::new();
        let scope = RequestScope::default();
        let empty = MemorySnapshot::new(scope.clone(), Default::default(), Default::default());
        ui.write_memory(&mut output, &scope, ContextStats::default(), &empty, None)
            .unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "Conversation · dialog: new · strategy: summary · messages: 0 · summary boundary: 0 · sticky facts: 0\nWorking · empty\nLong-term · empty\n"
        );
    }

    #[test]
    fn profile_report_prints_complete_markdown_or_explicit_empty_state() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: false,
        };
        let profile = UserProfile::restored(
            "alice",
            "# Preferences\n\n- Be concise.\n- Prefer Android.",
            "now",
        )
        .unwrap();
        let mut output = Vec::new();

        ui.write_profile(&mut output, "alice", Some(&profile))
            .unwrap();
        ui.write_profile(&mut output, "bob", None).unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "Profile · user: alice\n# Preferences\n\n- Be concise.\n- Prefer Android.\nProfile · user: bob · empty\n"
        );
    }

    fn totals(prompt: u64, completion: u64, total: u64, missing: bool) -> UsageTotals {
        let mut result = UsageTotals::default();
        result.record(Some(TokenUsage {
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: total,
            completion_tokens_details: None,
        }));
        if missing {
            result.record(None);
        }
        result
    }

    #[test]
    fn waiting_indicator_lasts_until_first_nonempty_text() {
        let ui = TerminalUi {
            styled: false,
            interactive: true,
            inline_images: false,
        };
        let mut output = Vec::new();
        let mut block = ui.start_response(&mut output).unwrap();
        assert!(String::from_utf8_lossy(block.writer).contains("Думаю…"));
        assert!(!String::from_utf8_lossy(block.writer).contains("Токены"));
        let before_empty = block.writer.len();
        block.write_text("").unwrap();
        assert_eq!(
            block.writer.len(),
            before_empty,
            "empty deltas must leave the loader alone"
        );

        let before_answer = block.writer.len();
        block.write_text("Ответ").unwrap();
        let answer_output = String::from_utf8_lossy(&block.writer[before_answer..]);
        assert!(
            answer_output.contains("\x1b[2K"),
            "clear the indicator before text"
        );
        assert!(!answer_output.contains("Думаю…"));
        assert!(!answer_output.contains("Токены"));
        block.finish().unwrap();
    }

    #[test]
    fn iterm_loader_embeds_the_gif_at_one_row_and_is_removed_on_completion() {
        let ui = TerminalUi {
            styled: false,
            interactive: true,
            inline_images: true,
        };
        let mut output = Vec::new();
        let block = ui.start_response(&mut output).unwrap();
        let initial = String::from_utf8_lossy(block.writer);
        let command = initial
            .split("\x1b]1337;File=")
            .nth(1)
            .expect("inline image");
        let (options, payload) = command.split_once(':').unwrap();
        assert!(options.split(';').any(|option| option == "height=1"));
        assert!(options.split(';').any(|option| option == "inline=1"));
        assert_eq!(
            STANDARD
                .decode(payload.split('\x07').next().unwrap())
                .unwrap(),
            include_bytes!("../assets/horse-loader-dark.gif")
        );
        assert!(initial.contains("Думаю…"));
        assert!(initial.find("Думаю…").unwrap() < initial.find("\x1b]1337;File=").unwrap());
        assert!(!initial.contains("Токены"));
        let end_of_waiting = block.writer.len();
        // Covers API errors and completed streams with no text: both call finish.
        block.finish().unwrap();
        let ending = String::from_utf8_lossy(&output[end_of_waiting..]);
        assert!(ending.contains("\x1b[2K"));
        assert!(!ending.contains("\x1b]1337"));
    }

    #[test]
    fn iterm_loader_is_sent_once_and_not_redrawn_with_streamed_text() {
        let ui = TerminalUi {
            styled: true,
            interactive: true,
            inline_images: true,
        };
        let mut output = Vec::new();
        let mut block = ui.start_response(&mut output).unwrap();
        block.write_text("").unwrap();
        block.write_text("Первая часть\n").unwrap();
        // A newline flushes the grapheme buffer before footer control sequences.
        block.write_text("Вторая часть\n").unwrap();
        block.finish().unwrap();
        let text = String::from_utf8_lossy(&output);
        assert_eq!(text.matches("\x1b]1337;File=").count(), 1);
        assert_eq!(text.matches("Думаю…").count(), 1);
        assert!(text.contains("Первая часть"));
        assert!(text.contains("Вторая часть"));
        assert!(
            !text.contains("Токены"),
            "usage must only be printed after the response finishes"
        );
    }

    #[test]
    fn narrow_terminal_uses_text_without_image_or_wrapping() {
        let mut output = Vec::new();
        let mut block = FullWidthBlock::new(&mut output, false, 8, "", "").unwrap();
        block.live_status = true;
        block.waiting_for_text = true;
        block.inline_images = true;
        block.draw_live_status().unwrap();
        let text = String::from_utf8_lossy(block.writer);
        assert!(!text.contains("\x1b]1337"));
        assert!(text.contains("Думаю…"));
        assert!(!text.contains("Токены"));
    }

    #[test]
    fn redirected_output_has_no_loader_or_control_sequences_even_for_iterm() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: true,
        };
        let mut output = Vec::new();
        let mut block = ui.start_response(&mut output).unwrap();
        block.write_text("Ответ").unwrap();
        block.finish().unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "assistant> Ответ\n");
    }

    #[test]
    fn tool_status_keeps_its_line_and_restores_the_pending_answer_cursor() {
        use std::cell::RefCell;
        use std::io::{self, Write};
        use std::rc::Rc;

        #[derive(Clone, Default)]
        struct SharedTerminal(Rc<RefCell<Vec<u8>>>);
        impl Write for SharedTerminal {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.borrow_mut().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let ui = TerminalUi {
            styled: false,
            interactive: true,
            inline_images: false,
        };
        let mut stdout = SharedTerminal::default();
        let mut stderr = stdout.clone();
        let output = stdout.0.clone();
        let mut block = ui.start_response(&mut stdout).unwrap();
        let start = output.borrow().len();
        block
            .write_status(ui, &mut stderr, "Tool started · call=call_1")
            .unwrap();
        let after_status = output.borrow().len();
        let status = String::from_utf8(output.borrow()[start..after_status].to_vec()).unwrap();
        assert!(
            status.contains("\r\x1b[2K\x1b[0mTool started · call=call_1\nassistant> "),
            "{status:?}"
        );
        assert!(status.ends_with("\x1b[12G"), "{status:?}");

        block.write_text("Final answer\n").unwrap();
        block.finish().unwrap();
        let answer = String::from_utf8(output.borrow()[after_status..].to_vec()).unwrap();
        assert!(answer.contains("\x1b[12GFinal answer\n"), "{answer:?}");
        assert!(!answer.contains("Думаю"));
        assert!(!answer.contains("Tool started"));
    }

    #[test]
    fn no_color_loader_restores_cursor_after_the_assistant_prefix() {
        let ui = TerminalUi {
            styled: false,
            interactive: true,
            inline_images: true,
        };
        let mut output = Vec::new();
        let mut block = ui.start_response(&mut output).unwrap();
        // "assistant> " occupies 11 columns; ANSI column numbers start at 1.
        assert!(String::from_utf8_lossy(block.writer).ends_with("\x1b[12G"));
        let start = block.writer.len();
        block.write_text("Answer\n").unwrap();
        let response = String::from_utf8_lossy(&block.writer[start..]);
        assert!(
            response.contains("\x1b[12GAnswer"),
            "answer must start after assistant> , not in column 1"
        );
        block.finish().unwrap();
    }

    #[test]
    fn input_rows_follow_terminal_tab_stops_and_wrapping() {
        assert_eq!(input_rows("\t123456789012", 20), 1);
        assert_eq!(input_rows("\t1234567890123", 20), 2);
        assert_eq!(input_rows(&"X".repeat(650), 40), 17);
        assert_eq!(input_rows("界界界", 10), 2);
    }

    #[test]
    fn status_stays_on_one_row_without_splitting_unicode() {
        assert_eq!(fit_line("abc界def", 6), "abc界…");
        assert_eq!(fit_line("e\u{301}xyz", 2), "e\u{301}…");
        assert_eq!(fit_line("abc", 0), "");
    }

    #[test]
    fn context_stats_show_raw_boundary_and_separate_api_costs() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: false,
        };
        let stats = ContextStats {
            strategy: ContextStrategy::Summary,
            full_message_count: 24,
            covered_message_count: 14,
            raw_message_count: 10,
            selected_message_count: 10,
            ordinary_usage: totals(12000, 2000, 14000, false),
            compaction_usage: totals(5000, 600, 5600, false),
            ..ContextStats::default()
        };
        let mut output = Vec::new();

        ui.write_context_stats(&mut output, stats).unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "Стратегия · summary\n\
             Контекст · полная история: 24 · покрыто summary: 14 · дословно: 10\n\
             Ответы · вход: 12000 · выход: 2000 · всего: 14000\n\
             Summary · вход: 5000 · выход: 600 · всего: 5600\n\
             Facts · вход: 0 · выход: 0 · всего: 0\n\
             API всего · 19600\n"
        );
    }

    #[test]
    fn context_stats_mark_calls_without_provider_usage() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: false,
        };
        let stats = ContextStats {
            ordinary_usage: totals(1, 2, 3, true),
            compaction_usage: totals(4, 5, 9, true),
            ..ContextStats::default()
        };
        let mut output = Vec::new();

        ui.write_context_stats(&mut output, stats).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("Ответы · вход: 1 · выход: 2 · всего: 3 · без данных API: 1"));
        assert!(output.contains("Summary · вход: 4 · выход: 5 · всего: 9 · без данных API: 1"));
    }

    #[test]
    fn sticky_facts_stats_show_selected_context_and_separate_cost() {
        let ui = TerminalUi {
            styled: false,
            interactive: false,
            inline_images: false,
        };
        let stats = ContextStats {
            strategy: ContextStrategy::StickyFacts,
            full_message_count: 8,
            selected_message_count: 3,
            facts_count: 2,
            facts_covered_message_count: 5,
            facts_usage: totals(10, 2, 12, false),
            ..ContextStats::default()
        };
        let mut output = Vec::new();

        ui.write_context_stats(&mut output, stats).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("Стратегия · sticky_facts"));
        assert!(
            output.contains("Контекст · полная история: 8 · в запросе: 3 · facts: 2 · facts до: 5")
        );
        assert!(output.contains("Facts · вход: 10 · выход: 2 · всего: 12"));
        assert!(output.contains("API всего · 12"));
    }
}
