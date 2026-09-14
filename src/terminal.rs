use std::io::{self, IsTerminal, Write};

use crate::client::TokenUsage;
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
        };
        block.write_text(prefix)?;
        block.flush_pending_grapheme()?;
        block.writer.flush()?;
        Ok(block)
    }

    pub fn write_text(&mut self, text: &str) -> io::Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        self.hide_live_status()?;
        self.waiting_for_text = false;
        self.write_content(text)?;
        self.draw_live_status()?;
        self.writer.flush()
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

    pub fn finish(mut self) -> io::Result<()> {
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
        self.writer.flush()
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

    use super::{FullWidthBlock, TerminalUi, fit_line, input_rows};

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
}
