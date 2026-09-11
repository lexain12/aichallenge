use std::io::{self, IsTerminal, Write};

use terminal_size::{Width, terminal_size};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const RESET: &str = "\x1b[0m";
const USER_STYLE: &str = "\x1b[48;2;20;45;80m\x1b[38;2;235;245;255m";
const ASSISTANT_STYLE: &str = "\x1b[48;2;15;65;45m\x1b[38;2;235;255;245m";
const SYSTEM_STYLE: &str = "\x1b[48;2;55;55;55m\x1b[38;2;245;245;245m";
const ERROR_STYLE: &str = "\x1b[48;2;90;25;25m\x1b[38;2;255;235;235m";

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
}

impl TerminalUi {
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
        let display_columns = visible_width(&format!("you> {input}"));
        let occupied_rows = display_columns.saturating_sub(1) / width + 1;
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
            ansi_style,
            last_was_newline: false,
        };
        block.write_text(prefix)?;
        block.flush_pending_grapheme()?;
        block.writer.flush()?;
        Ok(block)
    }

    pub fn write_text(&mut self, text: &str) -> io::Result<()> {
        if !self.styled {
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
            return self.writer.flush();
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
        self.writer.flush()
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

fn visible_width(text: &str) -> usize {
    let mut printable = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\t' => printable.push_str("    "),
            character if character.is_control() => {}
            character => printable.push(character),
        }
    }
    UnicodeWidthStr::width(printable.as_str())
}
