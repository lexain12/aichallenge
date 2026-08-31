use std::io::{self, IsTerminal, Write};

use terminal_size::{Width, terminal_size};
use unicode_width::UnicodeWidthStr;

const RESET: &str = "\x1b[0m";
const USER_STYLE: &str = "\x1b[48;2;20;45;80m\x1b[38;2;235;245;255m";
const ASSISTANT_STYLE: &str = "\x1b[48;2;15;65;45m\x1b[38;2;235;255;245m";
const SYSTEM_STYLE: &str = "\x1b[48;2;55;55;55m\x1b[38;2;245;245;245m";
const ERROR_STYLE: &str = "\x1b[48;2;90;25;25m\x1b[38;2;255;235;235m";

#[derive(Clone, Copy)]
pub enum BlockStyle {
    Assistant,
    System,
    Error,
}

impl BlockStyle {
    fn ansi(self) -> &'static str {
        match self {
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
    line_text: String,
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
            line_text: String::new(),
            ansi_style,
            last_was_newline: false,
        };
        block.write_text(prefix)?;
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
                    self.finish_line(true)?;
                    self.last_was_newline = true;
                }
                '\r' => {}
                '\t' => {
                    let spaces = 4 - self.column % 4;
                    for _ in 0..spaces {
                        self.write_character(' ')?;
                    }
                }
                character if character.is_control() => {}
                character => self.write_character(character)?,
            }
        }
        self.writer.flush()
    }

    fn write_character(&mut self, character: char) -> io::Result<()> {
        let previous_length = self.line_text.len();
        self.line_text.push(character);
        let mut new_width = UnicodeWidthStr::width(self.line_text.as_str());

        if new_width > self.width && previous_length > 0 {
            self.line_text.truncate(previous_length);
            self.finish_line(true)?;
            self.line_text.push(character);
            new_width = UnicodeWidthStr::width(self.line_text.as_str());
        }

        write!(self.writer, "{character}")?;
        self.column = new_width.min(self.width);
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
        self.line_text.clear();
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
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
