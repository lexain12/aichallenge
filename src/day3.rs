use std::io::{self, IsTerminal};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{self, Event as TerminalEvent, KeyCode, KeyEventKind, KeyModifiers};
use deepseek_cli::client::DeepSeekClient;
use deepseek_cli::config::Config;
use deepseek_cli::reasoning::{
    self, Event, Method, Phase, SAMPLE_REFERENCE, SAMPLE_TASK, TokenAccounting,
};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

const VERDICTS: [&str; 4] = ["не оценено", "верно", "частично", "неверно"];

struct Answer {
    method: Method,
    prompt: String,
    text: String,
    status: String,
    finished: bool,
    seconds: f64,
    tokens: TokenAccounting,
    verdict: usize,
    scroll: u16,
}

impl Answer {
    fn new(method: Method) -> Self {
        Self {
            method,
            prompt: String::new(),
            text: String::new(),
            status: "Ожидание".into(),
            finished: false,
            seconds: 0.0,
            tokens: TokenAccounting::default(),
            verdict: 0,
            scroll: 0,
        }
    }
}

enum Update {
    Stream(usize, Event),
    Finished(usize, Result<String, String>, f64),
}

struct App {
    client: Arc<DeepSeekClient>,
    task: String,
    input: String,
    settings: String,
    answers: [Answer; 4],
    receiver: mpsc::Receiver<Update>,
    sender: mpsc::Sender<Update>,
    jobs: Vec<tokio::task::JoinHandle<()>>,
    editing: bool,
    selected: usize,
    show_prompt: bool,
    notice: String,
    started: Option<Instant>,
    auto_saved: bool,
}

impl App {
    fn new(config: &Config) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel();
        Ok(Self {
            client: Arc::new(
                DeepSeekClient::new(config)
                    .map_err(|e| e.to_string())?
                    .without_thinking(),
            ),
            task: String::new(),
            input: String::new(),
            settings: format!(
                "{} · t={} · max_tokens={} · thinking=disabled",
                config.model(),
                config.temperature(),
                config.max_tokens()
            ),
            answers: Method::ALL.map(Answer::new),
            receiver,
            sender,
            jobs: Vec::new(),
            editing: true,
            selected: 0,
            show_prompt: false,
            notice: String::new(),
            started: None,
            auto_saved: false,
        })
    }

    fn running(&self) -> bool {
        self.started.is_some() && self.answers.iter().any(|answer| !answer.finished)
    }

    fn start(&mut self) {
        self.task = self.input.trim().to_owned();
        self.answers = Method::ALL.map(Answer::new);
        self.editing = false;
        self.show_prompt = false;
        self.notice.clear();
        self.started = Some(Instant::now());
        self.auto_saved = false;
        self.jobs.clear();
        for (index, method) in Method::ALL.into_iter().enumerate() {
            let sender = self.sender.clone();
            let client = self.client.clone();
            let task = self.task.clone();
            self.jobs.push(tokio::spawn(async move {
                let started = Instant::now();
                let result = reasoning::solve(&client, &task, method, |event| {
                    let _ = sender.send(Update::Stream(index, event));
                })
                .await
                .map_err(|e| e.to_string());
                let _ = sender.send(Update::Finished(
                    index,
                    result,
                    started.elapsed().as_secs_f64(),
                ));
            }));
        }
    }

    fn drain(&mut self) -> bool {
        let mut changed = false;
        while let Ok(update) = self.receiver.try_recv() {
            changed = true;
            match update {
                Update::Stream(index, event) => {
                    let answer = &mut self.answers[index];
                    match event {
                        Event::Usage(phase, usage) => answer.tokens.record(phase, usage),
                        Event::Started(Phase::Prompt) => answer.status = "Создаёт промпт…".into(),
                        Event::Started(Phase::Answer) => answer.status = "Решает…".into(),
                        Event::Text(Phase::Prompt, text) => answer.prompt.push_str(&text),
                        Event::Text(Phase::Answer, text) => answer.text.push_str(&text),
                    }
                }
                Update::Finished(index, result, seconds) => {
                    let answer = &mut self.answers[index];
                    answer.status = match result {
                        Ok(_) => "Ответ получен".into(),
                        Err(e) => format!("Ошибка: {e}"),
                    };
                    answer.finished = true;
                    answer.seconds = seconds;
                }
            }
        }
        if self.started.is_some() && !self.running() && !self.auto_saved {
            self.save();
            self.auto_saved = true;
            changed = true;
        }
        changed
    }

    fn save(&mut self) {
        self.notice = match save_report(self) {
            Ok(path) => format!("Сохранено: {path}"),
            Err(e) => format!("Ошибка сохранения: {e}"),
        };
    }

    fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> bool {
        if code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL) {
            return true;
        }
        if self.editing {
            match code {
                KeyCode::Esc => return true,
                KeyCode::Enter if !self.input.trim().is_empty() => self.start(),
                KeyCode::F(2) => self.input = SAMPLE_TASK.into(),
                KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => {
                    self.input.clear()
                }
                KeyCode::Char(c)
                    if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    self.input.push(c)
                }
                KeyCode::Backspace => {
                    self.input.pop();
                }
                _ => {}
            }
        } else {
            match code {
                KeyCode::Char('q') | KeyCode::Esc => return true,
                KeyCode::Tab => self.selected = (self.selected + 1) % 4,
                KeyCode::BackTab => self.selected = (self.selected + 3) % 4,
                KeyCode::Char(c @ '1'..='4') => self.selected = c as usize - '1' as usize,
                KeyCode::Up => {
                    self.answers[self.selected].scroll =
                        self.answers[self.selected].scroll.saturating_sub(1)
                }
                KeyCode::Down => {
                    self.answers[self.selected].scroll =
                        self.answers[self.selected].scroll.saturating_add(1)
                }
                KeyCode::PageUp => {
                    self.answers[self.selected].scroll =
                        self.answers[self.selected].scroll.saturating_sub(10)
                }
                KeyCode::PageDown => {
                    self.answers[self.selected].scroll =
                        self.answers[self.selected].scroll.saturating_add(10)
                }
                KeyCode::Home => self.answers[self.selected].scroll = 0,
                KeyCode::End => self.answers[self.selected].scroll = u16::MAX,
                KeyCode::Char('p') => {
                    self.show_prompt = !self.show_prompt;
                    self.answers[2].scroll = 0;
                    self.selected = 2;
                }
                KeyCode::Char('v') if self.answers[self.selected].finished => {
                    self.answers[self.selected].verdict =
                        (self.answers[self.selected].verdict + 1) % VERDICTS.len();
                }
                KeyCode::Char('s') => self.save(),
                KeyCode::Char('n') if !self.running() => self.editing = true,
                _ => {}
            }
        }
        false
    }

    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();
        if area.width < 90 || area.height < 28 {
            frame.render_widget(
                Paragraph::new(
                    "Увеличь терминал хотя бы до 90×28 для четырёх панелей. Ctrl+C — выход.",
                )
                .wrap(Wrap { trim: false }),
                area,
            );
            return;
        }
        let sections = Layout::vertical([
            Constraint::Length(2),
            Constraint::Length(5),
            Constraint::Min(12),
            Constraint::Length(4),
        ])
        .split(area);
        frame.render_widget(
            Paragraph::new(format!(
                "ДЕНЬ 3 · Четыре способа решения\n{}",
                self.settings
            ))
            .style(Style::default().fg(Color::Cyan)),
            sections[0],
        );
        let input_title = if self.editing {
            " Задача · Enter: запуск · F2: пример · Ctrl+U: очистить "
        } else {
            " Одна и та же задача во всех запросах "
        };
        let text = if self.editing {
            self.input.as_str()
        } else {
            self.task.as_str()
        };
        let text = if text.is_empty() {
            "Введи задачу здесь или нажми F2 для примера с проверенным ответом…"
        } else {
            text
        };
        frame.render_widget(
            Paragraph::new(text).wrap(Wrap { trim: false }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(input_title)
                    .border_style(Style::default().fg(if self.editing {
                        Color::Yellow
                    } else {
                        Color::DarkGray
                    })),
            ),
            sections[1],
        );
        let rows = Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(sections[2]);
        for (row_index, row) in rows.iter().enumerate() {
            let columns =
                Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(*row);
            for (column, area) in columns.iter().enumerate() {
                self.draw_answer(frame, row_index * 2 + column, *area);
            }
        }
        let reference = if !self.running() && self.task == SAMPLE_TASK {
            SAMPLE_REFERENCE
        } else {
            "Оцени точность по условиям задачи; длина ответа не определяет качество. Эксперты — роли одной модели."
        };
        frame.render_widget(Paragraph::new(format!("Tab/1–4: панель · ↑↓/PgUp/PgDn: прокрутка · Home/End · p: промпт №3\nv: оценка · s: сохранить · n: новая задача · q/Esc: выход\n{reference}\n{}", self.notice)), sections[3]);
    }

    fn draw_answer(&mut self, frame: &mut Frame, index: usize, area: Rect) {
        let elapsed = self
            .started
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let answer = &mut self.answers[index];
        let seconds = if answer.finished {
            answer.seconds
        } else {
            elapsed
        };
        let title = format!(
            " {} · {} слов · {:.0}с ",
            answer.method.title(),
            answer.text.split_whitespace().count(),
            seconds
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(
                Style::default().fg(if !self.editing && self.selected == index {
                    Color::Cyan
                } else {
                    Color::DarkGray
                }),
            );
        let inner = block.inner(area);
        let text = if index == 2 && self.show_prompt {
            &answer.prompt
        } else {
            &answer.text
        };
        let content = format!(
            "{} · Оценка: {}{}\n{}\n\n{}",
            answer.status,
            VERDICTS[answer.verdict],
            if index == 2 && self.show_prompt {
                " · ПРОМПТ"
            } else {
                ""
            },
            answer.tokens.summary(answer.method),
            text
        );
        let paragraph = Paragraph::new(content).wrap(Wrap { trim: false });
        let max_scroll = paragraph
            .line_count(inner.width)
            .saturating_sub(inner.height as usize)
            .min(u16::MAX as usize) as u16;
        answer.scroll = answer.scroll.min(max_scroll);
        frame.render_widget(paragraph.scroll((answer.scroll, 0)).block(block), area);
    }
}

impl Drop for App {
    fn drop(&mut self) {
        for job in &self.jobs {
            job.abort();
        }
    }
}

pub fn run(config: Config) -> Result<(), String> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("Режим --day3 требует интерактивный терминал".into());
    }
    let mut app = App::new(&config)?;
    let mut terminal = ratatui::try_init().map_err(|e| e.to_string())?;
    let result = (|| -> io::Result<()> {
        crossterm::execute!(io::stdout(), event::EnableBracketedPaste)?;
        let mut redraw = true;
        let mut last_draw = Instant::now();
        loop {
            let changed = app.drain();
            if redraw || changed || (app.running() && last_draw.elapsed() >= Duration::from_secs(1))
            {
                terminal.draw(|frame| app.draw(frame))?;
                last_draw = Instant::now();
                redraw = false;
            }
            if event::poll(Duration::from_millis(50))? {
                redraw = true;
                match event::read()? {
                    TerminalEvent::Key(key)
                        if key.kind != KeyEventKind::Release
                            && app.key(key.code, key.modifiers) =>
                    {
                        break;
                    }
                    TerminalEvent::Paste(text) if app.editing => {
                        app.input.push_str(&text.replace(['\n', '\r'], " "))
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    })();
    let _ = crossterm::execute!(io::stdout(), event::DisableBracketedPaste);
    ratatui::restore();
    result.map_err(|e| e.to_string())
}

fn save_report(app: &App) -> io::Result<String> {
    let reference = if app.task == SAMPLE_TASK {
        SAMPLE_REFERENCE
    } else {
        "Эталон не задан. Оценки ниже выставляет пользователь."
    };
    let mut report = format!(
        "# День 3. Разные способы рассуждения\n\n{}\n\n## Задача\n\n{}\n\n## Эталон\n\n{reference}\n\n",
        app.settings, app.task
    );
    for answer in &app.answers {
        report.push_str(&format!(
            "## {}\n\nСтатус: {}. Время: {:.1} с. Слов: {}. Оценка пользователя: {}.\n\n",
            answer.method.title(),
            answer.status,
            answer.seconds,
            answer.text.split_whitespace().count(),
            VERDICTS[answer.verdict]
        ));
        report.push_str(&format!("{}\n\n", answer.tokens.summary(answer.method)));
        if !answer.prompt.is_empty() {
            report.push_str(&format!(
                "### Сгенерированный промпт\n\n{}\n\n### Решение\n\n",
                answer.prompt
            ));
        }
        report.push_str(&answer.text);
        report.push_str("\n\n");
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::fs::create_dir_all("reports")?;
    let path = format!("reports/day3-{timestamp}.md");
    std::fs::write(&path, report)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    use std::io::Write;

    #[test]
    fn renders_four_panels_and_switches_focus_and_prompt() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "api_key = \"test\"").unwrap();
        let config = Config::load(file.path(), None).unwrap();
        let mut app = App::new(&config).unwrap();
        app.editing = false;
        app.answers[2].prompt = "GENERATED PROMPT".into();
        app.key(KeyCode::Char('p'), KeyModifiers::NONE);
        assert_eq!(app.selected, 2);
        let mut terminal = Terminal::new(TestBackend::new(140, 42)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let buffer = terminal.backend().buffer();
        let rendered: String = buffer.content.iter().map(|cell| cell.symbol()).collect();
        for label in [
            "Прямой ответ",
            "Решай пошагово",
            "Сначала создай промпт",
            "Группа экспертов",
            "GENERATED PROMPT",
        ] {
            assert!(rendered.contains(label), "missing {label}");
        }
        app.key(KeyCode::Tab, KeyModifiers::NONE);
        assert_eq!(app.selected, 3);
        app.key(KeyCode::Char('1'), KeyModifiers::NONE);
        assert_eq!(app.selected, 0);
    }
}
