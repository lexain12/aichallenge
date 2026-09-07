use crate::window_config::WindowConfig;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Reasoning,
    Temperatures,
}

pub const TEMPERATURES: [f64; 4] = [0.0, 0.7, 1.2, 1.0];

impl Mode {
    fn methods(self) -> [Method; 4] {
        match self {
            Self::Reasoning => Method::ALL,
            Self::Temperatures => [Method::Direct; 4],
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Reasoning => "ДЕНЬ 3 · Четыре способа решения",
            Self::Temperatures => "СРАВНЕНИЕ ТЕМПЕРАТУР · Один запрос, четыре ответа",
        }
    }

    fn answers(self) -> [Answer; 4] {
        let mut answers = self.methods().map(Answer::new);
        if self == Self::Temperatures {
            for (index, answer) in answers.iter_mut().enumerate() {
                answer.title = format!(
                    "{}. temperature = {}{}",
                    index + 1,
                    TEMPERATURES[index],
                    if index == 3 { " (база)" } else { "" }
                );
            }
        }
        answers
    }
}

const VERDICTS: [&str; 4] = ["не оценено", "верно", "частично", "неверно"];

struct Answer {
    method: Method,
    title: String,
    settings: String,
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
            title: method.title().into(),
            settings: String::new(),
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

const EDIT_FIELDS: [&str; 11] = [
    "temperature",
    "model",
    "base_url",
    "api_key",
    "system_prompt",
    "max_tokens",
    "timeout_seconds",
    "top_p",
    "stop",
    "thinking",
    "include_usage",
];

struct ConfigEditor {
    panel: usize,
    values: Vec<String>,
    selected: usize,
    error: String,
}

struct App {
    clients: [Arc<DeepSeekClient>; 4],
    window_config: WindowConfig,
    config_path: PathBuf,
    config_editor: Option<ConfigEditor>,
    config_buttons: [Rect; 4],
    save_button: Rect,
    cancel_button: Rect,
    run_settings: String,
    mode: Mode,
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
    fn new(config: &Config, mode: Mode) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel();
        let mut clients = Vec::new();
        for temperature in TEMPERATURES {
            let settings = if mode == Mode::Temperatures {
                config
                    .clone()
                    .with_temperature(temperature)
                    .map_err(|e| e.to_string())?
            } else {
                config.clone()
            };
            clients.push(Arc::new(
                DeepSeekClient::new(&settings)
                    .map_err(|e| e.to_string())?
                    .without_thinking(),
            ));
        }
        Ok(Self {
            clients: clients.try_into().map_err(|_| "expected four clients")?,
            window_config: WindowConfig::from_base(
                config,
                if mode == Mode::Temperatures {
                    TEMPERATURES
                } else {
                    [config.temperature(); 4]
                },
            ),
            config_path: PathBuf::from("panels"),
            config_editor: None,
            config_buttons: [Rect::default(); 4],
            save_button: Rect::default(),
            cancel_button: Rect::default(),
            run_settings: String::new(),
            mode,
            task: String::new(),
            input: String::new(),
            settings: format!(
                "{} · t={} · max_tokens={} · thinking=disabled",
                config.model(),
                if mode == Mode::Temperatures {
                    "0 / 0.7 / 1.2 / 1.0".into()
                } else {
                    config.temperature().to_string()
                },
                config.max_tokens()
            ),
            answers: mode.answers(),
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

    fn apply_windows(&mut self, windows: WindowConfig) -> Result<(), String> {
        let mut clients = Vec::new();
        for window in &windows.windows {
            clients.push(Arc::new(
                DeepSeekClient::new(&window.config()?).map_err(|e| e.to_string())?,
            ));
        }
        self.clients = clients.try_into().map_err(|_| "expected four clients")?;
        self.window_config = windows;
        self.settings =
            "F3: отдельный API-конфиг каждой панели · изменения действуют на следующий запрос"
                .into();
        if self.started.is_none() {
            self.label_answers();
        }
        Ok(())
    }

    fn label_answers(&mut self) {
        for (index, answer) in self.answers.iter_mut().enumerate() {
            let config = &self.window_config.windows[index];
            answer.title = if self.mode == Mode::Temperatures {
                format!(
                    "{}. temperature = {} · {}",
                    index + 1,
                    config.temperature,
                    config.model
                )
            } else {
                format!("{} · {}", answer.method.title(), config.model)
            };
            answer.settings = config.public_summary();
        }
    }

    fn open_editor(&mut self, panel: usize) {
        if self.running() {
            self.notice = "Настройки доступны после завершения запросов".into();
            return;
        }
        self.selected = panel;
        let w = &self.window_config.windows[panel];
        self.config_editor = Some(ConfigEditor {
            panel,
            selected: 0,
            error: String::new(),
            values: vec![
                w.temperature.to_string(),
                w.model.clone(),
                w.base_url.clone(),
                w.api_key.clone(),
                w.system_prompt.clone(),
                w.max_tokens.to_string(),
                w.timeout_seconds.to_string(),
                w.top_p.map(|v| v.to_string()).unwrap_or_default(),
                serde_json::to_string(&w.stop).unwrap(),
                w.thinking.clone().unwrap_or_default(),
                w.include_usage.to_string(),
            ],
        });
    }

    fn save_editor(&mut self) {
        let Some(editor) = &self.config_editor else {
            return;
        };
        let panel = editor.panel;
        let result = (|| -> Result<(), String> {
            let v = &editor.values;
            let parse = |i: usize| {
                v[i].trim()
                    .replace(',', ".")
                    .parse::<f64>()
                    .map_err(|_| format!("{}: введи число", EDIT_FIELDS[i]))
            };
            let settings = crate::window_config::WindowSettings {
                temperature: parse(0)?,
                model: v[1].trim().into(),
                base_url: v[2].trim().into(),
                api_key: v[3].trim().into(),
                system_prompt: v[4].clone(),
                max_tokens: v[5]
                    .trim()
                    .parse()
                    .map_err(|_| "max_tokens: нужно целое число")?,
                timeout_seconds: v[6]
                    .trim()
                    .parse()
                    .map_err(|_| "timeout_seconds: нужно целое число")?,
                top_p: if v[7].trim().is_empty() {
                    None
                } else {
                    Some(parse(7)?)
                },
                stop: serde_json::from_str(&v[8])
                    .map_err(|_| "stop: нужен JSON-массив строк, например [\"Готово\"]")?,
                thinking: if v[9].trim().is_empty() {
                    None
                } else {
                    Some(v[9].trim().into())
                },
                include_usage: v[10]
                    .trim()
                    .parse()
                    .map_err(|_| "include_usage: true или false")?,
            };
            let client =
                Arc::new(DeepSeekClient::new(&settings.config()?).map_err(|e| e.to_string())?);
            settings.save(&WindowConfig::path(&self.config_path, panel))?;
            self.clients[panel] = client;
            self.window_config.windows[panel] = settings;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.config_editor = None;
                if self.started.is_none() {
                    self.label_answers();
                }
                self.notice = format!(
                    "Панель {} сохранена. Настройки применятся к следующему запросу.",
                    panel + 1
                );
            }
            Err(error) => self.config_editor.as_mut().unwrap().error = error,
        }
    }

    fn editor_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        if code == KeyCode::Esc {
            self.config_editor = None;
            return;
        }
        if code == KeyCode::Enter
            || (code == KeyCode::Char('s') && modifiers.contains(KeyModifiers::CONTROL))
        {
            self.save_editor();
            return;
        }
        let editor = self.config_editor.as_mut().unwrap();
        match code {
            KeyCode::Tab | KeyCode::Down => {
                editor.selected = (editor.selected + 1) % EDIT_FIELDS.len()
            }
            KeyCode::BackTab | KeyCode::Up => {
                editor.selected = (editor.selected + EDIT_FIELDS.len() - 1) % EDIT_FIELDS.len()
            }
            KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => {
                editor.values[editor.selected].clear()
            }
            KeyCode::Char('j')
                if modifiers.contains(KeyModifiers::CONTROL) && editor.selected == 4 =>
            {
                editor.values[4].push('\n')
            }
            KeyCode::Backspace => {
                editor.values[editor.selected].pop();
            }
            KeyCode::Char(c)
                if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                editor.values[editor.selected].push(c)
            }
            _ => {}
        }
    }

    fn click(&mut self, x: u16, y: u16) {
        let position = ratatui::layout::Position::new(x, y);
        if self.config_editor.is_some() {
            if self.save_button.contains(position) {
                self.save_editor();
            } else if self.cancel_button.contains(position) {
                self.config_editor = None;
            }
        } else if let Some(index) = self
            .config_buttons
            .iter()
            .position(|rect| rect.contains(position))
        {
            self.open_editor(index);
        }
    }

    fn draw_editor(&mut self, frame: &mut Frame) {
        let Some(editor) = &self.config_editor else {
            return;
        };
        let area = frame.area();
        let width = 100.min(area.width);
        let height = 27.min(area.height);
        let popup = Rect::new(
            (area.width - width) / 2,
            (area.height - height) / 2,
            width,
            height,
        );
        frame.render_widget(ratatui::widgets::Clear, popup);
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Настройки панели {} ", editor.panel + 1))
            .border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(format!(
                "{}\nTab/↑↓: поле · Ctrl+U: очистить · Ctrl+J: новая строка · Enter: сохранить",
                WindowConfig::path(&self.config_path, editor.panel).display()
            )),
            Rect::new(inner.x + 1, inner.y, inner.width - 2, 2),
        );
        for (i, label) in EDIT_FIELDS.iter().enumerate() {
            frame.render_widget(
                Paragraph::new(format!(
                    "{} {}",
                    if i == editor.selected { ">" } else { " " },
                    label
                ))
                .style(Style::default().fg(if i == editor.selected {
                    Color::Yellow
                } else {
                    Color::White
                })),
                Rect::new(inner.x + 1, inner.y + 3 + i as u16, 27, 1),
            );
        }
        let value = if editor.selected == 3 {
            "•".repeat(editor.values[3].chars().count().min(40))
        } else {
            editor.values[editor.selected].clone()
        };
        let field = Rect::new(inner.x + 29, inner.y + 3, inner.width - 30, 14);
        let paragraph = Paragraph::new(format!("{value}▏")).wrap(Wrap { trim: false });
        let scroll = paragraph
            .line_count(field.width.saturating_sub(2))
            .saturating_sub(10)
            .min(u16::MAX as usize) as u16;
        frame.render_widget(
            paragraph.scroll((scroll, 0)).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(EDIT_FIELDS[editor.selected]),
            ),
            field,
        );
        let help = match editor.selected {
            2 => {
                "Адрес API до /chat/completions, например https://host/v1. Нужен совместимый SSE Chat Completions API."
            }
            3 => {
                "Ключ хранится только в файле этой панели. Ctrl+U — заменить. Глобальная переменная окружения его не переопределяет."
            }
            4 => {
                "System prompt может быть многострочным. Вставь текст или используй Ctrl+J. Пустое поле — без system message."
            }
            7 => "top_p: 0–1 или пусто (не отправлять).",
            8 => "stop: JSON-массив строк, например [\"Готово\"]. [] — отключить.",
            9 => {
                "thinking: enabled / disabled / пусто. Для другого провайдера обычно оставь пустым."
            }
            10 => {
                "true — запрашивать токены через stream_options; false — не отправлять этот параметр."
            }
            _ => "Изменения действуют на следующий запрос. Esc — отменить.",
        };
        frame.render_widget(
            Paragraph::new(help).wrap(Wrap { trim: false }),
            Rect::new(inner.x + 1, inner.y + 18, inner.width - 2, 3),
        );
        frame.render_widget(
            Paragraph::new(editor.error.as_str())
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: false }),
            Rect::new(inner.x + 1, inner.y + 21, inner.width - 2, 2),
        );
        self.save_button = Rect::new(inner.x + 2, inner.y + 24, 23, 1);
        self.cancel_button = Rect::new(inner.x + 28, inner.y + 24, 18, 1);
        frame.render_widget(
            Paragraph::new("[ Enter · Сохранить ]").style(Style::default().fg(Color::Green)),
            self.save_button,
        );
        frame.render_widget(Paragraph::new("[ Esc · Отмена ]"), self.cancel_button);
    }

    fn running(&self) -> bool {
        self.started.is_some() && self.answers.iter().any(|answer| !answer.finished)
    }

    fn start(&mut self) {
        self.task = self.input.trim().to_owned();
        self.answers = self.mode.answers();
        self.label_answers();
        self.run_settings = self.settings.clone();
        self.editing = false;
        self.show_prompt = false;
        self.notice.clear();
        self.started = Some(Instant::now());
        self.auto_saved = false;
        self.jobs.clear();
        for (index, method) in self.mode.methods().into_iter().enumerate() {
            let sender = self.sender.clone();
            let client = self.clients[index].clone();
            let task = self.task.clone();
            let system_prompt = self.window_config.windows[index].system_prompt.clone();
            self.jobs.push(tokio::spawn(async move {
                let started = Instant::now();
                let result =
                    reasoning::solve_with_system(&client, &task, method, &system_prompt, |event| {
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
        if self.config_editor.is_some() {
            self.editor_key(code, modifiers);
            return false;
        }
        if code == KeyCode::F(3) {
            self.open_editor(self.selected);
            return false;
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
                KeyCode::Char('p') if self.mode == Mode::Reasoning => {
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
            Paragraph::new(format!("{}\n{}", self.mode.title(), self.settings))
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
            "Сравни точность, структуру и разнообразие ответов. Один прогон не доказывает, какой вариант лучше."
        };
        frame.render_widget(Paragraph::new(format!("Tab/1–4: панель · ↑↓/PgUp/PgDn: прокрутка · Home/End{}\nv: оценка · s: сохранить · n: новая задача · q/Esc: выход\n{reference}\n{}",
            if self.mode == Mode::Reasoning { " · p: промпт №3" } else { " · F3: настройки" }, self.notice)), sections[3]);
        self.draw_editor(frame);
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
            " {} · {} слов · {:.2}с ",
            answer.title,
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
        let block = {
            let button_width = 16_u16.min(area.width.saturating_sub(2));
            self.config_buttons[index] = Rect::new(
                area.right().saturating_sub(button_width + 1),
                area.bottom().saturating_sub(1),
                button_width,
                1,
            );
            block.title_bottom(ratatui::text::Line::from("[F3 Настройки]").right_aligned())
        };
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

pub fn run(config: Config, mode: Mode, config_path: PathBuf) -> Result<(), String> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("Режим сравнения требует интерактивный терминал".into());
    }
    let mut app = App::new(&config, mode)?;
    app.config_path = config_path;
    let windows = WindowConfig::load(&app.config_path, app.window_config.clone())?;
    app.apply_windows(windows)?;
    let mut terminal = ratatui::try_init().map_err(|e| e.to_string())?;
    let result = (|| -> io::Result<()> {
        crossterm::execute!(
            io::stdout(),
            event::EnableBracketedPaste,
            event::EnableMouseCapture
        )?;
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
                    TerminalEvent::Mouse(mouse)
                        if mouse.kind == event::MouseEventKind::Down(event::MouseButton::Left) =>
                    {
                        app.click(mouse.column, mouse.row);
                    }
                    TerminalEvent::Paste(text) if app.config_editor.is_some() => {
                        if let Some(editor) = &mut app.config_editor {
                            editor.values[editor.selected].push_str(text.trim());
                        }
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
    let _ = crossterm::execute!(
        io::stdout(),
        event::DisableBracketedPaste,
        event::DisableMouseCapture
    );
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
        "# {}\n\n{}\n\n## Задача\n\n{}\n\n## Эталон\n\n{reference}\n\n",
        app.mode.title(),
        app.run_settings,
        app.task
    );
    for answer in &app.answers {
        report.push_str(&format!(
            "## {}\n\nСтатус: {}. Время: {:.1} с. Слов: {}. Оценка пользователя: {}.\n\n",
            answer.title,
            answer.status,
            answer.seconds,
            answer.text.split_whitespace().count(),
            VERDICTS[answer.verdict]
        ));
        report.push_str(&format!(
            "{}\n\n{}\n\n",
            answer.settings,
            answer.tokens.summary(answer.method)
        ));
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
    let prefix = if app.mode == Mode::Reasoning {
        "day3"
    } else {
        "temperatures"
    };
    let path = format!("reports/{prefix}-{timestamp}.md");
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
        let mut app = App::new(&config, Mode::Reasoning).unwrap();
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

    #[tokio::test]
    async fn temperature_mode_sends_four_identical_requests_except_temperature() {
        use serde_json::{Value, json};
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({
                "choices": [{"delta": {"content": "Result"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            })
        );
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .expect(4)
            .mount(&server)
            .await;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "api_key = \"test\"\nbase_url = \"{}\"\ntemperature = 0.42",
            server.uri()
        )
        .unwrap();
        let config = Config::load(file.path(), None).unwrap();
        let mut app = App::new(&config, Mode::Temperatures).unwrap();
        let mut custom = WindowConfig::from_base(&config, TEMPERATURES);
        custom.windows[1].temperature = 0.4;
        app.apply_windows(custom).unwrap();
        app.input = "Один и тот же вопрос".into();
        app.start();
        app.auto_saved = true; // Avoid creating report files in this transport test.
        tokio::time::timeout(Duration::from_secs(3), async {
            while app.running() {
                app.drain();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let requests = server.received_requests().await.unwrap();
        let mut seen = Vec::new();
        let mut expected = None;
        for request in requests {
            let mut body: Value = request.body_json().unwrap();
            seen.push(body["temperature"].as_f64().unwrap());
            assert_eq!(
                body["messages"],
                json!([{"role": "user", "content": app.input}])
            );
            assert_eq!(body["thinking"], json!({"type": "disabled"}));
            body.as_object_mut().unwrap().remove("temperature");
            if let Some(expected) = &expected {
                assert_eq!(&body, expected);
            } else {
                expected = Some(body);
            }
        }
        seen.sort_by(f64::total_cmp);
        assert_eq!(seen, [0.0, 0.4, 1.0, 1.2]);
        for answer in &app.answers {
            assert_eq!(answer.text, "Result");
            assert!(answer.prompt.is_empty());
            assert!(answer.tokens.summary(answer.method).contains("1/1"));
        }
        app.key(KeyCode::Char('p'), KeyModifiers::NONE);
        assert!(!app.show_prompt);
        let mut terminal = Terminal::new(TestBackend::new(140, 42)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let rendered: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        for label in [
            "temperature = 0",
            "temperature = 0.4",
            "temperature = 1.2",
            "temperature = 1",
        ] {
            assert!(rendered.contains(label), "missing {label}");
        }
    }
    #[tokio::test]
    async fn each_panel_uses_its_own_endpoint_key_prompt_and_options() {
        use serde_json::{Value, json};
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let servers = [MockServer::start().await, MockServer::start().await];
        for server in &servers {
            Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
                .expect(2).mount(server).await;
        }
        let config = Config::from_toml("api_key = \"base-key\"", None).unwrap();
        let mut app = App::new(&config, Mode::Temperatures).unwrap();
        let mut windows = WindowConfig::from_base(&config, TEMPERATURES);
        for (i, w) in windows.windows.iter_mut().enumerate() {
            w.base_url = format!("{}/v1", servers[i % 2].uri());
            w.api_key = format!("panel-key-{i}");
            w.model = format!("model-{i}");
            w.system_prompt = format!("System {i}\nSecond line");
            w.max_tokens = 200 + i as u32;
            w.top_p = Some(0.8);
            w.stop = vec!["END".into()];
            w.thinking = if i % 2 == 0 {
                None
            } else {
                Some("enabled".into())
            };
            w.include_usage = i % 2 != 0;
        }
        app.apply_windows(windows).unwrap();
        app.input = "Question".into();
        app.start();
        app.auto_saved = true;
        tokio::time::timeout(Duration::from_secs(3), async {
            while app.running() {
                app.drain();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for (server_index, server) in servers.iter().enumerate() {
            for request in server.received_requests().await.unwrap() {
                let body: Value = request.body_json().unwrap();
                let i = body["model"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("model-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                assert_eq!(i % 2, server_index);
                assert_eq!(request.url.path(), "/v1/chat/completions");
                assert_eq!(
                    request
                        .headers
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    format!("Bearer panel-key-{i}")
                );
                assert_eq!(
                    body["messages"],
                    json!([{"role":"system","content":format!("System {i}\nSecond line")},{"role":"user","content":"Question"}])
                );
                assert_eq!(body["max_tokens"], 200 + i as u32);
                assert_eq!(body["top_p"], 0.8);
                assert_eq!(body["stop"], json!(["END"]));
                if i % 2 == 0 {
                    assert!(body.get("thinking").is_none());
                    assert!(body.get("stream_options").is_none());
                } else {
                    assert_eq!(body["thinking"]["type"], "enabled");
                }
            }
        }
    }

    #[test]
    fn editor_changes_only_selected_file_masks_key_and_keeps_old_results() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config::from_toml("api_key = \"private-original-key\"", None).unwrap();
        let mut app = App::new(&config, Mode::Temperatures).unwrap();
        app.config_path = dir.path().to_path_buf();
        let windows = WindowConfig::load(dir.path(), app.window_config.clone()).unwrap();
        app.apply_windows(windows).unwrap();
        let neighbor = std::fs::read(WindowConfig::path(dir.path(), 0)).unwrap();
        app.open_editor(2);
        app.config_editor.as_mut().unwrap().selected = 3;
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).unwrap();
        terminal.draw(|f| app.draw(f)).unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(!text.contains("private-original-key"));
        app.config_editor.as_mut().unwrap().values[0] = "3".into();
        app.save_editor();
        assert!(app.config_editor.is_some());
        assert_eq!(app.window_config.windows[2].temperature, 1.2);
        app.config_editor.as_mut().unwrap().values[0] = "0.3".into();
        app.config_editor.as_mut().unwrap().values[4] = "Новый\nпромпт".into();
        app.started = Some(Instant::now());
        for a in &mut app.answers {
            a.finished = true;
        }
        let old = app.answers[2].settings.clone();
        app.save_editor();
        assert!(app.config_editor.is_none());
        assert_eq!(app.answers[2].settings, old);
        assert_eq!(
            std::fs::read(WindowConfig::path(dir.path(), 0)).unwrap(),
            neighbor
        );
        let read = WindowConfig::load(dir.path(), app.window_config.clone()).unwrap();
        assert_eq!(read.windows[2].system_prompt, "Новый\nпромпт");
        app.open_editor(2);
        app.config_editor.as_mut().unwrap().values[0] = "0.9".into();
        app.editor_key(KeyCode::Esc, KeyModifiers::NONE);
        assert_eq!(app.window_config.windows[2].temperature, 0.3);
    }
}
