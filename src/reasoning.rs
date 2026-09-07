use crate::chat::ChatHistory;
use crate::client::{ClientError, DeepSeekClient, StreamEvent, TokenUsage};

pub const SAMPLE_TASK: &str = "Реши задачу о рюкзаке. Вместимость — 10 кг. Каждый предмет можно взять только один раз: A — 6 кг, ценность 30; B — 5 кг, ценность 24; C — 5 кг, ценность 24; D — 3 кг, ценность 13; E — 2 кг, ценность 9. Какие предметы выбрать, чтобы суммарная ценность была максимальной? Укажи предметы, общий вес и максимальную ценность.";
pub const SAMPLE_REFERENCE: &str = "B + C: общий вес 10 кг, максимальная ценность 48. Проверено полным перебором всех 32 подмножеств. Предмет A имеет лучшую ценность на кг, но жадный выбор A не оптимален.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Direct,
    StepByStep,
    GeneratedPrompt,
    Experts,
}

impl Method {
    pub const ALL: [Self; 4] = [
        Self::Direct,
        Self::StepByStep,
        Self::GeneratedPrompt,
        Self::Experts,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Direct => "1. Прямой ответ",
            Self::StepByStep => "2. Решай пошагово",
            Self::GeneratedPrompt => "3. Сначала создай промпт",
            Self::Experts => "4. Группа экспертов",
        }
    }

    pub fn prompt(self, task: &str) -> String {
        match self {
            Self::Direct => task.to_owned(),
            Self::StepByStep => format!(
                "{task}\n\nРешай пошагово. Покажи основные шаги решения и проверку результата."
            ),
            Self::GeneratedPrompt => format!(
                "Составь эффективный промпт для решения задачи ниже. Верни только промпт, без решения задачи и без вступления.\n\nЗадача:\n{task}"
            ),
            Self::Experts => format!(
                "{task}\n\nПредставь группу экспертов: аналитик, инженер и критик. Каждый должен дать собственное решение задачи и итоговый ответ под отдельным заголовком. Аналитик оценивает ограничения, инженер предлагает алгоритм и проверку, критик ищет ошибки и контрпримеры. Заверши общим выводом, явно укажи разногласия, если они есть."
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Prompt,
    Answer,
}

pub enum Event {
    Started(Phase),
    Text(Phase, String),
    Usage(Phase, TokenUsage),
}

/// Store the latest usage per request so repeated streaming metadata isn't added twice.
#[derive(Default)]
pub struct TokenAccounting {
    prompt: Option<TokenUsage>,
    answer: Option<TokenUsage>,
}

impl TokenAccounting {
    pub fn record(&mut self, phase: Phase, usage: TokenUsage) {
        match phase {
            Phase::Prompt => self.prompt = Some(usage),
            Phase::Answer => self.answer = Some(usage),
        }
    }

    pub fn total(&self) -> Option<TokenUsage> {
        let mut entries = self.prompt.iter().chain(self.answer.iter()).peekable();
        entries.peek()?;
        Some(
            entries.fold(TokenUsage::default(), |sum, usage| TokenUsage {
                prompt_tokens: sum.prompt_tokens + usage.prompt_tokens,
                completion_tokens: sum.completion_tokens + usage.completion_tokens,
                total_tokens: sum.total_tokens + usage.total_tokens,
            }),
        )
    }

    pub fn summary(&self, method: Method) -> String {
        let expected = if method == Method::GeneratedPrompt {
            2
        } else {
            1
        };
        let received = usize::from(self.prompt.is_some()) + usize::from(self.answer.is_some());
        match self.total() {
            Some(usage) => format!(
                "Токены: вход {}, выход {}, всего {} (учтено {received}/{expected} запросов)",
                usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
            ),
            None => "Токены: нет данных API".into(),
        }
    }
}

/// Every request gets a fresh history and the same client settings.
/// The generated prompt is used only after its first API call succeeds.
pub async fn solve(
    client: &DeepSeekClient,
    task: &str,
    method: Method,
    emit: impl FnMut(Event),
) -> Result<String, ClientError> {
    solve_with_system(client, task, method, "", emit).await
}

pub async fn solve_with_system(
    client: &DeepSeekClient,
    task: &str,
    method: Method,
    system_prompt: &str,
    mut emit: impl FnMut(Event),
) -> Result<String, ClientError> {
    let history = ChatHistory::new(system_prompt.to_owned());
    let phase = if method == Method::GeneratedPrompt {
        Phase::Prompt
    } else {
        Phase::Answer
    };
    emit(Event::Started(phase));
    let first = client
        .stream_chat_events(&history.request_messages(&method.prompt(task)), |event| {
            match event {
                StreamEvent::Text(text) => emit(Event::Text(phase, text.to_owned())),
                StreamEvent::Usage(usage) => emit(Event::Usage(phase, usage)),
            }
            Ok(())
        })
        .await?;
    if first.trim().is_empty() {
        return Err(ClientError::EmptyAnswer);
    }
    if method != Method::GeneratedPrompt {
        return Ok(first);
    }
    emit(Event::Started(Phase::Answer));
    let request = format!("{first}\n\nИсходная задача (используй именно эти условия):\n{task}");
    let answer = client
        .stream_chat_events(&history.request_messages(&request), |event| {
            match event {
                StreamEvent::Text(text) => emit(Event::Text(Phase::Answer, text.to_owned())),
                StreamEvent::Usage(usage) => emit(Event::Usage(Phase::Answer, usage)),
            }
            Ok(())
        })
        .await?;
    if answer.trim().is_empty() {
        return Err(ClientError::EmptyAnswer);
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    #[test]
    fn sample_reference_is_the_unique_optimum() {
        let items = [(6, 30), (5, 24), (5, 24), (3, 13), (2, 9)];
        let mut best = (0, Vec::new());
        for mask in 0..32 {
            let (weight, value) = items
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .fold((0, 0), |(w, v), (_, (iw, iv))| (w + iw, v + iv));
            if weight <= 10 {
                if value > best.0 {
                    best = (value, vec![mask]);
                } else if value == best.0 {
                    best.1.push(mask);
                }
            }
        }
        assert_eq!(best, (48, vec![0b00110]));
    }
}
