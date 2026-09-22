use deepseek_cli::chat::{Message, Role};
use deepseek_cli::config::{Config, ContextConfig};
use deepseek_cli::context::{ContextState, ContextSummary};
use deepseek_cli::dialog::DialogStore;
use deepseek_cli::facts::{FactsState, plan_facts_update};
use deepseek_cli::system_context::{CompactionPolicy, ContextScope, SystemBlock};
use deepseek_cli::workflow::{StageRunId, WorkflowTaskId, WorkflowTaskState};
use deepseek_cli::workflow_context::{
    WorkflowRequestInput, facts_candidates, plan_stage_compaction, prepare_workflow_request,
    stage_history, workflow_task_block,
};
use deepseek_cli::workflow_store::WorkflowRepository;
use deepseek_cli::workflow_store::{ProtocolSource, StageProtocolMessage};

fn config(strategy: &str, keep: usize) -> ContextConfig {
    Config::from_toml(
        &format!("api_key='key'\n[context]\nstrategy='{strategy}'\nkeep_last_messages={keep}"),
        None,
    )
    .unwrap()
    .context()
    .clone()
}

fn task() -> WorkflowTaskState {
    WorkflowTaskState::new(
        WorkflowTaskId(1),
        1,
        1,
        "current goal".into(),
        StageRunId(1),
    )
    .unwrap()
}

fn row(id: i64, source: ProtocolSource, content: &str) -> StageProtocolMessage {
    StageProtocolMessage {
        message_id: id,
        message: Message::for_request(
            if source == ProtocolSource::Assistant {
                Role::Assistant
            } else {
                Role::User
            },
            content,
        ),
        source,
    }
}

fn inherited() -> Vec<SystemBlock> {
    [
        ("user_profile", ContextScope::User),
        ("user_memory", ContextScope::User),
        ("task_memory", ContextScope::Task),
    ]
    .into_iter()
    .map(|(name, scope)| {
        SystemBlock::new(
            name,
            format!("{name} protected"),
            scope,
            CompactionPolicy::Exclude,
        )
    })
    .collect()
}

#[test]
fn ordinary_request_contains_current_stage_protocol_in_stable_order_without_phantom_input() {
    let rows = vec![
        row(41, ProtocolSource::Human, "validate it"),
        row(42, ProtocolSource::Controller, "run focused tests"),
        row(43, ProtocolSource::Assistant, "tests pass"),
    ];
    let result = prepare_workflow_request(WorkflowRequestInput {
        base_prompt: "BASE",
        inherited_blocks: inherited(),
        task: &task(),
        stage_messages: &rows,
        pending_input: None,
        context_config: &config("summary", 2),
        context_state: &ContextState::default(),
        facts_state: &FactsState::default(),
    });
    assert_eq!(
        result.prepared.system_block_names(),
        [
            "base",
            "user_profile",
            "user_memory",
            "task_memory",
            "workflow_task"
        ]
    );
    assert_eq!(
        result
            .prepared
            .messages()
            .iter()
            .filter(|m| m.role() != Role::System)
            .map(|m| (m.role(), m.content()))
            .collect::<Vec<_>>(),
        [
            (Role::User, "validate it"),
            (Role::User, "run focused tests"),
            (Role::Assistant, "tests pass")
        ]
    );
    let metadata = result.prepared.system_block_metadata();
    assert_eq!(
        metadata.iter().map(|m| m.scope).collect::<Vec<_>>(),
        [
            ContextScope::Application,
            ContextScope::User,
            ContextScope::User,
            ContextScope::Task,
            ContextScope::Task
        ]
    );
    assert!(
        metadata
            .iter()
            .all(|m| m.compaction == CompactionPolicy::Exclude)
    );
}

#[test]
fn each_strategy_selects_inside_the_stage_and_pending_input_counts_once() {
    let rows = vec![
        row(1, ProtocolSource::Human, "first"),
        row(2, ProtocolSource::Controller, "controller"),
        row(3, ProtocolSource::Assistant, "answer"),
        row(4, ProtocolSource::Human, "last"),
    ];
    let context = ContextState::with_summary(ContextSummary::new("stage summary", 2));
    let facts =
        FactsState::default().updated([("deadline".into(), "Friday".into())].into(), 2, None);
    for (strategy, want, block) in [
        (
            "summary",
            vec!["answer", "last", "pending"],
            Some("summary"),
        ),
        ("sliding_window", vec!["last", "pending"], None),
        ("sticky_facts", vec!["last", "pending"], Some("facts")),
        (
            "branching",
            vec!["first", "controller", "answer", "last", "pending"],
            None,
        ),
    ] {
        let result = prepare_workflow_request(WorkflowRequestInput {
            base_prompt: "BASE",
            inherited_blocks: inherited(),
            task: &task(),
            stage_messages: &rows,
            pending_input: Some("pending"),
            context_config: &config(strategy, 2),
            context_state: &context,
            facts_state: &facts,
        });
        assert_eq!(
            result
                .prepared
                .messages()
                .iter()
                .filter(|m| m.role() != Role::System)
                .map(Message::content)
                .collect::<Vec<_>>(),
            want,
            "{strategy}"
        );
        let mut names = vec![
            "base",
            "user_profile",
            "user_memory",
            "task_memory",
            "workflow_task",
        ];
        names.extend(block);
        assert_eq!(result.prepared.system_block_names(), names, "{strategy}");
        assert_eq!(result.context_state, context);
        assert_eq!(result.facts_state, facts);
    }
}

#[test]
fn controller_is_summary_context_but_never_user_fact_evidence() {
    let rows = vec![
        row(1, ProtocolSource::Human, "deadline Friday"),
        row(2, ProtocolSource::Controller, "run cargo test"),
        row(3, ProtocolSource::Assistant, "tests passed"),
    ];
    assert_eq!(stage_history(&rows).messages().len(), 3);
    let mut blocks = inherited();
    blocks.push(workflow_task_block(&task()));
    let summary = plan_stage_compaction(&rows, &ContextState::default(), 1, &blocks).unwrap();
    assert!(
        !summary.request_messages()[1]
            .content()
            .contains("current goal")
    );
    assert_eq!(summary.covered_message_count(), 2);
    assert!(
        summary.request_messages()[1]
            .content()
            .contains("run cargo test")
    );
    assert!(
        !summary.request_messages()[1]
            .content()
            .contains("protected")
    );
    let candidates = facts_candidates(&rows);
    assert_eq!(
        candidates.iter().map(Message::content).collect::<Vec<_>>(),
        ["deadline Friday", "tests passed"]
    );
    let facts = plan_facts_update(&candidates, &FactsState::default()).unwrap();
    assert_eq!(facts.covered_message_count(), 2);
    assert!(
        facts.request_messages()[1]
            .content()
            .contains("deadline Friday")
    );
    assert!(
        !facts.request_messages()[1]
            .content()
            .contains("run cargo test")
    );
    assert!(
        !facts.request_messages()[1]
            .content()
            .contains("tests passed")
    );
    let covered = FactsState::default().updated(Default::default(), 2, None);
    assert!(plan_facts_update(&candidates, &covered).is_none());
}

#[test]
fn repository_request_and_reductions_cannot_leak_old_tasks_stages_handoffs_or_unmapped_messages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("isolation.sqlite3");
    let mut store = DialogStore::open(&path).unwrap();
    store.start_dialog("BASE", "OLD_TASK_MARKER").unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch(r#"
        PRAGMA foreign_keys=ON;
        BEGIN;
        INSERT INTO workflow_tasks (id,dialog_id,ordinal,phase,status,goal,plan_json,checkpoint_json,version)
        VALUES (1,1,1,'done','active','OLD_GOAL_MARKER','{"revision":0,"steps":[],"acceptance_criteria":[]}',
                '{"summary":"OLD_CHECKPOINT_MARKER","decisions":[],"open_issues":[]}',0),
               (2,1,2,'validation','active','CURRENT_GOAL','{"revision":0,"steps":[],"acceptance_criteria":[]}',
                '{"summary":"PROJECTED_CHECKPOINT","decisions":["PROJECTED_DECISION"],"open_issues":[]}',0);
        INSERT INTO task_stage_runs (id,workflow_task_id,phase,sequence,finished_at)
        VALUES (1,1,'planning',1,'9999-01-01'),(2,1,'done',2,NULL),
               (3,2,'execution',1,'9999-01-01'),(4,2,'validation',2,NULL);
        UPDATE workflow_tasks SET current_stage_run_id=2 WHERE id=1;
        UPDATE workflow_tasks SET current_stage_run_id=4 WHERE id=2;
        INSERT INTO dialog_workflow_state VALUES (1,2);
        INSERT INTO messages (id,dialog_id,role,content) VALUES
            (2,1,'user','LEGACY_AFTER_DONE_MARKER'),
            (3,1,'assistant','OLD_STAGE_MARKER'),
            (4,1,'user','validate it'),
            (5,1,'user','run focused tests'),
            (6,1,'assistant','tests pass');
        INSERT INTO message_task_stages VALUES (1,1,1),(3,2,3),(4,2,4),(5,2,4),(6,2,4);
        INSERT INTO response_processing (id,assistant_message_id,checker_name,expected_version,status)
        VALUES (1,3,'continuation',0,'completed');
        INSERT INTO workflow_inputs (dialog_id,message_id,source,checker_name,model_name,triggering_assistant_message_id,intent_json,outcome,processing_id)
        VALUES (1,5,'controller','continuation','checker',3,'{"continue":{"instruction":"run focused tests"}}','accepted',1);
        INSERT INTO task_transitions (id,workflow_task_id,from_stage_run_id,to_stage_run_id,event,source_version,handoff_json,workflow_input_id)
        VALUES (1,2,3,4,'execution_completed',0,'{"summary":"FULL_HANDOFF_MARKER"}',1);
        UPDATE workflow_tasks SET incoming_handoff_id=1 WHERE id=2;
        INSERT INTO task_stage_context (stage_run_id,context_json,facts_json)
        VALUES (3,'{"summary":{"content":"OLD_SUMMARY_MARKER","covered_message_count":1},"compaction_usage":{"call_count":0,"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"missing_usage_count":0}}',
        '{"facts":{"old":"OLD_FACT_MARKER"},"covered_message_count":1,"update_usage":{"call_count":0,"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"missing_usage_count":0}}');
        COMMIT;
    "#).unwrap();
    // Drop legacy reduction tables: managed loading must have no dependency on either.
    connection
        .execute_batch("DROP TABLE dialog_context; DROP TABLE dialog_facts;")
        .unwrap();
    let task = store.load_workflow(1).unwrap().current_task.unwrap();
    let rows = store
        .load_stage_messages(task.current_stage_run_id)
        .unwrap();
    let reductions = store
        .load_stage_reductions(task.current_stage_run_id)
        .unwrap();
    assert_eq!(
        rows.iter().map(|m| m.message_id).collect::<Vec<_>>(),
        [4, 5, 6]
    );
    for strategy in ["summary", "sliding_window", "sticky_facts", "branching"] {
        let result = prepare_workflow_request(WorkflowRequestInput {
            base_prompt: "BASE",
            inherited_blocks: inherited(),
            task: &task,
            stage_messages: &rows,
            pending_input: None,
            context_config: &config(strategy, 10),
            context_state: &reductions.context,
            facts_state: &reductions.facts,
        });
        let prompt = result
            .prepared
            .messages()
            .iter()
            .map(Message::content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(prompt.contains("CURRENT_GOAL"));
        assert!(prompt.contains("PROJECTED_CHECKPOINT"));
        assert!(prompt.contains("PROJECTED_DECISION"));
        assert!(prompt.contains("run focused tests"));
        assert!(!prompt.contains("MARKER"), "{strategy}: {prompt}");
        assert!(!prompt.contains("incoming_handoff_id"));
    }
    let compaction = plan_stage_compaction(&rows, &reductions.context, 1, &inherited()).unwrap();
    assert!(
        compaction.request_messages()[1]
            .content()
            .contains("run focused tests")
    );
    assert!(
        !compaction.request_messages()[1]
            .content()
            .contains("MARKER")
    );
    let facts = plan_facts_update(&facts_candidates(&rows), &reductions.facts).unwrap();
    assert!(
        facts.request_messages()[1]
            .content()
            .contains("validate it")
    );
    assert!(
        !facts.request_messages()[1]
            .content()
            .contains("run focused tests")
    );
    assert!(!facts.request_messages()[1].content().contains("MARKER"));
}
