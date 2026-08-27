use rmcp::{
    Json, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use time_strike::clock::MonotonicClock;
use time_strike::policy::{PolicyDecision, PolicyInput, ScheduleStatus, evaluate_time_policy};
use time_strike::{
    AdjustTaskRequest, CheckpointRequest, FileStore, FinishTaskRequest, StartTaskRequest,
    TaskManager, TaskView, TickRequest,
};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct AppConfig {
    persistence: PersistenceConfig,
    defaults: DefaultsConfig,
    output: OutputConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PersistenceConfig {
    enabled: bool,
    path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct DefaultsConfig {
    validation_reserve_percent: Option<f64>,
    finalization_reserve_percent: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct OutputConfig {
    compact: bool,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self { compact: true }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StartInput {
    /// Short human objective; stored truncated and never returned by tick.
    objective: Option<String>,
    /// Optional stable identifier. Generated when omitted.
    task_id: Option<String>,
    budget_seconds: f64,
    parent_task_id: Option<String>,
    validation_reserve_percent: Option<f64>,
    finalization_reserve_percent: Option<f64>,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct TickInput {
    /// Uses the most recently started task when omitted.
    task_id: Option<String>,
    progress_percent: Option<f64>,
    estimated_remaining_work_seconds: Option<f64>,
    current_action: Option<String>,
    current_action_estimated_seconds: Option<f64>,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CheckpointInput {
    task_id: Option<String>,
    progress_percent: Option<f64>,
    estimated_remaining_work_seconds: Option<f64>,
    #[serde(default)]
    completed: Vec<String>,
    note: Option<String>,
    #[serde(default)]
    plan_complete: bool,
    #[serde(default)]
    replan: bool,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AdjustInput {
    task_id: Option<String>,
    add_seconds: Option<f64>,
    remove_seconds: Option<f64>,
    set_total_seconds: Option<f64>,
    #[serde(default)]
    verbose: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FinishInput {
    task_id: Option<String>,
    reason: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
struct StartOutput {
    task_id: String,
    remaining_seconds: f64,
    mode: String,
    next_check_seconds: f64,
    clamped: bool,
    planning_budget_seconds: f64,
    directive: String,
    max_new_action_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct TickOutput {
    remaining_seconds: f64,
    elapsed_seconds: f64,
    remaining_percent: f64,
    mode: String,
    schedule: String,
    usable_work_seconds: f64,
    reserved_seconds: f64,
    max_new_action_seconds: f64,
    next_check_seconds: f64,
    must_converge: bool,
    must_validate: bool,
    must_finalize: bool,
    must_stop: bool,
    directive: String,
    must_plan: bool,
    planning_seconds_remaining: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_fits: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct CheckpointOutput {
    task_id: String,
    checkpoint_count: u64,
    remaining_seconds: f64,
    mode: String,
    schedule: String,
    next_check_seconds: f64,
    must_converge: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
struct AdjustOutput {
    task_id: String,
    total_budget_seconds: f64,
    remaining_seconds: f64,
    mode: String,
    clamped: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
struct FinishOutput {
    elapsed_seconds: f64,
    budget_seconds: f64,
    unused_seconds: f64,
    overrun_seconds: f64,
    budget_used_percent: f64,
    checkpoints: u64,
    deadline_met: bool,
}

#[derive(Clone)]
struct TimeStrikeServer {
    manager: Arc<TaskManager>,
    active_task: Arc<RwLock<Option<String>>>,
    ids: Arc<AtomicU64>,
    default_validation_percent: Option<f64>,
    default_finalization_percent: Option<f64>,
    default_verbose: bool,
    allow_budget_increase: bool,
    #[allow(dead_code)] // read by rmcp-generated routing code
    tool_router: ToolRouter<Self>,
}

impl TimeStrikeServer {
    fn new() -> Result<Self, String> {
        let config = load_config()?;
        let configured_state = if config.persistence.enabled {
            Some(config.persistence.path.unwrap_or_else(default_state_path))
        } else {
            None
        };
        let state_path = std::env::var_os("TIME_STRIKE_STATE")
            .map(PathBuf::from)
            .or(configured_state);
        let manager = if let Some(path) = state_path {
            let store = FileStore::new(path)?;
            TaskManager::with_store(MonotonicClock::new(), Arc::new(store))
                .map_err(|error| error.to_string())?
        } else {
            TaskManager::new(MonotonicClock::new())
        };
        Ok(Self {
            manager: Arc::new(manager),
            active_task: Arc::new(RwLock::new(None)),
            ids: Arc::new(AtomicU64::new(1)),
            default_validation_percent: config.defaults.validation_reserve_percent,
            default_finalization_percent: config.defaults.finalization_reserve_percent,
            default_verbose: !config.output.compact,
            allow_budget_increase: std::env::var("TIME_STRIKE_ALLOW_BUDGET_INCREASE")
                .map(|value| {
                    value == "1"
                        || value.eq_ignore_ascii_case("true")
                        || value.eq_ignore_ascii_case("yes")
                })
                .unwrap_or(false),
            tool_router: Self::tool_router(),
        })
    }

    fn resolve_task_id(&self, task_id: Option<String>) -> Result<String, String> {
        if let Some(task_id) = task_id {
            return Ok(task_id);
        }
        self.active_task
            .read()
            .map_err(|_| "active task lock poisoned".to_string())?
            .clone()
            .ok_or_else(|| "task_id required: no active task".to_string())
    }

    fn generated_task_id(&self) -> String {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let sequence = self.ids.fetch_add(1, Ordering::Relaxed);
        format!("pt-{epoch_ms}-{sequence}")
    }

    fn reserve_percent(view: &TaskView, key: &str) -> Option<f64> {
        view.metadata.get(key).and_then(|value| value.parse().ok())
    }

    fn policy(
        view: &TaskView,
        progress_percent: Option<f64>,
        eta_seconds: Option<f64>,
    ) -> PolicyDecision {
        let stored_progress = view
            .last_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.progress)
            .map(|progress| progress * 100.0);
        let stored_eta = view
            .last_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.estimated_remaining_work_secs);
        evaluate_time_policy(PolicyInput {
            total_secs: view.budget_secs,
            elapsed_secs: view.elapsed_secs,
            progress_percent: progress_percent.or(stored_progress),
            estimated_remaining_work_secs: eta_seconds.or(stored_eta),
            validation_reserve_percent: Self::reserve_percent(view, "validation_reserve_percent"),
            finalization_reserve_percent: Self::reserve_percent(
                view,
                "finalization_reserve_percent",
            ),
        })
    }

    fn tick_output(
        view: &TaskView,
        decision: PolicyDecision,
        action_seconds: Option<f64>,
        verbose: bool,
    ) -> TickOutput {
        let plan_required = !view.plan_submitted;
        let effective_max_action = if plan_required {
            0.0
        } else {
            decision.max_new_action_secs
        };
        let action_fits = action_seconds.map(|seconds| seconds <= effective_max_action);
        TickOutput {
            remaining_seconds: round3(decision.remaining_secs),
            elapsed_seconds: round3(view.elapsed_secs),
            remaining_percent: round3(decision.remaining_percent),
            mode: if plan_required {
                "plan".into()
            } else {
                format!("{:?}", decision.mode).to_ascii_lowercase()
            },
            schedule: schedule_name(decision.schedule).to_string(),
            usable_work_seconds: round3(decision.usable_work_secs),
            reserved_seconds: round3(decision.reserved_secs),
            max_new_action_seconds: round3(effective_max_action),
            next_check_seconds: round3(decision.next_check_secs),
            must_converge: decision.must_converge,
            must_validate: decision.must_validate,
            must_finalize: decision.must_finalize,
            must_stop: decision.must_stop,
            directive: directive(plan_required, &decision, action_fits).to_string(),
            must_plan: plan_required,
            planning_seconds_remaining: round3(if plan_required {
                (planning_budget_secs(view.budget_secs) - view.elapsed_secs).max(0.0)
            } else {
                0.0
            }),
            action_fits,
            reason: verbose.then(|| decision.reason.to_string()),
        }
    }
}

#[tool_router(router = tool_router)]
impl TimeStrikeServer {
    #[tool(
        description = "Start a hard time budget. The first required action is submitting a compact plan through checkpoint."
    )]
    async fn start_task(
        &self,
        Parameters(input): Parameters<StartInput>,
    ) -> Result<Json<StartOutput>, String> {
        let task_id = input.task_id.unwrap_or_else(|| self.generated_task_id());
        let mut request = StartTaskRequest::new(&task_id, input.budget_seconds);
        if let Some(parent_id) = input.parent_task_id {
            request = request.with_parent(parent_id);
        }
        let mut metadata = HashMap::new();
        if let Some(objective) = input.objective {
            metadata.insert(
                "objective".to_string(),
                objective.chars().take(256).collect(),
            );
        }
        if let Some(percent) = input
            .validation_reserve_percent
            .or(self.default_validation_percent)
        {
            metadata.insert(
                "validation_reserve_percent".to_string(),
                percent.to_string(),
            );
        }
        if let Some(percent) = input
            .finalization_reserve_percent
            .or(self.default_finalization_percent)
        {
            metadata.insert(
                "finalization_reserve_percent".to_string(),
                percent.to_string(),
            );
        }
        request.metadata = metadata;
        let outcome = self
            .manager
            .start_task(request)
            .map_err(|error| error.to_string())?;
        *self
            .active_task
            .write()
            .map_err(|_| "active task lock poisoned".to_string())? = Some(task_id.clone());
        let decision = Self::policy(&outcome.task, Some(0.0), None);
        Ok(Json(StartOutput {
            task_id,
            remaining_seconds: round3(decision.remaining_secs),
            mode: "plan".into(),
            next_check_seconds: round3(planning_budget_secs(outcome.task.budget_secs)),
            clamped: outcome.clamped,
            planning_budget_seconds: round3(planning_budget_secs(outcome.task.budget_secs)),
            directive: "submit_plan".into(),
            max_new_action_seconds: 0.0,
            reason: (input.verbose || self.default_verbose).then(|| decision.reason.to_string()),
        }))
    }

    #[tool(
        description = "Return the mandatory next directive, remaining budget, maximum action duration, and deadline pressure."
    )]
    async fn tick(
        &self,
        Parameters(input): Parameters<TickInput>,
    ) -> Result<Json<TickOutput>, String> {
        let task_id = self.resolve_task_id(input.task_id)?;
        let outcome = self
            .manager
            .tick(TickRequest::new(task_id))
            .map_err(|error| error.to_string())?;
        let decision = Self::policy(
            &outcome.task,
            input.progress_percent,
            input.estimated_remaining_work_seconds,
        );
        let _ = input.current_action;
        Ok(Json(Self::tick_output(
            &outcome.task,
            decision,
            input.current_action_estimated_seconds,
            input.verbose || self.default_verbose,
        )))
    }

    #[tool(
        description = "Submit the initial plan or record progress and ETA. The first checkpoint must use plan_complete=true."
    )]
    async fn checkpoint(
        &self,
        Parameters(input): Parameters<CheckpointInput>,
    ) -> Result<Json<CheckpointOutput>, String> {
        let task_id = self.resolve_task_id(input.task_id)?;
        let note = input.note.or_else(|| {
            (!input.completed.is_empty()).then(|| {
                input
                    .completed
                    .iter()
                    .take(4)
                    .map(|item| item.chars().take(64).collect::<String>())
                    .collect::<Vec<_>>()
                    .join("; ")
            })
        });
        let outcome = self
            .manager
            .checkpoint(CheckpointRequest {
                task_id: task_id.clone(),
                note,
                progress: input.progress_percent.map(|progress| progress / 100.0),
                estimated_remaining_work_secs: input.estimated_remaining_work_seconds,
                plan_complete: input.plan_complete,
                replan: input.replan,
            })
            .map_err(|error| error.to_string())?;
        let decision = Self::policy(
            &outcome.task,
            input.progress_percent,
            input.estimated_remaining_work_seconds,
        );
        let mode = format!("{:?}", decision.mode).to_ascii_lowercase();
        let schedule = schedule_name(decision.schedule).to_string();
        let _ = input.verbose;
        Ok(Json(CheckpointOutput {
            task_id,
            checkpoint_count: outcome.task.checkpoints,
            remaining_seconds: round3(decision.remaining_secs),
            mode,
            schedule,
            next_check_seconds: round3(decision.next_check_secs),
            must_converge: decision.must_converge,
        }))
    }

    #[tool(description = "Adjust an active total budget by adding, removing, or setting seconds")]
    async fn adjust_task(
        &self,
        Parameters(input): Parameters<AdjustInput>,
    ) -> Result<Json<AdjustOutput>, String> {
        let task_id = self.resolve_task_id(input.task_id)?;
        let operation_count = [
            input.add_seconds.is_some(),
            input.remove_seconds.is_some(),
            input.set_total_seconds.is_some(),
        ]
        .into_iter()
        .filter(|set| *set)
        .count();
        if operation_count != 1 {
            return Err(
                "provide exactly one of add_seconds, remove_seconds, set_total_seconds".into(),
            );
        }
        let current = self
            .manager
            .get_task(&task_id)
            .map_err(|error| error.to_string())?;
        let new_total = if let Some(total) = input.set_total_seconds {
            total
        } else if let Some(add) = input.add_seconds {
            current.budget_secs + add
        } else {
            current.budget_secs - input.remove_seconds.unwrap_or_default()
        };
        if new_total > current.budget_secs && !self.allow_budget_increase {
            return Err("budget increase is disabled; only the host may grant more time".into());
        }
        let outcome = self
            .manager
            .adjust_task(AdjustTaskRequest::new(&task_id).with_budget(new_total))
            .map_err(|error| error.to_string())?;
        let decision = Self::policy(&outcome.task, None, None);
        let _ = input.verbose;
        Ok(Json(AdjustOutput {
            task_id,
            total_budget_seconds: round3(outcome.task.budget_secs),
            remaining_seconds: round3(decision.remaining_secs),
            mode: format!("{:?}", decision.mode).to_ascii_lowercase(),
            clamped: outcome.clamped,
        }))
    }

    #[tool(description = "Finish a budget and return compact deadline metrics")]
    async fn finish_task(
        &self,
        Parameters(input): Parameters<FinishInput>,
    ) -> Result<Json<FinishOutput>, String> {
        let task_id = self.resolve_task_id(input.task_id)?;
        let outcome = self
            .manager
            .finish_task(FinishTaskRequest {
                task_id: task_id.clone(),
                reason: input.reason,
                force: input.force,
            })
            .map_err(|error| error.to_string())?;
        let actual_elapsed = outcome.actual_elapsed_secs;
        let overrun = outcome.overrun_secs;
        let view = outcome.task;
        let unused = (view.budget_secs - actual_elapsed).max(0.0);
        let is_active = self
            .active_task
            .read()
            .map_err(|_| "active task lock poisoned".to_string())?
            .as_deref()
            == Some(task_id.as_str());
        if is_active {
            *self
                .active_task
                .write()
                .map_err(|_| "active task lock poisoned".to_string())? = None;
        }
        Ok(Json(FinishOutput {
            elapsed_seconds: round3(actual_elapsed),
            budget_seconds: round3(view.budget_secs),
            unused_seconds: round3(unused),
            overrun_seconds: round3(overrun),
            budget_used_percent: round3(actual_elapsed / view.budget_secs * 100.0),
            checkpoints: view.checkpoints,
            deadline_met: overrun <= f64::EPSILON,
        }))
    }
}

#[tool_handler(
    name = "time-strike",
    version = "0.2.0",
    instructions = "Immediately call start_task for deadline work. If directive=submit_plan, call checkpoint with plan_complete=true, a compact plan, and an ETA before any costly action. Call tick before and after searches, edits, tests, delegation, and tool calls. Never start work longer than max_new_action_seconds; split it. On converge_required_only stop exploration and perform required work only. On validate only verify. On finalize deliver. On stop return immediately. Never increase the budget."
)]
impl ServerHandler for TimeStrikeServer {}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn planning_budget_secs(total_secs: f64) -> f64 {
    (total_secs * 0.05).clamp(2.0, 60.0).min(total_secs * 0.15)
}

fn directive(
    plan_required: bool,
    decision: &PolicyDecision,
    action_fits: Option<bool>,
) -> &'static str {
    if decision.must_stop {
        "stop"
    } else if plan_required {
        "submit_plan"
    } else if action_fits == Some(false) {
        "split_action"
    } else if decision.must_finalize {
        "finalize"
    } else if decision.must_validate {
        "validate"
    } else if decision.must_converge {
        "converge_required_only"
    } else {
        "execute"
    }
}

fn schedule_name(schedule: ScheduleStatus) -> &'static str {
    match schedule {
        ScheduleStatus::Ahead => "ahead",
        ScheduleStatus::OnTrack => "on_track",
        ScheduleStatus::Behind => "behind",
        ScheduleStatus::Critical => "critical",
    }
}

fn load_config() -> Result<AppConfig, String> {
    let Some(path) = std::env::var_os("TIME_STRIKE_CONFIG") else {
        return Ok(AppConfig::default());
    };
    let path = PathBuf::from(path);
    let source = std::fs::read_to_string(&path)
        .map_err(|error| format!("cannot read config {}: {error}", path.display()))?;
    toml::from_str(&source).map_err(|error| format!("invalid config {}: {error}", path.display()))
}

fn default_state_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".time-strike/state.json")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = TimeStrikeServer::new().map_err(anyhow::Error::msg)?;
    server.serve(stdio()).await?.waiting().await?;
    Ok(())
}
