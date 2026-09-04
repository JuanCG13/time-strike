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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time_strike::clock::{Clock, MonotonicClock};
use time_strike::enforcement::ActionLeaseGrant;
use time_strike::policy::{PolicyDecision, PolicyInput, ScheduleStatus, evaluate_time_policy};
use time_strike::{
    AdjustTaskRequest, CheckpointRequest, FileStore, FinishTaskRequest, StartTaskRequest,
    TaskManager, TaskTiming, TaskView, TickRequest,
};

const MIN_PLAN_STEPS: usize = 2;
const MAX_PLAN_STEPS: usize = 8;
const MAX_PLAN_FIELD_CHARS: usize = 160;
const MAX_ACTION_CHARS: usize = 160;

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
struct PlanStepInput {
    /// Concrete action to perform.
    action: String,
    /// Bounded estimate for this step, in seconds.
    estimated_seconds: f64,
    /// Observable condition that proves the step is complete.
    done_when: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CheckpointInput {
    task_id: Option<String>,
    progress_percent: Option<f64>,
    estimated_remaining_work_seconds: Option<f64>,
    #[serde(default)]
    completed: Vec<String>,
    note: Option<String>,
    /// Two to eight auditable steps for an initial or replacement plan.
    #[serde(default)]
    plan_steps: Vec<PlanStepInput>,
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
    /// Compatibility field. MCP callers cannot exercise this host-only core
    /// privilege.
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
    deadline_authority: String,
    planning_budget_seconds: f64,
    directive: String,
    max_new_action_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct TickOutput {
    remaining_seconds: f64,
    actual_elapsed_seconds: f64,
    accounted_elapsed_seconds: f64,
    /// Backward-compatible alias for `accounted_elapsed_seconds`.
    elapsed_seconds: f64,
    overrun_seconds: f64,
    deadline_met: bool,
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
    action_lease_ceiling_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_fits: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_lease: Option<ActionLeaseGrant>,
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
    plan_step_count: usize,
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
    host_deadline: Option<Duration>,
    #[allow(dead_code)] // read by rmcp-generated routing code
    tool_router: ToolRouter<Self>,
}

impl TimeStrikeServer {
    fn new() -> Result<Self, String> {
        let config = load_config()?;
        let clock = MonotonicClock::new();
        // Capture the monotonic anchor before reading wall time. Any delay in
        // conversion therefore shortens, rather than extends, the host budget.
        let monotonic_anchor = clock.now();
        let host_deadline =
            monotonic_host_deadline(parse_host_deadline()?, current_unix_ms(), monotonic_anchor);
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
            TaskManager::with_store(clock, Arc::new(store)).map_err(|error| error.to_string())?
        } else {
            TaskManager::new(clock)
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
            host_deadline,
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
        timing: &TaskTiming,
        decision: PolicyDecision,
        tick: u64,
        current_action: Option<&str>,
        action_seconds: Option<f64>,
        verbose: bool,
    ) -> TickOutput {
        let plan_required = !view.plan_submitted;
        let effective_max_action = if plan_required {
            0.0
        } else {
            decision.max_new_action_secs
        };
        let action_lease_ceiling = effective_max_action.min(decision.next_check_secs);
        let action_lease = issue_action_lease(
            &view.task_id,
            tick,
            current_action,
            action_seconds,
            action_lease_ceiling,
        );
        let action_fits = action_seconds.map(|seconds| {
            if current_action.is_some() {
                action_lease.is_some()
            } else {
                seconds <= effective_max_action
            }
        });
        TickOutput {
            remaining_seconds: round3(decision.remaining_secs),
            actual_elapsed_seconds: round3(timing.actual_elapsed_secs),
            accounted_elapsed_seconds: round3(timing.accounted_elapsed_secs),
            elapsed_seconds: round3(view.elapsed_secs),
            overrun_seconds: round3(timing.overrun_secs),
            deadline_met: timing.deadline_met,
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
            action_lease_ceiling_seconds: round3(action_lease_ceiling),
            action_fits,
            action_lease,
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
            .start_task_with_deadline(request, self.host_deadline)
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
            deadline_authority: if self.host_deadline.is_some() {
                "host_absolute"
            } else {
                "agent_relative"
            }
            .into(),
            planning_budget_seconds: round3(planning_budget_secs(outcome.task.budget_secs)),
            directive: "submit_plan".into(),
            max_new_action_seconds: 0.0,
            reason: (input.verbose || self.default_verbose).then(|| decision.reason.to_string()),
        }))
    }

    #[tool(
        description = "Return the mandatory next directive and deadline pressure. Supplying current_action plus its ETA can grant a bounded action_lease for host enforcement."
    )]
    async fn tick(
        &self,
        Parameters(input): Parameters<TickInput>,
    ) -> Result<Json<TickOutput>, String> {
        validate_action_proposal(
            input.current_action.as_deref(),
            input.current_action_estimated_seconds,
        )?;
        let task_id = self.resolve_task_id(input.task_id)?;
        let (outcome, timing) = self
            .manager
            .tick_with_timing(TickRequest::new(task_id))
            .map_err(|error| error.to_string())?;
        let decision = Self::policy(
            &outcome.task,
            input.progress_percent,
            input.estimated_remaining_work_seconds,
        );
        let tick = outcome.tick;
        Ok(Json(Self::tick_output(
            &outcome.task,
            &timing,
            decision,
            tick,
            input.current_action.as_deref(),
            input.current_action_estimated_seconds,
            input.verbose || self.default_verbose,
        )))
    }

    #[tool(
        description = "Submit the initial plan or record progress and ETA. Prefer two to eight plan_steps with action, estimated_seconds, and done_when; legacy note plans remain accepted. The first checkpoint must use plan_complete=true."
    )]
    async fn checkpoint(
        &self,
        Parameters(input): Parameters<CheckpointInput>,
    ) -> Result<Json<CheckpointOutput>, String> {
        let task_id = self.resolve_task_id(input.task_id)?;
        let structured_plan = structured_plan(&input.plan_steps, input.plan_complete)?;
        let plan_step_count = input.plan_steps.len();
        let estimated_remaining_work_seconds = structured_plan
            .as_ref()
            .map(|plan| plan.estimated_seconds)
            .or(input.estimated_remaining_work_seconds);
        let note = structured_plan
            .map(|plan| plan.note)
            .or(input.note)
            .or_else(|| {
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
                estimated_remaining_work_secs: estimated_remaining_work_seconds,
                plan_complete: input.plan_complete,
                replan: input.replan,
            })
            .map_err(|error| error.to_string())?;
        let decision = Self::policy(
            &outcome.task,
            input.progress_percent,
            estimated_remaining_work_seconds,
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
            plan_step_count,
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
            .adjust_task_with_deadline(
                AdjustTaskRequest::new(&task_id).with_budget(new_total),
                self.host_deadline,
            )
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
        if input.force {
            return Err(
                "force is a host-only core privilege and cannot be requested over MCP".into(),
            );
        }
        let task_id = self.resolve_task_id(input.task_id)?;
        let outcome = self
            .manager
            .finish_task(FinishTaskRequest {
                task_id: task_id.clone(),
                reason: input.reason,
                force: false,
            })
            .map_err(|error| error.to_string())?;
        let deadline_met = outcome.deadline_met();
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
            deadline_met,
        }))
    }
}

#[tool_handler(
    name = "time-strike",
    version = "0.2.3",
    instructions = "Immediately call start_task for deadline work. If directive=submit_plan, call checkpoint before costly work with plan_complete=true and 2-8 plan_steps; each needs action, estimated_seconds, and done_when. Before costly work call tick with current_action and current_action_estimated_seconds, then proceed only with the returned action_lease and its relative expiry. Call tick after searches, edits, tests, delegation, and tool calls. On converge_required_only stop exploration and perform required work only. On validate only verify. On finalize deliver. On stop return immediately. Never increase the budget."
)]
impl ServerHandler for TimeStrikeServer {}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn planning_budget_secs(total_secs: f64) -> f64 {
    (total_secs * 0.05).clamp(2.0, 60.0).min(total_secs * 0.15)
}

fn validate_action_proposal(
    action: Option<&str>,
    estimated_seconds: Option<f64>,
) -> Result<(), String> {
    if let Some(action) = action {
        let action = action.trim();
        if action.is_empty() {
            return Err("current_action must not be empty".into());
        }
        if action.chars().count() > MAX_ACTION_CHARS {
            return Err(format!(
                "current_action must be at most {MAX_ACTION_CHARS} characters"
            ));
        }
    }
    if let Some(seconds) = estimated_seconds
        && (!seconds.is_finite() || seconds <= 0.0)
    {
        return Err("current_action_estimated_seconds must be finite and > 0".into());
    }
    Ok(())
}

fn issue_action_lease(
    task_id: &str,
    tick: u64,
    action: Option<&str>,
    estimated_seconds: Option<f64>,
    lease_ceiling_seconds: f64,
) -> Option<ActionLeaseGrant> {
    let action = action?.trim();
    let duration_seconds = estimated_seconds?;
    // Never serialize a rounded expiry that is later than the policy ceiling.
    let expires_in_seconds = (lease_ceiling_seconds.max(0.0) * 1000.0).floor() / 1000.0;
    (duration_seconds <= expires_in_seconds).then(|| ActionLeaseGrant {
        lease_id: format!("{task_id}:{tick}"),
        task_id: task_id.to_owned(),
        action: action.to_owned(),
        duration_seconds,
        expires_in_seconds,
        expiry_anchor: "tick_request_started".into(),
        one_shot: true,
    })
}

struct StructuredPlan {
    note: String,
    estimated_seconds: f64,
}

fn structured_plan(
    steps: &[PlanStepInput],
    plan_complete: bool,
) -> Result<Option<StructuredPlan>, String> {
    if steps.is_empty() {
        return Ok(None);
    }
    if !plan_complete {
        return Err("plan_steps requires plan_complete=true".into());
    }
    if !(MIN_PLAN_STEPS..=MAX_PLAN_STEPS).contains(&steps.len()) {
        return Err(format!(
            "plan_steps must contain between {MIN_PLAN_STEPS} and {MAX_PLAN_STEPS} steps"
        ));
    }

    let mut estimated_seconds = 0.0;
    let mut rendered = Vec::with_capacity(steps.len());
    for (index, step) in steps.iter().enumerate() {
        let action = step.action.trim();
        let done_when = step.done_when.trim();
        if action.is_empty() || done_when.is_empty() {
            return Err(format!(
                "plan_steps[{}] requires non-empty action and done_when",
                index
            ));
        }
        if action.chars().count() > MAX_PLAN_FIELD_CHARS
            || done_when.chars().count() > MAX_PLAN_FIELD_CHARS
        {
            return Err(format!(
                "plan_steps[{}] action and done_when must each be at most {MAX_PLAN_FIELD_CHARS} characters",
                index
            ));
        }
        if !step.estimated_seconds.is_finite() || step.estimated_seconds <= 0.0 {
            return Err(format!(
                "plan_steps[{}].estimated_seconds must be finite and > 0",
                index
            ));
        }
        estimated_seconds += step.estimated_seconds;
        if !estimated_seconds.is_finite() {
            return Err("plan_steps estimated_seconds total is not finite".into());
        }
        rendered.push(format!(
            "{}. {} [{}s; done: {}]",
            index + 1,
            action,
            round3(step.estimated_seconds),
            done_when
        ));
    }

    Ok(Some(StructuredPlan {
        note: rendered.join(" "),
        estimated_seconds,
    }))
}

fn monotonic_host_deadline(
    host_deadline_unix_ms: Option<u64>,
    now_unix_ms: u64,
    monotonic_anchor: Duration,
) -> Option<Duration> {
    host_deadline_unix_ms.map(|deadline| {
        monotonic_anchor.saturating_add(Duration::from_millis(deadline.saturating_sub(now_unix_ms)))
    })
}

fn parse_host_deadline() -> Result<Option<u64>, String> {
    let Some(value) = std::env::var_os("TIME_STRIKE_DEADLINE_UNIX_MS") else {
        return Ok(None);
    };
    value
        .to_string_lossy()
        .parse::<u64>()
        .map(Some)
        .map_err(|_| {
            "TIME_STRIKE_DEADLINE_UNIX_MS must be an unsigned Unix timestamp in milliseconds".into()
        })
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_deadline_is_converted_once_to_a_monotonic_instant() {
        let anchor = Duration::from_secs(5);
        let deadline = monotonic_host_deadline(Some(20_000), 10_000, anchor).unwrap();
        assert_eq!(deadline, Duration::from_secs(15));

        // A subsequent wall-clock rollback is irrelevant: only monotonic time
        // is used after initialization, so remaining time cannot increase.
        let wall_after_rollback = 1_000_u64;
        assert!(wall_after_rollback < 10_000);
        assert_eq!(
            deadline.saturating_sub(Duration::from_secs(6)),
            Duration::from_secs(9)
        );
        assert_eq!(
            deadline.saturating_sub(Duration::from_secs(7)),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn elapsed_wall_deadline_maps_to_the_monotonic_anchor() {
        let anchor = Duration::from_secs(5);
        assert_eq!(
            monotonic_host_deadline(Some(9_999), 10_000, anchor),
            Some(anchor)
        );
    }

    #[test]
    fn structured_plan_is_bounded_and_derives_eta() {
        let steps = vec![
            PlanStepInput {
                action: "Inspect the failing path".into(),
                estimated_seconds: 4.0,
                done_when: "the invariant is identified".into(),
            },
            PlanStepInput {
                action: "Apply and verify the minimal fix".into(),
                estimated_seconds: 6.5,
                done_when: "the regression test passes".into(),
            },
        ];
        let plan = structured_plan(&steps, true).unwrap().unwrap();
        assert_eq!(plan.estimated_seconds, 10.5);
        assert!(plan.note.contains("1. Inspect the failing path"));
        assert!(plan.note.contains("done: the regression test passes"));
    }

    #[test]
    fn structured_plan_rejects_non_auditable_shapes() {
        let one_step = vec![PlanStepInput {
            action: "Do everything".into(),
            estimated_seconds: 1.0,
            done_when: "done".into(),
        }];
        assert!(structured_plan(&one_step, true).is_err());

        let invalid_estimate = vec![
            PlanStepInput {
                action: "Inspect".into(),
                estimated_seconds: f64::NAN,
                done_when: "cause known".into(),
            },
            PlanStepInput {
                action: "Verify".into(),
                estimated_seconds: 1.0,
                done_when: "test passes".into(),
            },
        ];
        assert!(structured_plan(&invalid_estimate, true).is_err());
        assert!(structured_plan(&invalid_estimate, false).is_err());
    }

    #[test]
    fn action_leases_are_bounded_and_unique_to_the_tick() {
        let lease = issue_action_lease("task", 7, Some("Inspect"), Some(4.0), 5.0).unwrap();
        assert_eq!(lease.lease_id, "task:7");
        assert_eq!(lease.task_id, "task");
        assert_eq!(lease.action, "Inspect");
        assert_eq!(lease.duration_seconds, 4.0);
        assert_eq!(lease.expires_in_seconds, 5.0);
        assert_eq!(lease.expiry_anchor, "tick_request_started");
        assert!(lease.one_shot);
        assert!(issue_action_lease("task", 8, Some("Inspect"), Some(6.0), 5.0).is_none());
        assert!(issue_action_lease("task", 8, None, Some(1.0), 5.0).is_none());
        assert!(issue_action_lease("task", 8, Some("Inspect"), Some(1.0), 0.0009).is_none());
    }

    #[test]
    fn invalid_action_proposals_fail_closed() {
        assert!(validate_action_proposal(Some(""), Some(1.0)).is_err());
        assert!(validate_action_proposal(Some("Inspect"), Some(-1.0)).is_err());
        assert!(validate_action_proposal(Some("Inspect"), Some(f64::NAN)).is_err());
        assert!(validate_action_proposal(Some(&"a".repeat(161)), Some(1.0)).is_err());
        assert!(validate_action_proposal(Some("Inspect"), Some(1.0)).is_ok());
    }
}
