//! # Scheduler Module
//! TBD

use std::{
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant},
};

use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio_cron_scheduler::{JobScheduler, JobSchedulerError};
use uuid::Uuid;

use crate::{app::Hooks, environment::Environment, task::Tasks};

static RE_IS_CRON_SYNTAX: OnceLock<Regex> = OnceLock::new();

fn get_re_is_cron_syntax() -> &'static Regex {
    RE_IS_CRON_SYNTAX.get_or_init(|| Regex::new(r"^[\*\d]").unwrap())
}

/// Errors that may occur while operating the scheduler.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("schedulers not configured")]
    Empty,

    #[error("task `{0}` not found")]
    TaskNotFound(String),

    #[error("Scheduler config file not found in path: '{}'", path.display())]
    ConfigNotFound { path: PathBuf, error: io::Error },

    #[error("Invalid scheduler config schema. err: '{}'", error.as_display())]
    InvalidConfigSchema { error: serde_yaml::Error },

    #[error("Invalid cron {cron}. err: '{}'", error.as_display())]
    InvalidCronSyntax { cron: String, error: String },

    #[error(transparent)]
    Question(#[from] JobSchedulerError),

    #[error(transparent)]
    IO(#[from] std::io::Error),
}

/// Result type used in the module, with a custom error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration structure for the scheduler.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// A list of jobs to be scheduled.
    pub jobs: HashMap<String, Job>,
    /// The default output setting for the jobs.
    #[serde(default)]
    pub output: Output,
}

/// Representing a single job in the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// The command to run.
    /// In case of task: it should be a task name and also task arguments
    pub run: String,
    #[serde(default)]
    pub shell: bool,
    #[serde(default)]
    pub run_on_start: bool,
    #[serde(rename = "schedule")]
    /// The cron expression defining the job's schedule.
    ///
    /// The format is as follows:
    /// sec   min   hour   day of month   month   day of week   year
    /// * *     *      *              *       *             *
    pub cron: String,
    /// Tags for tagging the job.
    pub tags: Option<Vec<String>>,
    /// Output settings for the job.
    pub output: Option<Output>,
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "#      job_name       run_on_start      schedule               tags               run"
        )?;

        let mut job_names: Vec<&String> = self.jobs.keys().collect();
        job_names.sort();

        for (index, &job_name) in job_names.iter().enumerate() {
            if let Some(job) = self.jobs.get(job_name) {
                writeln!(
                    f,
                    "{:<6} {:<15} {:<12} {:<22} {:<18} {:?}",
                    index + 1,
                    job_name,
                    job.run_on_start,
                    job.cron,
                    job.tags
                        .as_ref()
                        .map_or_else(|| "-".to_string(), |tags| tags.join(", ")),
                    job.run,
                )?;
            }
        }

        Ok(())
    }
}

/// Representing the scheduler itself.
#[derive(Clone, Debug)]
pub struct Scheduler {
    pub jobs: HashMap<String, Job>,
    binary_path: PathBuf,
    default_output: Output,
    environment: Environment,
}

/// Specification used to filter all scheduler job with the given Spec.
#[derive(Debug)]
pub struct Spec {
    pub name: Option<String>,
    pub tag: Option<String>,
}

/// Enum representing the scheduler job output.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub enum Output {
    /// Silent output, the STDOUT or STDERR of the job will not view out.
    #[serde(rename = "silent")]
    Silent,
    /// The STDOUT or STDERR of the job will view propagated
    #[default]
    #[serde(rename = "stdout")]
    STDOUT,
}

/// Structure representing the job command.
#[derive(Clone, Debug)]
pub struct JobDescription {
    /// The command to execute.
    pub command: String,
    /// The output setting for the job.
    pub output: Output,
    /// The environment in which the job will run.
    pub environment: Environment,
}

impl Job {
    /// Prepares a command description based on the job's configuration.
    ///
    /// [`JobDescription::run`] executes the description through a shell. The
    /// scheduler prepares a separate process to execute native tasks directly.
    #[must_use]
    pub fn prepare_command(
        &self,
        binary_path: &Path,
        default_output: &Output,
        environment: &Environment,
    ) -> JobDescription {
        let command = if self.shell {
            self.run.clone()
        } else {
            [
                binary_path.display().to_string(),
                "task".to_string(),
                self.run.clone(),
            ]
            .join(" ")
        };

        JobDescription {
            command,
            output: self
                .output
                .clone()
                .unwrap_or_else(|| default_output.clone()),
            environment: environment.clone(),
        }
    }

    fn task_arguments(&self) -> io::Result<Vec<String>> {
        shlex::split(&self.run)
            .filter(|arguments| {
                arguments.first().is_some_and(|name| !name.is_empty())
                    && arguments.iter().all(|argument| !argument.contains('\0'))
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native scheduler task requires a task name, balanced quotes and no NUL bytes",
                )
            })
    }

    fn prepare_process(&self, binary_path: &Path) -> io::Result<duct::Expression> {
        if self.shell {
            Ok(shell_process(&self.run))
        } else {
            Ok(duct::cmd(
                binary_path,
                std::iter::once("task".to_string()).chain(self.task_arguments()?),
            ))
        }
    }
}

impl JobDescription {
    /// Executes this command string through the platform shell.
    ///
    /// Native scheduler jobs use a separate direct process path.
    ///
    /// # Errors
    ///
    /// In addition to all the IO errors possible
    pub fn run(&self) -> io::Result<std::process::Output> {
        tracing::info!(command = &self.command, "execute job command");
        self.run_process(&shell_process(&self.command))
    }

    fn run_process(&self, process: &duct::Expression) -> io::Result<std::process::Output> {
        let mut exec_job = process.env("LOCO_ENV", self.environment.to_string());
        exec_job = match self.output {
            Output::Silent => exec_job.stdout_null().stderr_null(),
            Output::STDOUT => exec_job,
        };

        exec_job.run()
    }
}

fn shell_process(command: &str) -> duct::Expression {
    // Run the command through the platform shell (previously `duct_sh`).
    if cfg!(windows) {
        duct::cmd!("cmd.exe", "/C", command)
    } else {
        duct::cmd!("/bin/sh", "-c", command)
    }
}

impl Scheduler {
    /// Creates a new scheduler instance from the given configuration file.
    ///
    /// # Errors
    ///
    /// When could not parse the given file content into a [`Config`] struct.
    pub fn from_config<H: Hooks>(config: &Path, environment: &Environment) -> Result<Self> {
        let config_str =
            std::fs::read_to_string(config).map_err(|error| Error::ConfigNotFound {
                path: config.to_path_buf(),
                error,
            })?;

        let config: Config = serde_yaml::from_str(&config_str)
            .map_err(|error| Error::InvalidConfigSchema { error })?;

        Self::new::<H>(&config, environment)
    }

    /// Creates a new scheduler instance from the provided configuration data.
    ///
    /// When creating a new scheduler instance all register task should be
    /// loaded for validate the given configuration.
    ///
    /// # Errors
    ///
    /// When there is not job in the given config
    pub fn new<H: Hooks>(data: &Config, environment: &Environment) -> Result<Self> {
        let mut tasks = Tasks::default();
        H::register_tasks(&mut tasks);

        let mut jobs = HashMap::new();
        for (job_name, job) in &data.jobs {
            if job.shell {
                jobs.insert(job_name.clone(), job.clone());
            } else {
                let arguments = job.task_arguments()?;
                let task_name = arguments.first().map_or("", String::as_str);
                if tasks.names().iter().any(|name| name.as_str() == task_name) {
                    jobs.insert(job_name.clone(), job.clone());
                } else {
                    return Err(Error::TaskNotFound(task_name.to_string()));
                }
            }
        }

        if jobs.is_empty() {
            return Err(Error::Empty);
        }

        Ok(Self {
            jobs,
            binary_path: std::env::current_exe()?,
            default_output: data.output.clone(),
            environment: environment.clone(),
        })
    }

    /// Filters the scheduler's jobs based on the provided specification.
    #[must_use]
    pub fn by_spec(self, include_jobs: &Spec) -> Self {
        let jobs = self
            .jobs
            .into_iter()
            .filter(|(job_name, job)| {
                if let Some(name) = &include_jobs.name {
                    return name == job_name;
                }

                if let Some(tag) = &include_jobs.tag
                    && let Some(job_tags) = &job.tags
                {
                    return job_tags.contains(tag);
                }

                true
            })
            .collect::<HashMap<String, Job>>();

        Self { jobs, ..self }
    }

    /// Runs the scheduled jobs according to their cron expressions.
    ///
    /// # Errors
    ///
    /// When could not add job to the scheduler
    pub async fn run(self) -> Result<()> {
        let mut sched = JobScheduler::new().await?;

        for (job_name, job) in &self.jobs {
            let job_description =
                job.prepare_command(&self.binary_path, &self.default_output, &self.environment);
            let process = job.prepare_process(&self.binary_path)?.unchecked();

            let cron_syntax = if get_re_is_cron_syntax().is_match(&job.cron) {
                job.cron.clone()
            } else {
                english_to_cron::str_cron_syntax(&job.cron).map_err(|err| {
                    Error::InvalidCronSyntax {
                        cron: job.cron.clone(),
                        error: err.to_string(),
                    }
                })?
            };

            if job.run_on_start {
                let job_description = job_description.clone();
                let process = process.clone();
                let job_name = job_name.clone();
                sched
                    .add(tokio_cron_scheduler::Job::new_one_shot_async(
                        Duration::from_secs(0),
                        move |uuid, _l| {
                            let job_description = job_description.clone();
                            let process = process.clone();
                            let job_name = job_name.clone();
                            Box::pin(async move {
                                // `job_description.run()` blocks the thread for the
                                // whole child-process lifetime; keep it off the
                                // async runtime's worker threads.
                                let _ = tokio::task::spawn_blocking(move || {
                                    execute_job(
                                        job_name.as_str(),
                                        uuid,
                                        &job_description,
                                        &process,
                                    );
                                })
                                .await;
                            })
                        },
                    )?)
                    .await?;
            }

            let job_name = job_name.clone();
            sched
                .add(tokio_cron_scheduler::Job::new_async(
                    cron_syntax.as_str(),
                    move |uuid, mut _l| {
                        let job_description = job_description.clone();
                        let process = process.clone();
                        let job_name = job_name.clone();
                        Box::pin(async move {
                            // `job_description.run()` blocks the thread for the whole
                            // child-process lifetime; keep it off the async runtime's
                            // worker threads.
                            let _ = tokio::task::spawn_blocking(move || {
                                execute_job(job_name.as_str(), uuid, &job_description, &process);
                            })
                            .await;
                        })
                    },
                )?)
                .await?;
        }

        sched.start().await?;

        tokio::signal::ctrl_c().await?;
        sched.shutdown().await?;

        Ok(())
    }
}

fn execute_job(
    job_name: &str,
    uuid: Uuid,
    job_description: &JobDescription,
    process: &duct::Expression,
) {
    let task_span = tracing::span!(
        tracing::Level::DEBUG,
        "run_job",
        job_name,
        job_id = ?uuid,
    );
    let start = Instant::now();
    let _guard = task_span.enter();
    match job_description.run_process(process) {
        Ok(output) if !output.status.success() => {
            tracing::error!(
                duration = ?start.elapsed(),
                status_code = output.status.code(),
                "failed to execute scheduler job in sub process"
            );
        }
        Ok(output) => {
            tracing::debug!(
                duration = ?start.elapsed(),
                status_code = output.status.code(),
                "execute scheduler job finished"
            );
        }
        Err(err) => {
            tracing::error!(
                duration = ?start.elapsed(),
                error = ?err.kind(),
                "failed to execute scheduler job in sub process"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use rstest::rstest;
    use tests_cfg::db::AppHook;
    use tokio::time::{self, Duration};
    use tree_fs::TreeBuilder;

    use super::*;
    use crate::tests_cfg;

    fn setup_scheduler_config() -> (Scheduler, tree_fs::Tree) {
        let tree = TreeBuilder::default()
            .add_file(
                "scheduler.yaml",
                r#"
jobs:
  print_task:
    run: foo
    schedule: "*/5 * * * * *"
    tags:
      - base
      - echo

  write_to_file:
    run: "echo loco >> ./scheduler.txt"
    shell: true
    schedule: "*/5 * * * * *"
    tags:
      - base
      - write

  run_on_start_task:
    run: "echo \"Does this run on start?\" >> ./run_on_start.txt "
    shell: true
    schedule: "every 24 hours"
    run_on_start: true
    tags:
      - start
"#,
            )
            .create()
            .expect("Failed to create test directory structure");

        let scheduler = Scheduler::from_config::<AppHook>(
            &tree.root.join("scheduler.yaml"),
            &Environment::Development,
        )
        .expect("Failed to create scheduler from config");

        (scheduler, tree)
    }

    #[test]
    pub fn can_display_scheduler() {
        let (scheduler, _tree) = setup_scheduler_config();
        assert_debug_snapshot!(format!("{scheduler}"));
    }

    #[test]
    pub fn can_load_from_config_local_config() {
        // Succeeds as long as loading the config doesn't panic or error.
        let (_, _tree) = setup_scheduler_config();
    }

    #[tokio::test]
    pub async fn can_load_from_env_config() {
        let app_context = tests_cfg::app::get_app_context().await;
        let scheduler = Scheduler::new::<AppHook>(
            &app_context.config.scheduler.unwrap(),
            &Environment::Development,
        );

        assert!(scheduler.is_ok());
    }

    #[test]
    pub fn can_load_jobs_by_spec_tag_multiple_jobs() {
        let (scheduler, _tree) = setup_scheduler_config();
        let scheduler = scheduler.by_spec(&Spec {
            name: None,
            tag: Some("base".to_string()),
        });

        assert_eq!(scheduler.jobs.len(), 2);
    }

    #[test]
    pub fn can_load_jobs_by_spec_tag_single_jobs() {
        let (scheduler, _tree) = setup_scheduler_config();
        let scheduler = scheduler.by_spec(&Spec {
            name: None,
            tag: Some("echo".to_string()),
        });

        assert_eq!(scheduler.jobs.len(), 1);
        assert!(scheduler.jobs.contains_key("print_task"));
    }

    #[test]
    pub fn can_load_jobs_by_spec_with_job_name() {
        let (scheduler, _tree) = setup_scheduler_config();
        let scheduler = scheduler.by_spec(&Spec {
            name: Some("write_to_file".to_string()),
            tag: None,
        });

        assert_eq!(scheduler.jobs.len(), 1);
        assert!(scheduler.jobs.contains_key("write_to_file"));
    }

    #[rstest]
    #[case("shell", "echo loco", true)]
    #[case("task", "foo LOCO_ENV:test SCHEDULER:true", false)]
    pub fn can_prepare_command(#[case] test_name: &str, #[case] run: &str, #[case] shell: bool) {
        let job = Job {
            run: run.to_string(),
            shell,
            run_on_start: false,
            cron: "*/5 * * * * *".to_string(),
            tags: None,
            output: None,
        };

        let prepare_command = job.prepare_command(
            PathBuf::from("[BIN_PATH]").as_path(),
            &Output::STDOUT,
            &Environment::Test,
        );
        assert_debug_snapshot!(
            format!("can_prepare_command_[{test_name}]"),
            prepare_command
        );
    }

    #[cfg(unix)]
    #[test]
    fn native_task_preserves_arguments_on_start_and_later_ticks() {
        let tree = TreeBuilder::default()
            .drop(true)
            .add_file(
                "probe.rs",
                include_str!("../tests/fixtures/scheduler_probe.rs"),
            )
            .create()
            .unwrap();
        let executable = tree.root.join("application with spaces");
        let compiled = std::process::Command::new("rustc")
            .arg(tree.root.join("probe.rs"))
            .arg("-o")
            .arg(&executable)
            .output()
            .unwrap();
        assert!(
            compiled.status.success(),
            "{}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        let output = tree.root.join("native results");
        let periodic_output = tree.root.join("periodic results");
        let sentinel = tree.root.join("shell-sentinel");
        let (mut scheduler, _config_tree) = setup_scheduler_config();
        scheduler.jobs.retain(|name, _| name == "print_task");
        let job = scheduler.jobs.get_mut("print_task").unwrap();
        job.run = format!(
            "foo '{}' 'two words' '' '$LOCO_ENV' ; touch '{}'",
            output.display(),
            sentinel.display()
        );
        job.run_on_start = true;
        job.cron = "0 0 0 1 1 * 2099".to_string();
        let mut periodic = job.clone();
        periodic.run = format!(
            "foo '{}' 'two words' '' '$LOCO_ENV' ; touch '{}'",
            periodic_output.display(),
            sentinel.display()
        );
        periodic.run_on_start = false;
        periodic.cron = "* * * * * *".to_string();
        scheduler.jobs.insert("periodic".to_string(), periodic);
        scheduler.binary_path = executable;
        scheduler.environment = Environment::Test;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let completed = runtime.block_on(async {
            let handle = tokio::spawn(async move { scheduler.run().await });
            let completed = time::timeout(Duration::from_secs(5), async {
                loop {
                    if [&output, &periodic_output].iter().all(|path| {
                        std::fs::read_to_string(path).is_ok_and(|contents| !contents.is_empty())
                    }) {
                        break;
                    }
                    time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            handle.abort();
            let _ = handle.await;
            completed
        });
        drop(runtime);
        assert!(
            completed.is_ok(),
            "native scheduler did not invoke its executable on startup and a later tick"
        );
        let arguments = [
            "two words",
            "",
            "$LOCO_ENV",
            ";",
            "touch",
            sentinel.to_str().unwrap(),
        ];
        let expected = format!("{arguments:?}|test|true");
        for report in [output, periodic_output] {
            let contents = std::fs::read_to_string(report).unwrap();
            assert!(contents.lines().all(|line| line == expected));
        }
        assert!(
            !sentinel.exists(),
            "native arguments must not execute shell syntax"
        );
    }

    #[test]
    fn native_job_validation_uses_quoted_arguments() {
        let (scheduler, _tree) = setup_scheduler_config();
        let mut config = Config {
            jobs: scheduler.jobs,
            output: Output::STDOUT,
        };
        config.jobs.retain(|name, _| name == "print_task");
        config.jobs.get_mut("print_task").unwrap().run = "'foo' 'value:two words'".to_string();
        assert!(Scheduler::new::<AppHook>(&config, &Environment::Test).is_ok());
        config.jobs.get_mut("print_task").unwrap().run = "foo 'unclosed".to_string();
        assert!(matches!(
            Scheduler::new::<AppHook>(&config, &Environment::Test),
            Err(Error::IO(error)) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    #[cfg(unix)]
    #[test]
    fn shell_jobs_retain_output_modes_and_unsuccessful_status() {
        for output in [Output::STDOUT, Output::Silent] {
            let job = Job {
                run: "printf stdout; printf stderr >&2; exit 37".to_string(),
                shell: true,
                run_on_start: false,
                cron: "* * * * * *".to_string(),
                tags: None,
                output: Some(output.clone()),
            };
            let description = job.prepare_command(Path::new("unused"), &output, &Environment::Test);
            let process = job
                .prepare_process(Path::new("unused"))
                .unwrap()
                .unchecked();
            let process = if matches!(output, Output::STDOUT) {
                process.stdout_capture().stderr_capture()
            } else {
                process
            };
            let result = description.run_process(&process).unwrap();
            assert_eq!(result.status.code(), Some(37));
            if matches!(output, Output::Silent) {
                assert!(result.stdout.is_empty());
                assert!(result.stderr.is_empty());
            } else {
                assert_eq!(result.stdout, b"stdout");
                assert_eq!(result.stderr, b"stderr");
            }
        }
    }

    #[test]
    fn invalid_native_arguments_fail_before_process_creation() {
        let (scheduler, _tree) = setup_scheduler_config();
        let mut job = scheduler.jobs["print_task"].clone();
        for run in [
            "",
            " ",
            "''",
            "'",
            "foo 'unclosed",
            "foo\0",
            "foo value:bad\0argument",
        ] {
            job.run = run.to_string();
            assert_eq!(
                job.prepare_process(Path::new("unused")).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            let config = Config {
                jobs: [("invalid".to_string(), job.clone())].into(),
                output: Output::STDOUT,
            };
            assert!(Scheduler::new::<AppHook>(&config, &Environment::Test).is_err());
        }
    }

    #[cfg(unix)]
    fn compile_native_probe() -> (tree_fs::Tree, PathBuf) {
        let tree = TreeBuilder::default()
            .drop(true)
            .add_file(
                "probe.rs",
                include_str!("../tests/fixtures/scheduler_probe.rs"),
            )
            .create()
            .unwrap();
        let binary = tree.root.join("application with spaces");
        let compiled = std::process::Command::new("rustc")
            .arg(tree.root.join("probe.rs"))
            .arg("-o")
            .arg(&binary)
            .output()
            .unwrap();
        assert!(
            compiled.status.success(),
            "{}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        (tree, binary)
    }

    #[cfg(unix)]
    #[test]
    fn native_process_preserves_output_modes_and_exit_status() {
        let (tree, binary) = compile_native_probe();
        let (scheduler, _config_tree) = setup_scheduler_config();
        let mut job = scheduler.jobs["print_task"].clone();
        for output in [Output::STDOUT, Output::Silent] {
            job.run = format!("foo '{}'", tree.root.join("results").display());
            let description = job.prepare_command(&binary, &output, &Environment::Test);
            let process = job.prepare_process(&binary).unwrap().unchecked();
            let process = if matches!(output, Output::STDOUT) {
                process.stdout_capture().stderr_capture()
            } else {
                process
            };
            let result = description.run_process(&process).unwrap();
            assert!(result.status.success());
            if matches!(output, Output::Silent) {
                assert!(result.stdout.is_empty());
                assert!(result.stderr.is_empty());
            } else {
                assert_eq!(result.stdout, b"synthetic scheduler stdout\n");
                assert_eq!(result.stderr, b"synthetic scheduler stderr\n");
            }
        }
        job.run = "fail synthetic-secret:do-not-log".to_string();
        let description = job.prepare_command(&binary, &Output::Silent, &Environment::Test);
        let process = job.prepare_process(&binary).unwrap().unchecked();
        assert_eq!(
            description.run_process(&process).unwrap().status.code(),
            Some(37)
        );
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_failure_logs_omit_native_arguments() {
        let (tree, binary) = compile_native_probe();
        let log_path = tree.root.join("scheduler.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || log.try_clone().unwrap())
            .finish();
        let (scheduler, _config_tree) = setup_scheduler_config();
        let mut job = scheduler.jobs["print_task"].clone();
        job.run = "fail synthetic-secret:do-not-log".to_string();
        let description = job.prepare_command(&binary, &Output::Silent, &Environment::Test);
        tracing::subscriber::with_default(subscriber, || {
            let process = job.prepare_process(&binary).unwrap().unchecked();
            execute_job("failure", Uuid::new_v4(), &description, &process);
            let process = job
                .prepare_process(&tree.root.join("missing"))
                .unwrap()
                .unchecked();
            assert_eq!(
                description.run_process(&process).unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
            execute_job("missing", Uuid::new_v4(), &description, &process);
        });
        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(contents.contains("status_code=37"), "{contents}");
        assert!(contents.contains("NotFound"), "{contents}");
        assert!(!contents.contains("synthetic-secret"), "{contents}");
    }

    #[tokio::test]
    pub async fn can_run() {
        let (mut scheduler, _config_tree) = setup_scheduler_config();

        let tree_fs = tree_fs::TreeBuilder::default()
            .drop(true)
            .add("scheduler.txt", "")
            .add("scheduler2.txt", "")
            .add("scheduler3.txt", "")
            .create()
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(tree_fs.root.join("scheduler.txt"))
                .unwrap()
                .lines()
                .count(),
            0
        );
        assert_eq!(
            std::fs::read_to_string(tree_fs.root.join("scheduler2.txt"))
                .unwrap()
                .lines()
                .count(),
            0
        );
        assert_eq!(
            std::fs::read_to_string(tree_fs.root.join("scheduler3.txt"))
                .unwrap()
                .lines()
                .count(),
            0
        );

        scheduler.jobs = HashMap::from([
            (
                "test".to_string(),
                Job {
                    run: format!(
                        "echo loco >> {}",
                        tree_fs.root.join("scheduler.txt").display()
                    ),
                    shell: true,
                    run_on_start: false,
                    cron: "run every 1 second".to_string(),
                    tags: None,
                    output: None,
                },
            ),
            (
                "test_2".to_string(),
                Job {
                    run: format!(
                        "echo loco >> {}",
                        tree_fs.root.join("scheduler2.txt").display()
                    ),
                    shell: true,
                    run_on_start: false,
                    cron: "* * * * * ? *".to_string(),
                    tags: None,
                    output: None,
                },
            ),
            (
                "test_3".to_string(),
                Job {
                    run: format!(
                        "echo loco >> {}",
                        tree_fs.root.join("scheduler3.txt").display()
                    ),
                    shell: true,
                    run_on_start: true,
                    cron: "0 0 * * * * *".to_string(),
                    tags: None,
                    output: None,
                },
            ),
        ]);

        let handle = tokio::spawn(async move {
            scheduler.run().await.unwrap();
        });

        time::sleep(Duration::from_secs(5)).await;
        handle.abort();

        assert!(
            std::fs::read_to_string(tree_fs.root.join("scheduler.txt"))
                .unwrap()
                .lines()
                .count()
                >= 4
        );
        assert!(
            std::fs::read_to_string(tree_fs.root.join("scheduler2.txt"))
                .unwrap()
                .lines()
                .count()
                >= 4
        );
        assert_eq!(
            std::fs::read_to_string(tree_fs.root.join("scheduler3.txt"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
}
