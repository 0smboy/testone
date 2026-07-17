use crate::runner::{self, RunOverrides};
use crate::task::FOOL_SUITE;
use anyhow::{bail, Result};
use tracing::info;

pub async fn run_suite(
    start_task: Option<&str>,
    backend: &str,
    cosbench_url: &str,
) -> Result<()> {
    let start = start_task.unwrap_or("64KB_read_1");
    let mut idx = FOOL_SUITE
        .iter()
        .position(|t| *t == start)
        .ok_or_else(|| anyhow::anyhow!("unknown fool task: {start}"))?;
    for job in &FOOL_SUITE[idx..] {
        println!("job is: {job}");
        run_one_with_defaults(job, backend, cosbench_url).await?;
        idx += 1;
        info!(done = idx, total = FOOL_SUITE.len(), "fool progress");
    }
    Ok(())
}

pub async fn rerun_one(task: &str, backend: &str, cosbench_url: &str) -> Result<()> {
    if !FOOL_SUITE.contains(&task) {
        bail!("unknown fool task: {task}");
    }
    run_one_with_defaults(task, backend, cosbench_url).await?;
    Ok(())
}

async fn run_one_with_defaults(task: &str, backend: &str, cosbench_url: &str) -> Result<()> {
    // mirror fool_job sizes
    let overrides = if task.starts_with("100MB") {
        RunOverrides {
            object_count: Some(500),
            prepare_worker: Some(100),
            ..Default::default()
        }
    } else if task.starts_with("10MB") {
        RunOverrides {
            object_count: Some(5000),
            prepare_worker: Some(300),
            ..Default::default()
        }
    } else {
        RunOverrides {
            object_count: Some(10000),
            prepare_worker: Some(1500),
            ..Default::default()
        }
    };
    // for mock backend shrink
    let overrides = if backend == "mock" {
        RunOverrides {
            object_count: Some(20),
            prepare_worker: Some(4),
            container_count: Some(1),
            runtime: Some(2),
        }
    } else {
        overrides
    };
    runner::run_task(task, overrides, backend, cosbench_url, true).await?;
    Ok(())
}
