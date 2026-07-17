use crate::env_cfg::{self, S3Creds};
use crate::progress::ProgressGuard;
use crate::task::{fool_defaults, parse_task, Method, TaskSpec};
use anyhow::{bail, Context, Result};
use chrono::Local;
use cosbench_core::config::{
    IdRange, ObjectSpec, Operation, Stage, StorageConfig, Workload,
};
use cosbench_core::Driver;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tracing::info;

#[derive(Debug, Default, Clone)]
pub struct RunOverrides {
    pub prepare_worker: Option<u32>,
    pub object_count: Option<u64>,
    pub container_count: Option<u64>,
    pub runtime: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub wid: String,
    pub task: TaskSpec,
    pub result_csv: PathBuf,
}

pub async fn run_task(
    name: &str,
    overrides: RunOverrides,
    backend: &str,
    cosbench_url: &str,
    is_fool: bool,
) -> Result<RunOutcome> {
    env_cfg::ensure_dirs()?;
    let task = parse_task(name)?;
    let creds = if backend == "mock" {
        env_cfg::S3Creds {
            access_key: "mock".into(),
            secret_key: "mock".into(),
            endpoint: "127.0.0.1:9".into(),
        }
    } else {
        let c = env_cfg::load_s3_creds()?;
        if let Err(e) = env_cfg::probe_s3(&c) {
            tracing::warn!("S3 probe failed (continuing): {e:#}");
        }
        c
    };

    let (def_objs, def_prep) = if is_fool {
        fool_defaults(&task.size_label)
    } else {
        (1000, 100)
    };
    let object_count = overrides.object_count.unwrap_or(def_objs);
    let prepare_worker = overrides.prepare_worker.unwrap_or(def_prep);
    let container_count = overrides.container_count.unwrap_or(10);
    let runtime = overrides.runtime.unwrap_or(150);

    info!(
        task = %task.name,
        workers = task.workers,
        size = task.size_bytes,
        object_count,
        prepare_worker,
        container_count,
        runtime,
        backend,
        "starting task"
    );

    let _pg = ProgressGuard::start(task.method.as_str());

    let outcome = match backend {
        "cosbench-rs" | "local" => {
            run_via_cosbench_rs(&task, &creds, object_count, prepare_worker, container_count, runtime)
                .await?
        }
        "mock" => {
            run_via_mock(&task, object_count, container_count, runtime).await?
        }
        "java" => {
            run_via_java_controller(
                &task,
                &creds,
                object_count,
                prepare_worker,
                container_count,
                runtime,
                cosbench_url,
            )
            .await?
        }
        other => bail!("unknown backend `{other}` (use cosbench-rs|java|mock)"),
    };

    // store under result or fool
    let dest_dir = if is_fool {
        env_cfg::cabt_home().join("fool")
    } else {
        env_cfg::cabt_home().join("result")
    };
    fs::create_dir_all(&dest_dir)?;
    let ts = Local::now().format("%Y-%m-%d-%H:%M:%S");
    let fname = format!(
        "{}-{}-{}-{}-{}.csv",
        outcome.wid,
        task.size_label,
        task.method.as_str(),
        task.workers,
        ts
    );
    let dest = dest_dir.join(&fname);
    fs::copy(&outcome.result_csv, &dest)
        .with_context(|| format!("copy result to {}", dest.display()))?;
    // invalidate collect cache
    let _ = fs::remove_file(dest_dir.join(".collect"));
    println!(
        "\x1b[32m  {} finished! wid={} result={}\x1b[0m",
        task.method.as_str(),
        outcome.wid,
        dest.display()
    );
    Ok(RunOutcome {
        wid: outcome.wid,
        task,
        result_csv: dest,
    })
}

struct InternalOutcome {
    wid: String,
    result_csv: PathBuf,
}

async fn run_via_cosbench_rs(
    task: &TaskSpec,
    creds: &S3Creds,
    object_count: u64,
    prepare_worker: u32,
    container_count: u64,
    runtime: u64,
) -> Result<InternalOutcome> {
    let cprefix = format!(
        "cabt{}",
        &format!("{:x}", md5_lite(&format!("{}{}", task.name, Local::now())))[..8]
    );
    let endpoint = env_cfg::endpoint_url(&creds.endpoint);
    let storage = StorageConfig::S3 {
        endpoint: Some(endpoint),
        region: "us-east-1".into(),
        access_key: creds.access_key.clone(),
        secret_key: creds.secret_key.clone(),
        path_style: true,
        timeout_ms: 120_000,
        multipart_threshold: 8 * 1024 * 1024,
        multipart_part_size: 8 * 1024 * 1024,
    };

    let objects = ObjectSpec {
        cprefix: cprefix.clone(),
        containers: IdRange {
            start: 1,
            end: container_count.max(1),
        },
        oprefix: "obj-".into(),
        objects: IdRange {
            start: 1,
            end: object_count.max(1),
        },
        size: task.size_bytes,
        size_min: 0,
        size_max: 0,
        hash_check: false,
    };

    let mut stages = Vec::new();
    // init containers
    stages.push(Stage {
        name: "init".into(),
        kind: "init".into(),
        workers: 1,
        runtime_secs: 0,
        total_ops: container_count.max(1),
        sequential: true,
        operations: vec![Operation {
            op_type: "create_container".into(),
            ratio: 100,
            config: HashMap::new(),
        }],
        objects: objects.clone(),
    });

    if task.method == Method::Read {
        // prepare write objects first
        stages.push(Stage {
            name: "prepare".into(),
            kind: "prepare".into(),
            workers: prepare_worker.min(256).max(1),
            runtime_secs: 0,
            total_ops: object_count,
            sequential: true,
            operations: vec![Operation {
                op_type: "write".into(),
                ratio: 100,
                config: HashMap::new(),
            }],
            objects: objects.clone(),
        });
        stages.push(Stage {
            name: "normal".into(),
            kind: "main".into(),
            workers: task.workers.max(1),
            runtime_secs: runtime,
            total_ops: 0,
            sequential: false,
            operations: vec![Operation {
                op_type: "read".into(),
                ratio: 100,
                config: HashMap::new(),
            }],
            objects: objects.clone(),
        });
    } else {
        stages.push(Stage {
            name: "normal".into(),
            kind: "main".into(),
            workers: task.workers.max(1),
            runtime_secs: runtime,
            total_ops: 0,
            sequential: false,
            operations: vec![Operation {
                op_type: "write".into(),
                ratio: 100,
                config: HashMap::new(),
            }],
            objects: objects.clone(),
        });
    }

    let wl = Workload {
        name: task.name.clone(),
        description: format!(
            "{} {}worker {}",
            task.size_label,
            task.workers,
            task.method.as_str()
        ),
        storage,
        auth: None,
        stages,
    };
    wl.validate()?;
    let reports = Driver::new(wl).run_all().await?;
    let normal = reports
        .iter()
        .find(|r| r.name == "normal")
        .or_else(|| reports.last())
        .context("no stage reports")?;
    let wid = next_wid()?;
    let csv = write_stage_csv(&wid, task, normal)?;
    Ok(InternalOutcome {
        wid,
        result_csv: csv,
    })
}

async fn run_via_mock(
    task: &TaskSpec,
    object_count: u64,
    container_count: u64,
    runtime: u64,
) -> Result<InternalOutcome> {
    let objects = ObjectSpec {
        cprefix: "mockbucket".into(),
        containers: IdRange {
            start: 1,
            end: container_count.max(1),
        },
        oprefix: "obj-".into(),
        objects: IdRange {
            start: 1,
            end: object_count.max(1),
        },
        size: task.size_bytes.min(4096).max(64), // keep mock light
        size_min: 0,
        size_max: 0,
        hash_check: false,
    };
    let mut stages = vec![Stage {
        name: "init".into(),
        kind: "init".into(),
        workers: 1,
        runtime_secs: 0,
        total_ops: 1,
        sequential: true,
        operations: vec![Operation {
            op_type: "create_container".into(),
            ratio: 100,
            config: HashMap::new(),
        }],
        objects: objects.clone(),
    }];
    if task.method == Method::Read {
        stages.push(Stage {
            name: "prepare".into(),
            kind: "prepare".into(),
            workers: 4,
            runtime_secs: 0,
            total_ops: object_count.min(50),
            sequential: true,
            operations: vec![Operation {
                op_type: "write".into(),
                ratio: 100,
                config: HashMap::new(),
            }],
            objects: ObjectSpec {
                objects: IdRange {
                    start: 1,
                    end: object_count.min(50),
                },
                ..objects.clone()
            },
        });
    }
    stages.push(Stage {
        name: "normal".into(),
        kind: "main".into(),
        workers: task.workers.min(8).max(1),
        runtime_secs: runtime.min(3).max(1),
        total_ops: 0,
        sequential: false,
        operations: vec![Operation {
            op_type: task.method.as_str().into(),
            ratio: 100,
            config: HashMap::new(),
        }],
        objects: ObjectSpec {
            objects: IdRange {
                start: 1,
                end: object_count.min(50).max(1),
            },
            ..objects
        },
    });
    let wl = Workload {
        name: task.name.clone(),
        description: "mock".into(),
        storage: StorageConfig::Mock { latency_us: 20 },
        auth: None,
        stages,
    };
    let reports = Driver::new(wl).run_all().await?;
    let normal = reports.iter().find(|r| r.name == "normal").unwrap();
    let wid = next_wid()?;
    let csv = write_stage_csv(&wid, task, normal)?;
    Ok(InternalOutcome {
        wid,
        result_csv: csv,
    })
}

async fn run_via_java_controller(
    task: &TaskSpec,
    creds: &S3Creds,
    object_count: u64,
    prepare_worker: u32,
    container_count: u64,
    runtime: u64,
    cosbench_url: &str,
) -> Result<InternalOutcome> {
    // Generate COSBench XML (same shape as original templates) and submit.
    let cprefix = format!(
        "mycontainers{}",
        &format!("{:x}", md5_lite(&task.name))[..7]
    );
    let object_size = format!("(1,1){}", {
        // original used (num,num)UNIT from label; for write sizes=u(num,num)UNIT
        let n: String = task.size_label.chars().filter(|c| c.is_ascii_digit()).collect();
        let u: String = task
            .size_label
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .collect();
        format!("({n},{n}){u}")
    });
    let xml = if task.method == Method::Write {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<workload name="{name}" description="{size} {w}worker write">
  <auth type="none"/>
  <storage type="s3" config="accesskey={ak};secretkey={sk};endpoint=http://{ep}/;path_style_access=true"/>
  <workflow>
    <workstage name="init">
      <work name="init" type="init" workers="10" totalOps="1" config="cprefix={cp};containers=r(1,{cc})">
        <operation type="init" ratio="100" config="cprefix={cp};containers=r(1,{cc});objects=r(0,0);sizes=c(0)B"/>
      </work>
    </workstage>
    <workstage name="normal">
      <work name="normal" type="normal" workers="{w}" runtime="{rt}" afr="200000">
        <operation type="write" ratio="100" config="cprefix={cp};containers=u(1,{cc});objects=u(1,{oc});sizes=u{osz}"/>
      </work>
    </workstage>
  </workflow>
</workload>"#,
            name = task.name,
            size = task.size_label,
            w = task.workers,
            ak = creds.access_key,
            sk = creds.secret_key,
            ep = creds.endpoint,
            cp = cprefix,
            cc = container_count,
            rt = runtime,
            oc = object_count,
            osz = object_size,
        )
    } else {
        // read: init+prepare then read
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<workload name="{name}" description="{size} {w}worker read">
  <auth type="none"/>
  <storage type="s3" config="accesskey={ak};secretkey={sk};endpoint=http://{ep}/;path_style_access=true"/>
  <workflow>
    <workstage name="init">
      <work name="init" type="init" workers="10" totalOps="{cc}" config="cprefix={cp};containers=r(1,{cc})">
        <operation type="init" ratio="100" config="cprefix={cp};containers=r(1,{cc});objects=r(0,0);sizes=c(0)B"/>
      </work>
    </workstage>
    <workstage name="prepare">
      <work name="prepare" type="prepare" workers="{pw}" totalOps="{pw}" config="cprefix={cp};containers=r(1,{cc});objects=r(1,{oc});sizes=u{osz}">
        <operation type="prepare" ratio="100" config="cprefix={cp};containers=r(1,{cc});objects=r(1,{oc});sizes=u{osz};createContainer=false"/>
      </work>
    </workstage>
    <workstage name="normal">
      <work name="normal" type="normal" workers="{w}" runtime="{rt}" afr="200000">
        <operation type="read" ratio="100" config="cprefix={cp};containers=u(1,{cc});objects=u(1,{oc});"/>
      </work>
    </workstage>
  </workflow>
</workload>"#,
            name = task.name,
            size = task.size_label,
            w = task.workers,
            ak = creds.access_key,
            sk = creds.secret_key,
            ep = creds.endpoint,
            cp = cprefix,
            cc = container_count,
            pw = prepare_worker,
            oc = object_count,
            osz = object_size,
            rt = runtime,
        )
    };

    // health
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let health = client
        .get(format!("{cosbench_url}/workload.html"))
        .send()
        .await
        .context("cosbench controller unreachable")?;
    if !health.status().is_success() {
        bail!("Cosbench not running at {cosbench_url}");
    }

    let part = reqwest::multipart::Part::bytes(xml.into_bytes())
        .file_name(format!("{}.xml", task.name))
        .mime_str("application/xml")?;
    let form = reqwest::multipart::Form::new().part("config", part);
    let resp = client
        .post(format!("{cosbench_url}/cli/submit.action"))
        .multipart(form)
        .send()
        .await?
        .text()
        .await?;
    // "Accepted with ID: w1" style
    let wid = resp
        .split_whitespace()
        .find(|t| t.starts_with('w') && t[1..].chars().all(|c| c.is_ascii_digit()))
        .unwrap_or("w?")
        .to_string();
    info!(%wid, "submitted to java cosbench");

    // poll until idle
    loop {
        let idx = client
            .get(format!("{cosbench_url}/cli/index.action"))
            .send()
            .await?
            .text()
            .await?;
        if idx.contains("0 active") || idx.contains("active workloads: 0") {
            break;
        }
        // also parse "N active workloads"
        if let Some(line) = idx.lines().find(|l| l.contains("active workload")) {
            if line.contains('0') {
                break;
            }
            println!("\x1b[32m  {line}\x1b[0m");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    // try to copy archive csv if present (best effort)
    let archive_root = PathBuf::from("/root/cosbench/0.4.2.c4/archive");
    let mut found = None;
    if archive_root.is_dir() {
        for ent in walkdir::WalkDir::new(&archive_root).max_depth(2) {
            let ent = ent?;
            let p = ent.path();
            if p.is_file()
                && p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.contains("normal-worker") && s.ends_with(".csv"))
                    .unwrap_or(false)
            {
                if p
                    .parent()
                    .and_then(|s| s.file_name())
                    .and_then(|s| s.to_str())
                    .map(|s| s.starts_with(&wid))
                    .unwrap_or(false)
                {
                    found = Some(p.to_path_buf());
                    break;
                }
            }
        }
    }
    let tmp = env_cfg::cabt_home().join("config").join(format!("{wid}-raw.csv"));
    if let Some(p) = found {
        fs::copy(p, &tmp)?;
    } else {
        // synthesize minimal csv so list/collect still work
        fs::write(
            &tmp,
            "Timestamp,Op-Type,Op-Count,Byte-Count,Avg-ResTime,Avg-ProcTime,Throughput,Bandwidth,Succ-Ratio\n\
             0,op,0,0,0,0,0,0,100\n",
        )?;
    }
    Ok(InternalOutcome {
        wid,
        result_csv: tmp,
    })
}

fn write_stage_csv(
    wid: &str,
    task: &TaskSpec,
    stage: &cosbench_core::StageReport,
) -> Result<PathBuf> {
    let path = env_cfg::cabt_home()
        .join("config")
        .join(format!("{wid}-stage.csv"));
    // Cosbench-like worker csv columns used by list()
    let mut w = csv::Writer::from_path(&path)?;
    w.write_record([
        "Timestamp",
        "Op-Type",
        "Op-Count",
        "Byte-Count",
        "Avg-ResTime",
        "Avg-ProcTime",
        "Throughput",
        "Bandwidth",
        "Succ-Ratio",
    ])?;
    let thr = stage.metrics.throughput_ops(stage.elapsed_secs);
    let bw = stage.metrics.bandwidth_mib_s(stage.elapsed_secs) * 1024.0; // KB/s-ish
    w.write_record([
        "0".into(),
        task.method.as_str().into(),
        (stage.metrics.ops_ok + stage.metrics.ops_fail).to_string(),
        stage.metrics.bytes.to_string(),
        format!("{:.2}", stage.metrics.lat_mean_us as f64 / 1000.0),
        format!("{:.2}", stage.metrics.lat_p50_us as f64 / 1000.0),
        format!("{:.2}", thr),
        format!("{:.2}", bw),
        format!("{:.2}", stage.metrics.success_ratio * 100.0),
    ])?;
    w.flush()?;
    Ok(path)
}

fn next_wid() -> Result<String> {
    let counter = env_cfg::cabt_home().join("config").join(".wid");
    let mut n: u64 = if counter.is_file() {
        fs::read_to_string(&counter)?.trim().parse().unwrap_or(0)
    } else {
        0
    };
    n += 1;
    fs::write(&counter, n.to_string())?;
    Ok(format!("w{n}"))
}

fn md5_lite(s: &str) -> u64 {
    // not real md5; stable hash for prefixes
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}
