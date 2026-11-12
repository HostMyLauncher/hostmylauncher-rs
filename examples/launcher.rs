//! Demo: list instances, resolve by code, fetch files, and stream real-time events.
//!
//! Run against a local HostMyLauncher API:
//!   HML_BASE_URL=http://localhost:8787 HML_LAUNCHER_ID=lns HML_ACCESS_CODE=SECRET \
//!     cargo run --example launcher

use std::time::Duration;

use futures::StreamExt;
use hostmylauncher::{Client, ConnectOpts, ListOpts};

#[tokio::main]
async fn main() -> hostmylauncher::Result<()> {
    let launcher = std::env::var("HML_LAUNCHER_ID").unwrap_or_else(|_| "lns".into());
    let client = match std::env::var("HML_BASE_URL") {
        Ok(base_url) => Client::with_base_url(&base_url)?,
        Err(std::env::VarError::NotPresent) => Client::new(),
        Err(error) => return Err(hostmylauncher::Error::Other(error.to_string())),
    };

    let instances = client
        .list_instances(&launcher, &ListOpts::default())
        .await?;
    println!("instances ({}):", instances.len());
    for i in &instances {
        println!("  - {} [{}]", i.title.clone().unwrap_or_default(), i.id);
    }

    if let Ok(code) = std::env::var("HML_ACCESS_CODE") {
        match client.resolve_by_code(&[launcher.as_str()], &code).await {
            Ok(inst) => {
                println!(
                    "resolved public instance: {} [{}]",
                    inst.title.clone().unwrap_or_default(),
                    inst.id
                );
                let files = client
                    .get_instance_files(
                        inst.launcher_id
                            .as_deref()
                            .expect("resolver returns launcher_id"),
                        &inst.id,
                        Some(&code),
                    )
                    .await
                    .unwrap_or_default();
                println!("  files: {}", files.len());
            }
            Err(e) => println!("resolve_by_code error: {e}"),
        }
    }

    println!("listening for real-time events (30s)...");
    let mut rt = client.connect(&launcher, ConnectOpts::default());
    let timeout = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            _ = &mut timeout => { println!("done."); break; }
            ev = rt.next() => match ev {
                Some(ev) => println!("event: {ev:?}"),
                None => { println!("stream ended."); break; }
            },
        }
    }
    Ok(())
}
