mod config;
mod http;
mod output;
mod sim;

use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, error::ErrorKind};
use config::Config;
use serde::Serialize;

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let config = match Config::try_parse() {
        Ok(config) => config,
        Err(err) => {
            let code = if matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) {
                0
            } else {
                1
            };
            let _ = err.print();
            return ExitCode::from(code);
        }
    };

    if let Err(err) = config.validate() {
        eprintln!("configuration error: {err:#}");
        return ExitCode::from(1);
    }

    if config.manifest_json {
        if let Err(err) = print_manifest() {
            eprintln!("manifest error: {err:#}");
            return ExitCode::from(1);
        }
        return ExitCode::SUCCESS;
    }

    let mut engine = match sim::Engine::new(&config).await {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("bootstrap failure: {err:#}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = engine.run().await {
        eprintln!("simulation failure: {err:#}");
        return ExitCode::from(3);
    }

    ExitCode::SUCCESS
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,beacon_chain::canonical_head=off,store=warn",
                )
            }),
        )
        .init();
}

#[derive(Serialize)]
struct EngineManifest {
    engine_name: &'static str,
    engine_version: &'static str,
    engine_commit: &'static str,
    build_flags: Vec<&'static str>,
    fcr_spec_commit: &'static str,
}

fn print_manifest() -> Result<()> {
    let manifest = EngineManifest {
        engine_name: "lighthouse",
        engine_version: engine_version(),
        engine_commit: option_env!("VERGEN_GIT_SHA").unwrap_or(""),
        build_flags: build_flags(),
        fcr_spec_commit: "",
    };

    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn engine_version() -> &'static str {
    option_env!("VERGEN_GIT_DESCRIBE")
        .or(option_env!("VERGEN_GIT_SEMVER"))
        .unwrap_or(env!("CARGO_PKG_VERSION"))
}

fn build_flags() -> Vec<&'static str> {
    let mut flags = Vec::new();

    #[cfg(feature = "fake_crypto")]
    flags.push("fake_crypto");

    flags
}
