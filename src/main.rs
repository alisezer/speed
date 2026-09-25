mod latency;
mod net;
mod servers;
mod test;
mod ui;
mod watch;

use clap::{Parser, Subcommand};

/// Internet speed test and live throughput monitor.
#[derive(Parser)]
#[command(name = "speed", version, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    /// `speed [test options]` works as shorthand for `speed test`
    #[command(flatten)]
    test: test::Opts,
}

#[derive(Subcommand)]
enum Cmd {
    /// One-shot measurement: latency, download, upload (the default)
    Test(test::Opts),
    /// Continuous download monitor with a live chart; Ctrl+C to stop
    Watch(watch::Opts),
    /// Rank the download mirrors by round-trip time from here
    Servers,
}

#[tokio::main]
async fn main() {
    // Exit quietly when piped into something like `head` instead of panicking
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    ui::init_color();
    let cli = Cli::parse();
    let cmd = cli.cmd.unwrap_or(Cmd::Test(cli.test));
    let res = match cmd {
        Cmd::Test(o) => tokio::select! {
            r = test::run(o) => r,
            _ = tokio::signal::ctrl_c() => {
                // Background transfers die with the process; just restore the terminal
                println!("{}", ui::SHOW_CURSOR);
                std::process::exit(130);
            }
        },
        Cmd::Watch(o) => watch::run(o).await,
        Cmd::Servers => servers::list().await,
    };
    if let Err(e) = res {
        eprint!("{}", ui::SHOW_CURSOR);
        eprintln!("speed: {e:#}");
        std::process::exit(1);
    }
}
