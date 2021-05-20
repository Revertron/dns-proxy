use crate::dns::DnsProxy;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};
use simplelog::*;

mod dns;

fn main() {
    let config = ConfigBuilder::new()
        .add_filter_ignore_str("mio::poll")
        .set_thread_level(LevelFilter::Off)
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_time_level(LevelFilter::Error)
        .set_time_to_local(true)
        .build();

    if let Err(e) = TermLogger::init(LevelFilter::Trace, config, TerminalMode::Stdout, ColorChoice::Auto) {
        println!("Unable to initialize logger!\n{}", e);
    }

    info!("Starting DNS proxy...");
    let mut proxy = DnsProxy::new("127.0.0.1:53", &["8.8.8.8:53".to_owned(), "1.1.1.1:53".to_owned()]);
    proxy.run_server();
}
