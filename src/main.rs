use crate::dns::DnsProxy;

mod dns;

fn main() {
    println!("Starting DNS proxy...");
    let mut proxy = DnsProxy::new("127.0.0.1:53", &["8.8.8.8:53".to_owned(), "1.1.1.1:53".to_owned()]);
    proxy.run_server();
}
