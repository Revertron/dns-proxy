use crate::dns::DnsProxy;

mod dns;

fn main() {
    println!("Starting DNS proxy...");
    let mut proxy = DnsProxy::new(&["192.168.44.1:53".to_owned()]);
    proxy.run_server();
}
