use std::net::{SocketAddr, Shutdown};
use mio::net::{UdpSocket, TcpStream, TcpListener};
use mio::{Events, Poll, Interest, Token, Registry};
use std::time::{Duration, Instant};
use trust_dns_proto::op::Message;
use trust_dns_proto::serialize::binary::BinEncodable;
use std::collections::{HashMap, VecDeque};
use rand::prelude::IteratorRandom;
use mio::event::Event;
use std::io::{Read, Write, ErrorKind};
use std::{thread, io};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, LevelFilter};

const SERVER_UDP: Token = Token(0);
const SERVER_TCP: Token = Token(1);
const FORWARD_UDP4: Token = Token(2);
const FORWARD_UDP6: Token = Token(3);
const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1000));
const MAX_READ_BLOCK_TIME: u128 = 100;

const POLL_ERROR: &str = "Error registering poll";
const BIND_ERROR: &str = "Error binding DNS server socket!";
const ADDR_PARSE_ERROR: &str = "Error parsing DNS server socket address!";
const LOOP_FINISHED: &str = "DNS loop has finished";

pub struct DnsProxy {
    listen: String,
    forwarders: Vec<SocketAddr>,
    bucket_udp: HashMap<u16, (SocketAddr, u16)>,
    bucket_tcp: HashMap<u16, (Token, u16)>,
    tcp_peers: HashMap<Token, TcpStream>,
    tcp_forward_sockets: HashMap<Token, TcpStream>,
    tcp_requests: VecDeque<Message>,
    responses: HashMap<Token, Vec<Message>>,
    token: Token,
    id: u16
}

impl DnsProxy {
    pub fn new(listen: &str, servers: &[String]) -> Self {
        let forwarders: Vec<SocketAddr> = servers.iter().map(|addr| addr.parse().unwrap()).collect();
        DnsProxy {
            listen: listen.to_owned(),
            forwarders,
            bucket_udp: HashMap::new(),
            bucket_tcp: HashMap::new(),
            tcp_peers: HashMap::new(),
            tcp_forward_sockets: HashMap::new(),
            tcp_requests: VecDeque::new(),
            responses: HashMap::new(),
            token: Token(5),
            id: 1
        }
    }

    pub fn run_server(&mut self) {
        let mut buffer = [0u8; 2048];
        let mut poll = Poll::new().expect("Unable to create poll");
        let mut events = Events::with_capacity(16);

        // Listening UDP socket
        let addr = self.listen.parse().expect(ADDR_PARSE_ERROR);
        let mut server_udp = UdpSocket::bind(addr).expect(BIND_ERROR);
        poll.registry().register(&mut server_udp, SERVER_UDP, Interest::READABLE).expect(POLL_ERROR);

        // Listening TCP socket
        let addr = self.listen.parse().expect(ADDR_PARSE_ERROR);
        let mut server_tcp = TcpListener::bind(addr).expect(BIND_ERROR);
        poll.registry().register(&mut server_tcp, SERVER_TCP, Interest::READABLE).expect(POLL_ERROR);

        // UDP socket to forward to any IPv4 upstream
        let addr4 = SocketAddr::new("0.0.0.0".parse().unwrap(), generate_port());
        let mut forward_udp4 = UdpSocket::bind(addr4).expect(BIND_ERROR);
        poll.registry().register(&mut forward_udp4, FORWARD_UDP4, Interest::READABLE).expect(POLL_ERROR);

        // UDP socket to forward to any IPv6 upstream
        let addr6 = SocketAddr::new("::".parse().unwrap(), generate_port());
        let mut forward_udp6 = UdpSocket::bind(addr6).expect(BIND_ERROR);
        poll.registry().register(&mut forward_udp6, FORWARD_UDP6, Interest::READABLE).expect(POLL_ERROR);

        loop {
            // Poll Mio for events, blocking until we get an event.
            poll.poll(&mut events, POLL_TIMEOUT).expect("Error polling sockets");

            // Process each event.
            for event in events.iter() {
                match event.token() {
                    SERVER_UDP => {
                        if event.is_readable() {
                            let (_size, addr) = match server_udp.recv_from(&mut buffer) {
                                Ok(pair) => pair,
                                Err(err) => {
                                    if let Some(code) = err.raw_os_error() {
                                        if code == 10004 {
                                            println!("{}", LOOP_FINISHED);
                                            break;
                                        }
                                    }
                                    debug!("Failed to read from UDP socket: {:?}", err);
                                    poll.registry().reregister(&mut server_udp, SERVER_UDP, Interest::READABLE).expect(POLL_ERROR);
                                    continue;
                                }
                            };
                            if let Ok(message) = Message::from_vec(&buffer) {
                                let server = self.forwarders.iter().choose(&mut rand::thread_rng()).unwrap().clone();
                                let mut socket = Self::select_socket(&server, &mut forward_udp4, &mut forward_udp6);
                                debug!("Sending request to {}", &server);
                                self.process_request_from_udp(&mut socket, server, message, addr, poll.registry());
                            }
                        }
                        poll.registry().reregister(&mut server_udp, SERVER_UDP, Interest::READABLE).expect(POLL_ERROR);
                    }
                    SERVER_TCP => {
                        // If this is an event for the server, it means a connection is ready to be accepted.
                        let connection = server_tcp.accept();
                        match connection {
                            Ok((mut stream, address)) => {
                                let token = self.next_token();
                                debug!("Connected new client {} from {}", &token.0, &address);
                                poll.registry().register(&mut stream, token, Interest::READABLE).expect(POLL_ERROR);
                                self.tcp_peers.insert(token, stream);
                            }
                            Err(_) => {}
                        }
                        poll.registry().reregister(&mut server_tcp, SERVER_TCP, Interest::READABLE).expect(POLL_ERROR);
                    }
                    FORWARD_UDP4 => {
                        if event.is_readable() {
                            if !self.process_upstream_response(&mut forward_udp4, &mut server_udp, poll.registry(), &mut buffer) {
                                warn!("{}", LOOP_FINISHED);
                                break;
                            }
                        } else {
                            warn!("Event: {:?}", &event);
                        }
                        poll.registry().reregister(&mut forward_udp4, FORWARD_UDP4, Interest::READABLE).expect(POLL_ERROR);
                    }
                    FORWARD_UDP6 => {
                        if event.is_readable() {
                            if !self.process_upstream_response(&mut forward_udp6, &mut server_udp, poll.registry(), &mut buffer) {
                                warn!("{}", LOOP_FINISHED);
                                break;
                            }
                        } else {
                            warn!("Event: {:?}", &event);
                        }
                        poll.registry().reregister(&mut forward_udp6, FORWARD_UDP6, Interest::READABLE).expect(POLL_ERROR);
                    }
                    token => {
                        if self.tcp_peers.contains_key(&token) {
                            //debug!("Event from TCP client: {:?}", &event);
                            let server = self.forwarders.iter().choose(&mut rand::thread_rng()).unwrap().clone();
                            if self.process_client_tcp_event(&event, &token, server, poll.registry(), &mut buffer).is_err() {
                                if let Some(stream) = self.tcp_peers.get_mut(&token) {
                                    let _ = poll.registry().deregister(stream);
                                    let _ = stream.shutdown(Shutdown::Both);
                                }
                                self.tcp_peers.remove(&token);
                            }
                        } else if self.tcp_forward_sockets.contains_key(&token) {
                            //debug!("Event from TCP upstream: {:?}", &event);
                            if self.process_upstream_tcp_event(&event, &token, poll.registry(), &mut buffer).is_err() {
                                if let Some(stream) = self.tcp_peers.get_mut(&token) {
                                    let _ = poll.registry().deregister(stream);
                                    let _ = stream.shutdown(Shutdown::Both);
                                }
                                self.tcp_peers.remove(&token);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Processes TCP events from our dear clients
    fn process_client_tcp_event(&mut self, event: &Event, token: &Token, server: SocketAddr, registry: &Registry, buffer: &mut [u8]) -> io::Result<()> {
        let stream = match self.tcp_peers.get_mut(token) {
            Some(stream) => { stream }
            None => { return Err(io::Error::from(ErrorKind::NotFound)); }
        };

        if event.is_error() || (event.is_read_closed() && event.is_write_closed()) {
            // TODO close peer
            return Err(io::Error::from(ErrorKind::BrokenPipe));
        }

        if event.is_readable() {
            if event.is_read_closed() {
                return Err(io::Error::from(ErrorKind::ConnectionAborted));
            }
            match read_message(stream, buffer) {
                Ok(_size) => {
                    if let Ok(message) = Message::from_vec(&buffer) {
                        //debug!("Got request from client: {:?}, sending to {}", &message, &server);
                        self.process_request_from_tcp(server, message, token.clone(), registry);
                    }
                }
                Err(_) => {
                    return Err(io::Error::from(ErrorKind::BrokenPipe));
                }
            }
        }
        if event.is_writable() {
            match self.tcp_peers.get_mut(token) {
                None => { return Err(io::Error::from(ErrorKind::NotFound)); }
                Some(stream) => {
                    if let Some(responses) = self.responses.remove(&token) {
                        for message in responses {
                            //println!("Writing response: {:?}", &message);
                            let buf = message.to_vec().unwrap();
                            stream.write_u16::<BigEndian>(buf.len() as u16)?;
                            stream.write_all(&buf)?;
                            registry.reregister(stream, token.clone(), Interest::READABLE)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Processes TCP events from our dear clients
    fn process_upstream_tcp_event(&mut self, event: &Event, token: &Token, registry: &Registry, buffer: &mut [u8]) -> io::Result<()> {
        let stream = match self.tcp_forward_sockets.get_mut(token) {
            Some(stream) => { stream }
            None => { return Err(io::Error::from(ErrorKind::NotFound)); }
        };

        if event.is_error() || (event.is_read_closed() && event.is_write_closed()) {
            // TODO close peer
            return Err(io::Error::from(ErrorKind::BrokenPipe));
        }

        if event.is_readable() {
            if event.is_read_closed() {
                return Err(io::Error::from(ErrorKind::ConnectionAborted));
            }
            match read_message(stream, buffer) {
                Ok(_size) => {
                    if let Ok(message) = Message::from_vec(&buffer) {
                        //debug!("Got response from TCP upstream: {:?}", &message);
                        self.process_response_from_tcp(message, registry);
                    }
                }
                Err(_) => {
                    return Err(io::Error::from(ErrorKind::BrokenPipe));
                }
            }
        }
        if event.is_writable() {
            match self.tcp_forward_sockets.get_mut(token) {
                None => { return Err(io::Error::from(ErrorKind::NotFound)); }
                Some(stream) => {
                    if let Some(message) = self.tcp_requests.pop_front() {
                        let buf = message.to_vec().unwrap();
                        stream.write_u16::<BigEndian>(buf.len() as u16)?;
                        stream.write_all(&buf)?;
                        if !self.tcp_requests.is_empty() {
                            registry.reregister(stream, token.clone(), Interest::READABLE | Interest::WRITABLE)?;
                        } else {
                            registry.reregister(stream, token.clone(), Interest::READABLE)?;
                        }
                    } else {
                        registry.reregister(stream, token.clone(), Interest::READABLE)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_upstream_response(&mut self, socket: &mut UdpSocket, mut server_udp: &mut UdpSocket, registry: &Registry, buffer: &mut [u8]) -> bool {
        let (_size, _addr) = match socket.recv_from(buffer) {
            Ok(pair) => pair,
            Err(err) => {
                if let Some(code) = err.raw_os_error() {
                    if code == 10004 {
                        return false;
                    }
                }
                warn!("Failed to read from UDP socket: {:?}", err);
                return true;
            }
        };
        // TODO check addr to be equal to forwarding address
        if let Ok(message) = Message::from_vec(&buffer) {
            //trace!("Response: {:?}", &message);
            self.process_response(&mut server_udp, registry, message);
        }
        true
    }

    fn process_request_from_udp(&mut self, socket: &mut UdpSocket, forward: SocketAddr, mut message: Message, source: SocketAddr, registry: &Registry) {
        let id = self.next_id();
        let request_id = message.id();
        message.set_id(id);
        let token= if socket.local_addr().unwrap().is_ipv6() { FORWARD_UDP6 } else { FORWARD_UDP4 };
        if socket.send_to(message.to_bytes().unwrap().as_slice(), forward).is_ok() {
            registry.reregister(socket, token, Interest::READABLE).expect("Error registering poll");
            self.bucket_udp.insert(id, (source, request_id));
        }
    }

    fn process_request_from_tcp(&mut self, forward: SocketAddr, mut message: Message, source_token: Token, registry: &Registry) {
        let id = self.next_id();
        let request_id = message.id();
        message.set_id(id);
        self.tcp_requests.push_back(message);
        if let Ok(mut socket) = TcpStream::connect(forward) {
            debug!("Connected to {}", &forward);
            let token = self.next_token();
            registry.register(&mut socket, token, Interest::WRITABLE).expect("Error registering poll");
            self.bucket_tcp.insert(id, (source_token, request_id));
            self.tcp_forward_sockets.insert(token, socket);
        }
    }

    fn process_response_from_tcp(&mut self, mut message: Message, registry: &Registry) {
        if let Some((token, id)) = self.bucket_tcp.get(&message.id()) {
            // If we still have that connection
            if let Some(stream) = self.tcp_peers.get_mut(&token) {
                message.set_id(*id);
                self.responses.entry(token.to_owned()).or_default().push(message);
                if registry.reregister(stream, token.clone(), Interest::WRITABLE).is_err() {
                    let _ = registry.deregister(stream);
                    let _ = stream.shutdown(Shutdown::Both);
                    self.tcp_peers.remove(token);
                    self.responses.remove(token);
                }
            }
        }
    }

    fn process_response(&mut self, socket: &mut UdpSocket, registry: &Registry, mut message: Message) {
        if let Some((addr, id)) = self.bucket_udp.get(&message.id()) {
            message.set_id(*id);
            socket.send_to(message.to_bytes().unwrap().as_slice(), *addr).expect("Error sending response to client!");
            return;
        }
        if let Some((token, id)) = self.bucket_tcp.get(&message.id()) {
            // If we still have that connection
            if let Some(stream) = self.tcp_peers.get_mut(&token) {
                message.set_id(*id);
                self.responses.entry(token.to_owned()).or_default().push(message);
                if registry.reregister(stream, token.clone(), Interest::WRITABLE).is_err() {
                    let _ = registry.deregister(stream);
                    let _ = stream.shutdown(Shutdown::Both);
                    self.tcp_peers.remove(token);
                }
            }
        }
    }

    fn select_socket<'a>(addr: &'a SocketAddr, socket_v4: &'a mut UdpSocket, socket_v6: &'a mut UdpSocket) -> &'a mut UdpSocket {
        match &addr {
            SocketAddr::V4(_) => { socket_v4 }
            SocketAddr::V6(_) => { socket_v6 }
        }
    }

    /// Gets new token from old token, mutating the last
    pub fn next_id(&mut self) -> u16 {
        let next = self.id;
        self.id = self.id.wrapping_add(1);
        next
    }

    /// Gets new token from old token, mutating the last
    pub fn next_token(&mut self) -> Token {
        let current = self.token.0;
        self.token.0 += 1;
        Token(current)
    }
}

fn generate_port() -> u16 {
    (rand::random::<u16>() % 55000) + 2000
}

fn read_message(stream: &mut TcpStream, buffer: &mut [u8]) -> Result<usize, usize> {
    let instant = Instant::now();
    let data_size = match stream.read_u16::<BigEndian>() {
        Ok(size) => { size as usize }
        Err(e) => {
            println!("Error reading from socket! {}", e);
            0
        }
    };
    if data_size > buffer.len() || data_size == 0 {
        return Err(0);
    }
    let mut bytes_read = 0;
    let delay = Duration::from_millis(2);
    loop {
        match stream.read(&mut buffer[bytes_read..]) {
            Ok(bytes) => {
                bytes_read += bytes;
                if bytes_read == data_size {
                    break;
                }
            }
            // Would block "errors" are the OS's way of saying that the connection is not actually ready to perform this I/O operation.
            Err(ref err) if would_block(err) => {
                // We give every connection no more than 100ms to read a message
                if instant.elapsed().as_millis() < MAX_READ_BLOCK_TIME {
                    // We need to sleep a bit, otherwise it can eat CPU
                    thread::sleep(delay);
                    continue;
                } else {
                    break;
                }
            },
            Err(ref err) if interrupted(err) => continue,
            // Other errors we'll consider fatal.
            Err(_) => {
                println!("Error reading message, only {}/{} bytes read", bytes_read, data_size);
                return Err(bytes_read)
            },
        }
    }
    if bytes_read == data_size {
        Ok(bytes_read)
    } else {
        Err(bytes_read)
    }
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}