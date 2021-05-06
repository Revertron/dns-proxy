# DNS proxy

A minimal DNS-proxy to stream DNS-queries and responses to multiple upstreams.
It uses `mio::poll` and non-blocking sockets to implement single-threaded and very tight on resources DNS proxy server.
For parsing DNS messages it uses `trust-dns-proto` crate.
It is very fast and straight forward.

It is being developed to replace current DNS implementation in [ALFIS](https://github.com/Revertron/Alfis/).
