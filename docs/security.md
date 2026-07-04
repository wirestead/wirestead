# Security and Threat Model

## No transport encryption

`unilink` does not implement TLS, DTLS, or any other transport-level
encryption. Every transport (TCP, UDP, Serial, UDS) sends and receives data
in plaintext. There is currently no pluggable hook (e.g. an
`ssl::stream<tcp::socket>`-compatible interface) for adding encryption
without patching the library.

## Intended trust model

`unilink` is designed for use over networks and channels you already trust:
a local machine, a private LAN, a point-to-point serial/UDS link, or inside
a network perimeter secured by other means (VPN, SSH tunnel, physical
access control). It is **not** designed to be used directly over the public
internet or any other untrusted network, since:

- Data (including any application-level credentials or secrets your
  protocol carries) is visible to anyone who can observe the traffic.
- Data can be modified in transit without detection - there is no message
  authentication.
- `transport::TcpClient`/`transport::TcpServer` and `transport::UdsServer`
  do not authenticate peers beyond what the transport itself provides (a
  TCP handshake, or standard UDS file permissions - `UdsServer` supports
  restricting the socket file's local permissions via
  `UdsServer::socket_permissions(mode)`).

If you need confidentiality, integrity, or peer authentication over an
untrusted network, terminate TLS (or an equivalent) outside `unilink` -
for example, a reverse proxy/stunnel in front of a TCP server, or an SSH/VPN
tunnel wrapping the connection - and point `unilink` at the resulting local,
trusted endpoint.

## Reporting a vulnerability

Open an issue on this repository.
