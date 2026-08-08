# distruct

Raft-based cluster coordination and strongly-consistent distributed data structures.

## Create a certificate chain and a private key for testing:

openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature,keyEncipherment" -addext "extendedKeyUsage=serverAuth"

## License

distruct is released under the **MIT license**. See the [LICENSE](LICENSE) file for more information.
