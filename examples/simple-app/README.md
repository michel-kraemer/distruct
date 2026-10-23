# Simple cluster application example

1. Create a certificate chain and a private key for testing:

    ```sh
    openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature,keyEncipherment" -addext "extendedKeyUsage=serverAuth"
    ```

2. Start first instance:

    ```sh
    cargo r -- --bind-addr ::1 --bind-port 35000 --public-addr localhost --public-port 35000
    ```

3. Start second instance:

    ```sh
    cargo r -- --bind-addr ::1 --bind-port 35001 --public-addr localhost --public-port 35001 --seed-addr localhost --seed-port 35000
    ```
