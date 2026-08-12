use anyhow::{Context, Result};
use log::{debug, error, info};
use quinn::{ConnectionError, Endpoint, Incoming, RecvStream, SendStream};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};
use tracing::{Instrument, info_span};

use crate::connection::message::{Request, Response, ResponseError};

pub(crate) struct Server {
    receiver: UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ResponseError>>)>,
}

impl Server {
    pub(super) fn new(endpoint: Endpoint) -> Self {
        let (sender, receiver) = unbounded_channel();

        tokio::spawn(async move {
            while let Some(conn) = endpoint.accept().await {
                if !conn.remote_address_validated() {
                    info!("requiring connection to validate its address");
                    conn.retry().unwrap();
                } else {
                    info!("accepting connection");
                    let fut = handle_connection(conn, sender.clone());
                    tokio::spawn(async move {
                        if let Err(e) = fut.await {
                            error!("connection failed: {e}");
                        }
                    });
                }
            }
        });

        Server { receiver }
    }

    pub(crate) async fn recv(
        &mut self,
    ) -> Option<(Request, oneshot::Sender<Result<Response, ResponseError>>)> {
        self.receiver.recv().await
    }
}

async fn handle_connection(
    conn: Incoming,
    sender: UnboundedSender<(Request, oneshot::Sender<Result<Response, ResponseError>>)>,
) -> Result<()> {
    let connection = conn.await?;

    let span = info_span!(
        "connection",
        remote = %connection.remote_address()
    );
    async {
        info!("connection established");

        // each stream initiated by the client constitutes a new request
        loop {
            let stream = match connection.accept_bi().await {
                Err(
                    ConnectionError::ApplicationClosed { .. }
                    | ConnectionError::TimedOut
                    | ConnectionError::LocallyClosed,
                ) => {
                    debug!("connection closed");
                    return anyhow::Ok(());
                }
                Err(e) => return Err(e.into()),
                Ok(s) => s,
            };

            let fut = handle_request(stream, sender.clone());
            tokio::spawn(
                async move {
                    if let Err(e) = fut.await {
                        error!("request failed: {e}");
                    }
                }
                .instrument(info_span!("request")),
            );
        }
    }
    .instrument(span)
    .await?;
    Ok(())
}

async fn handle_request(
    (mut send, mut recv): (SendStream, RecvStream),
    sender: UnboundedSender<(Request, oneshot::Sender<Result<Response, ResponseError>>)>,
) -> Result<()> {
    let req = recv
        .read_to_end(64 * 1024) // TODO maximum message size should be configurable
        .await
        .context("failed to read request")?;

    let message: Request = postcard::from_bytes(&req)?;

    let (reply_sender, reply_receiver) = oneshot::channel();

    sender.send((message, reply_sender))?;

    if let Ok(reply) = reply_receiver.await {
        // write response
        let msg = postcard::to_allocvec(&reply)?;
        send.write_all(&msg)
            .await
            .context("failed to send response")?;
    }

    send.finish().unwrap();

    Ok(())
}
