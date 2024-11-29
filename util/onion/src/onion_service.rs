use base64::Engine;
use ckb_async_runtime::Handle;
use ckb_error::{Error, InternalErrorKind};
use ckb_logger::{debug, error, info, warn};
use ckb_network::multiaddr::MultiAddr;
use ckb_network::NetworkController;
use ckb_stop_handler::CancellationToken;
use futures::future::BoxFuture;
use multiaddr::Multiaddr;
use std::borrow::Cow;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::{fs::File, io::AsyncReadExt};
use torut::control::{AsyncEvent, ConnError};
use torut::onion;
use torut::{
    control::{TorAuthData, TorAuthMethod, UnauthenticatedConn, COOKIE_LENGTH},
    onion::TorSecretKeyV3,
};

use crate::tor_controller::{self, TorController};
use crate::OnionServiceConfig;

type TorEventHandlerFn = fn(torut::control::AsyncEvent) -> Result<(), torut::control::ConnError>;

async fn tor_event_handler(
    _event: torut::control::AsyncEvent<'_>,
) -> Result<(), torut::control::ConnError> {
    //
    Ok(())
}

/// Onion service.
pub struct OnionService {
    key: TorSecretKeyV3,
    config: OnionServiceConfig,
    handle: Handle,
}
impl OnionService {
    /// Create a new onion service with the given configuration.
    pub fn new(
        handle: Handle,
        config: OnionServiceConfig,
        node_id: String,
    ) -> Result<(OnionService, MultiAddr), Error> {
        let key: TorSecretKeyV3 =
            load_or_create_tor_secret_key(config.onion_private_key_path.clone())?;

        let tor_address_without_dot_onion = key
            .public()
            .get_onion_address()
            .get_address_without_dot_onion();

        let onion_multi_addr_str = format!(
            "/onion3/{}:8115/p2p/{}",
            tor_address_without_dot_onion, node_id
        );
        let onion_multi_addr = MultiAddr::from_str(&onion_multi_addr_str).map_err(|err| {
            InternalErrorKind::Other.other(format!(
                "Failed to parse onion address {} to multi_addr: {:?}",
                onion_multi_addr_str, err
            ))
        })?;

        let onion_service = OnionService {
            config,
            key,
            handle,
        };
        Ok((onion_service, onion_multi_addr))
    }

    pub async fn start(
        &self,
        network_controller: NetworkController,
        onion_service_addr: MultiAddr,
    ) -> Result<(), Error> {
        let stop_rx = ckb_stop_handler::new_tokio_exit_rx();
        loop {
            let (tor_server_alive_tx, mut tor_server_alive_rx) =
                tokio::sync::mpsc::unbounded_channel::<()>();
            match self
                .launch_onion_service(stop_rx.clone(), tor_server_alive_tx)
                .await
            {
                Ok(_) => {
                    info!("CKB has started listening on the onion hidden network, the onion service address is: {}", onion_service_addr.clone());
                    network_controller.add_public_addr(onion_service_addr.clone());
                }
                Err(err) => {
                    error!("start onion service failed: {}", err);
                }
            }

            let _ = tor_server_alive_rx.recv().await;
            if stop_rx.is_cancelled() {
                return Ok(());
            }
            warn!("It seem that the connection to tor server's controller has been closed, retry connect to tor controller({})", self.config.tor_controller.to_string());
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn launch_onion_service(
        &self,
        stop_rx: CancellationToken,
        tor_server_alive_tx: UnboundedSender<()>,
    ) -> Result<(), Error> {
        let tor_controller = self.config.tor_controller.to_string();
        let tor_password = self.config.tor_password.clone();

        let mut tor_controller = TorController::new(tor_controller, tor_password, None).await?;

        tor_controller.wait_tor_server_bootstrap_done().await;

        info!("Adding onion service v3...");
        let mut onion_listeners = [(8115, self.config.onion_service_target)];
        tor_controller
            .add_onion_v3(self.key.clone(), &mut onion_listeners.iter())
            .await
            .map_err(|err| {
                InternalErrorKind::Other.other(format!("Failed to add onion service: {:?}", err))
            })?;
        info!("Added onion service v3!");

        self.handle.spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(3));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let uptime = tor_controller.get_uptime().await;
                        if let Err(err) = uptime {
                            error!("Failed to get tor server uptime: {:?}", err);
                            return;
                        }
                    }
                    _ = stop_rx.cancelled() => {
                        info!("OnionService received stop signal, exiting...");
                        return;
                    }
                }
            }
            let _tx = tor_server_alive_tx;
        });
        Ok(())
    }
}

fn load_or_create_tor_secret_key(onion_private_key_path: String) -> Result<TorSecretKeyV3, Error> {
    let is_onion_private_key_exists = std::fs::exists(&onion_private_key_path).map_err(|err| {
        InternalErrorKind::Other.other(format!("Failed to check onion private key path: {:?}", err))
    })?;
    let key = match is_onion_private_key_exists {
        true => load_tor_secret_key(onion_private_key_path)?,
        false => create_tor_secret_key(onion_private_key_path)?,
    };
    Ok(key)
}

fn create_tor_secret_key(onion_private_key_path: String) -> Result<TorSecretKeyV3, Error> {
    let key = torut::onion::TorSecretKeyV3::generate();
    info!(
        "Generated new onion service v3 key for address: {}",
        key.public().get_onion_address()
    );
    std::fs::write(
        &onion_private_key_path,
        base64::engine::general_purpose::STANDARD.encode(key.as_bytes()),
    )
    .map_err(|err| {
        InternalErrorKind::Other.other(format!("Failed to write onion private key: {:?}", err))
    })?;
    Ok(key)
}

fn load_tor_secret_key(onion_private_key_path: String) -> Result<TorSecretKeyV3, Error> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(std::fs::read_to_string(onion_private_key_path).unwrap())
        .map_err(|err| {
            InternalErrorKind::Other.other(format!("Failed to decode onion private key: {:?}", err))
        })?;
    let raw = raw.as_slice();
    if raw.len() != 64 {
        return Err(InternalErrorKind::Other
            .other("Invalid secret key length")
            .into());
    }
    let mut buf = [0u8; 64];
    buf.clone_from_slice(raw);
    Ok(TorSecretKeyV3::from(buf))
}
