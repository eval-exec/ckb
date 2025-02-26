use ckb_logger::info;
use ckb_util::Mutex;
use rand::distributions::Alphanumeric;
use rand::prelude::*;

use super::TorServer;
use crate::{Node, Spec};

pub struct TorHashPasswordConnect {
    tor_server: Mutex<TorServer>,
}

/// Generate a random alphanumeric string with provided length
pub fn random_alphanumeric_with_len(len: usize) -> String {
    let mut rng = thread_rng();
    std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(len)
        .collect()
}

impl Default for TorHashPasswordConnect {
    fn default() -> Self {
        let tor_server = Mutex::new(TorServer::new(Some(random_alphanumeric_with_len(63))));
        TorHashPasswordConnect { tor_server }
    }
}

impl Spec for TorHashPasswordConnect {
    crate::setup!(num_nodes: 3);
    fn before_run(&self) -> Vec<Node> {
        let tor_controller_url = format!("127.0.0.1:{}", self.tor_server.lock().control_port);
        let mut nodes = (0..self.setup().num_nodes)
            .map(|i| Node::new(self.name(), &format!("node{i}")))
            .collect::<Vec<_>>();
        nodes.iter_mut().for_each(|node| {
            node.modify_app_config(|config: &mut ckb_app_config::CKBAppConfig| {
                config.logger.filter = Some("ckb-network=trace,info".to_string());

                config.network.connect_outbound_interval_secs = 15;

                config.network.onion.listen_on_onion = true;

                config.network.onion.onion_server =
                    Some(format!("127.0.0.1:{}", self.tor_server.lock().socks_port));

                config.network.onion.tor_controller = tor_controller_url.clone();

                let p2p_addr = config.network.listen_addresses.first().unwrap().to_string();

                let p2p_port: u16 = p2p_addr.split("/tcp/").last().unwrap().parse().unwrap();
                info!("node p2p listen port: {}", p2p_port);

                config.network.onion.onion_service_target = Some(format!("127.0.0.1:{}", p2p_port));
            });

            node.start();
        });
        nodes
    }

    fn run(&self, nodes: &mut Vec<crate::Node>) {
        assert!(self.tor_server.lock().controller_password.is_some());
        self.tor_server.lock().tor_wait_bootstrap_done();
        info!("tor tor bootstrap done");
    }
}
