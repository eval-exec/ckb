use crate::{Relayer, SyncShared};
use ckb_app_config::NetworkConfig;
use ckb_chain::chain::ChainService;
use ckb_chain_spec::consensus::{build_genesis_epoch_ext, ConsensusBuilder};
use ckb_launcher::SharedBuilder;
use ckb_network::{
    async_trait, bytes::Bytes as P2pBytes, Behaviour, CKBProtocolContext, DefaultExitHandler,
    Error, Flags, NetworkController, NetworkService, NetworkState, Peer, PeerIndex, ProtocolId,
    SupportProtocols, TargetSession,
};
use ckb_shared::Shared;
use ckb_store::ChainStore;
use ckb_test_chain_utils::always_success_cell;
use ckb_types::prelude::*;
use ckb_types::{
    bytes::Bytes,
    core::{
        capacity_bytes, BlockBuilder, BlockNumber, Capacity, EpochNumberWithFraction,
        HeaderBuilder, HeaderView, TransactionBuilder, TransactionView,
    },
    packed::{
        CellDep, CellInput, CellOutputBuilder, IndexTransaction, IndexTransactionBuilder, OutPoint,
        Script,
    },
    utilities::difficulty_to_compact,
    U256,
};
use ckb_verification_traits::Switch;
use faketime::{self, unix_time_as_millis};
use std::{cell::RefCell, future::Future, pin::Pin, sync::Arc, time::Duration};

pub(crate) fn new_index_transaction(index: usize) -> IndexTransaction {
    let transaction = TransactionBuilder::default()
        .output(
            CellOutputBuilder::default()
                .capacity(Capacity::bytes(index).unwrap().pack())
                .build(),
        )
        .output_data(Bytes::new().pack())
        .build();

    IndexTransactionBuilder::default()
        .index(index.pack())
        .transaction(transaction.data())
        .build()
}

pub(crate) fn new_header_builder(shared: &Shared, parent: &HeaderView) -> HeaderBuilder {
    let parent_hash = parent.hash();
    let snapshot = shared.snapshot();
    let epoch = snapshot
        .consensus()
        .next_epoch_ext(parent, &snapshot.borrow_as_data_loader())
        .unwrap()
        .epoch();
    HeaderBuilder::default()
        .parent_hash(parent_hash)
        .number((parent.number() + 1).pack())
        .timestamp((parent.timestamp() + 1).pack())
        .epoch(epoch.number_with_fraction(parent.number() + 1).pack())
        .compact_target(epoch.compact_target().pack())
}

pub(crate) fn new_transaction(
    relayer: &Relayer,
    index: usize,
    always_success_out_point: &OutPoint,
) -> TransactionView {
    let previous_output = {
        let snapshot = relayer.shared.shared().snapshot();
        let tip_hash = snapshot.tip_header().hash();
        let block = relayer
            .shared
            .shared()
            .store()
            .get_block(&tip_hash)
            .expect("getting tip block");
        let txs = block.transactions();
        let cellbase = txs.first().expect("getting cellbase from tip block");
        cellbase.output_pts()[0].clone()
    };

    TransactionBuilder::default()
        .input(CellInput::new(previous_output, 0))
        .output(
            CellOutputBuilder::default()
            .capacity(Capacity::bytes(500 + index).unwrap().pack()) // use capacity to identify transactions
            .build(),
        )
        .output_data(Bytes::new().pack())
        .cell_dep(
            CellDep::new_builder()
                .out_point(always_success_out_point.to_owned())
                .build(),
        )
        .build()
}

pub(crate) fn dummy_network(shared: &Shared) -> NetworkController {
    let tmp_dir = tempfile::Builder::new().tempdir().unwrap();
    let config = NetworkConfig {
        max_peers: 19,
        max_outbound_peers: 5,
        path: tmp_dir.path().to_path_buf(),
        ping_interval_secs: 15,
        ping_timeout_secs: 20,
        connect_outbound_interval_secs: 1,
        discovery_local_address: true,
        bootnode_mode: true,
        reuse_port_on_linux: true,
        ..Default::default()
    };

    let network_state =
        Arc::new(NetworkState::from_config(config).expect("Init network state failed"));
    NetworkService::new(
        network_state,
        vec![],
        vec![],
        (
            shared.consensus().identify_name(),
            "test".to_string(),
            Flags::COMPATIBILITY,
        ),
        DefaultExitHandler::default(),
    )
    .start(shared.async_handle())
    .expect("Start network service failed")
}

pub(crate) fn build_chain(tip: BlockNumber) -> (Relayer, OutPoint) {
    let (always_success_cell, always_success_cell_data, always_success_script) =
        always_success_cell();
    let always_success_tx = TransactionBuilder::default()
        .input(CellInput::new(OutPoint::null(), 0))
        .output(always_success_cell.clone())
        .output_data(always_success_cell_data.pack())
        .witness(always_success_script.clone().into_witness())
        .build();
    let always_success_out_point = OutPoint::new(always_success_tx.hash(), 0);

    let (shared, mut pack) = {
        let genesis = BlockBuilder::default()
            .timestamp(unix_time_as_millis().pack())
            .compact_target(difficulty_to_compact(U256::from(1000u64)).pack())
            .transaction(always_success_tx)
            .build();
        let epoch_ext = build_genesis_epoch_ext(
            Capacity::shannons(191_780_821_917_808),
            0x20800000,
            // genesis epoch length change to 1
            1,
            4 * 60 * 60,
            (1, 40),
        );
        let consensus = ConsensusBuilder::new(genesis, epoch_ext)
            .cellbase_maturity(EpochNumberWithFraction::new(0, 0, 1))
            .build();
        SharedBuilder::with_temp_db()
            .consensus(consensus)
            .build()
            .unwrap()
    };

    let network = dummy_network(&shared);
    pack.take_tx_pool_builder().start(network);

    let chain_controller = {
        let chain_service = ChainService::new(shared.clone(), pack.take_proposal_table());
        chain_service.start::<&str>(None)
    };

    // Build 1 ~ (tip-1) heights
    for i in 0..tip {
        let parent = shared
            .store()
            .get_block_hash(i)
            .and_then(|block_hash| shared.store().get_block(&block_hash))
            .unwrap();
        let cellbase = TransactionBuilder::default()
            .input(CellInput::new_cellbase_input(parent.header().number() + 1))
            .output(
                CellOutputBuilder::default()
                    .capacity(capacity_bytes!(50000).pack())
                    .lock(always_success_script.to_owned())
                    .build(),
            )
            .output_data(Bytes::new().pack())
            .witness(Script::default().into_witness())
            .build();
        let header = new_header_builder(&shared, &parent.header()).build();
        let block = BlockBuilder::default()
            .header(header)
            .transaction(cellbase)
            .build();
        chain_controller
            .internal_process_block(Arc::new(block), Switch::DISABLE_ALL)
            .expect("processing block should be ok");
    }

    let sync_shared = Arc::new(SyncShared::new(
        shared,
        Default::default(),
        pack.take_relay_tx_receiver(),
    ));
    (
        Relayer::new(chain_controller, sync_shared),
        always_success_out_point,
    )
}

pub(crate) struct MockProtocolContext {
    protocol: SupportProtocols,
    sent_messages: RefCell<Vec<(ProtocolId, PeerIndex, P2pBytes)>>,
}

// test mock context with single thread
unsafe impl Send for MockProtocolContext {}
unsafe impl Sync for MockProtocolContext {}

impl MockProtocolContext {
    pub(crate) fn new(protocol: SupportProtocols) -> Self {
        Self {
            protocol,
            sent_messages: Default::default(),
        }
    }

    pub(crate) fn has_sent(
        &self,
        protocol_id: ProtocolId,
        peer_index: PeerIndex,
        data: P2pBytes,
    ) -> bool {
        self.sent_messages
            .borrow()
            .contains(&(protocol_id, peer_index, data))
    }
}

#[async_trait]
impl CKBProtocolContext for MockProtocolContext {
    async fn set_notify(&self, _interval: Duration, _token: u64) -> Result<(), Error> {
        unimplemented!()
    }
    async fn remove_notify(&self, _token: u64) -> Result<(), Error> {
        unimplemented!()
    }
    async fn async_quick_send_message(
        &self,
        _proto_id: ProtocolId,
        _peer_index: PeerIndex,
        _data: P2pBytes,
    ) -> Result<(), Error> {
        unimplemented!();
    }
    async fn async_quick_send_message_to(
        &self,
        _peer_index: PeerIndex,
        _data: P2pBytes,
    ) -> Result<(), Error> {
        unimplemented!();
    }
    async fn async_quick_filter_broadcast(
        &self,
        _target: TargetSession,
        _data: P2pBytes,
    ) -> Result<(), Error> {
        unimplemented!();
    }
    async fn async_future_task(
        &self,
        _task: Pin<Box<dyn Future<Output = ()> + 'static + Send>>,
        _blocking: bool,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn async_send_message(
        &self,
        proto_id: ProtocolId,
        peer_index: PeerIndex,
        data: P2pBytes,
    ) -> Result<(), Error> {
        self.sent_messages
            .borrow_mut()
            .push((proto_id, peer_index, data));
        Ok(())
    }
    async fn async_send_message_to(
        &self,
        peer_index: PeerIndex,
        data: P2pBytes,
    ) -> Result<(), Error> {
        let protocol_id = self.protocol_id();
        self.send_message(protocol_id, peer_index, data)
    }

    async fn async_filter_broadcast(
        &self,
        _target: TargetSession,
        _data: P2pBytes,
    ) -> Result<(), Error> {
        unimplemented!();
    }
    async fn async_disconnect(&self, _peer_index: PeerIndex, _message: &str) -> Result<(), Error> {
        unimplemented!();
    }
    fn quick_send_message(
        &self,
        _proto_id: ProtocolId,
        _peer_index: PeerIndex,
        _data: P2pBytes,
    ) -> Result<(), Error> {
        unimplemented!();
    }
    fn quick_send_message_to(&self, _peer_index: PeerIndex, _data: P2pBytes) -> Result<(), Error> {
        unimplemented!();
    }
    fn quick_filter_broadcast(&self, _target: TargetSession, _data: P2pBytes) -> Result<(), Error> {
        unimplemented!();
    }
    fn future_task(
        &self,
        _task: Pin<Box<dyn Future<Output = ()> + 'static + Send>>,
        _blocking: bool,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn send_message(
        &self,
        proto_id: ProtocolId,
        peer_index: PeerIndex,
        data: P2pBytes,
    ) -> Result<(), Error> {
        self.sent_messages
            .borrow_mut()
            .push((proto_id, peer_index, data));
        Ok(())
    }
    fn send_message_to(&self, peer_index: PeerIndex, data: P2pBytes) -> Result<(), Error> {
        let protocol_id = self.protocol_id();
        self.send_message(protocol_id, peer_index, data)
    }

    fn filter_broadcast(&self, _target: TargetSession, _data: P2pBytes) -> Result<(), Error> {
        unimplemented!();
    }
    fn disconnect(&self, _peer_index: PeerIndex, _message: &str) -> Result<(), Error> {
        unimplemented!();
    }
    fn get_peer(&self, _peer_index: PeerIndex) -> Option<Peer> {
        unimplemented!();
    }
    fn with_peer_mut(&self, _peer_index: PeerIndex, _f: Box<dyn FnOnce(&mut Peer)>) {
        unimplemented!();
    }
    fn connected_peers(&self) -> Vec<PeerIndex> {
        unimplemented!();
    }
    fn report_peer(&self, _peer_index: PeerIndex, _behaviour: Behaviour) {
        unimplemented!();
    }
    fn ban_peer(&self, _peer_index: PeerIndex, _duration: Duration, _reason: String) {
        unimplemented!();
    }
    fn protocol_id(&self) -> ProtocolId {
        self.protocol.protocol_id()
    }
}
