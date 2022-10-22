mod block_fetcher;
mod block_process;
mod get_blocks_process;
mod get_headers_process;
mod headers_process;
mod in_ibd_process;

pub(crate) use self::block_fetcher::BlockFetcher;
pub(crate) use self::block_process::BlockProcess;
pub(crate) use self::get_blocks_process::GetBlocksProcess;
pub(crate) use self::get_headers_process::GetHeadersProcess;
pub(crate) use self::headers_process::HeadersProcess;
pub(crate) use self::in_ibd_process::InIBDProcess;

use crate::block_status::BlockStatus;
use crate::types::{HeaderView, HeadersSyncController, IBDState, Peers, SyncShared};
use crate::utils::send_message_to;
use crate::{Status, StatusCode};

use ckb_chain::chain::ChainController;
use ckb_channel as channel;
use ckb_constant::sync::{
    BAD_MESSAGE_BAN_TIME, CHAIN_SYNC_TIMEOUT, EVICTION_HEADERS_RESPONSE_TIME,
    INIT_BLOCKS_IN_TRANSIT_PER_PEER, MAX_TIP_AGE,
};
use ckb_db::RocksDB;
use ckb_db_schema::COLUMNS;
use ckb_error::Error as CKBError;
use ckb_logger::{debug, error, info, trace, warn};
use ckb_metrics::metrics;
use ckb_network::{
    async_trait, bytes::Bytes, tokio, CKBProtocolContext, CKBProtocolHandler, PeerId, PeerIndex,
    ServiceControl, SupportProtocols,
};
use ckb_store::{ChainDB, ChainStore};
use ckb_types::packed::{Header, HeaderVec, SendBlock};
use ckb_types::{
    core::{self, BlockNumber},
    packed::{self, Byte32},
    prelude::*,
};
use crossbeam_queue::ArrayQueue;
use faketime::unix_time_as_millis;
use iter_tools::Itertools;
use std::rc::Rc;
use std::{
    collections::HashSet,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

pub const SEND_GET_HEADERS_TOKEN: u64 = 0;
pub const IBD_BLOCK_FETCH_TOKEN: u64 = 1;
pub const NOT_IBD_BLOCK_FETCH_TOKEN: u64 = 2;
pub const TIMEOUT_EVICTION_TOKEN: u64 = 3;
pub const NO_PEER_CHECK_TOKEN: u64 = 255;

const SYNC_NOTIFY_INTERVAL: Duration = Duration::from_secs(1);
const IBD_BLOCK_FETCH_INTERVAL: Duration = Duration::from_millis(40);
const NOT_IBD_BLOCK_FETCH_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Copy, Clone)]
enum CanStart {
    Ready,
    MinWorkNotReach,
    AssumeValidNotFound,
}

enum FetchCMD {
    Fetch((Vec<PeerIndex>, IBDState)),
}

struct BlockFetchCMD {
    sync: Synchronizer,
    p2p_control: ServiceControl,
    recv: channel::Receiver<FetchCMD>,
    can_start: CanStart,
    number: BlockNumber,
}

impl BlockFetchCMD {
    fn run(&mut self) {
        while let Ok(cmd) = self.recv.recv() {
            match cmd {
                FetchCMD::Fetch((peers, state)) => match self.can_start() {
                    CanStart::Ready => {
                        for peer in peers {
                            if let Some(fetch) = BlockFetcher::new(&self.sync, peer, state).fetch()
                            {
                                for item in fetch {
                                    BlockFetchCMD::send_getblocks(item, &self.p2p_control, peer);
                                }
                            }
                        }
                    }
                    CanStart::MinWorkNotReach => {
                        let best_known = self.sync.shared.state().shared_best_header_ref();
                        let number = best_known.number();
                        if number != self.number && (number - self.number) % 10000 == 0 {
                            self.number = number;
                            info!(
                                    "best known header number: {}, total difficulty: {:#x}, \
                                 require min header number on 500_000, min total difficulty: {:#x}, \
                                 then start to download block",
                                    number,
                                    best_known.total_difficulty(),
                                    self.sync.shared.state().min_chain_work()
                                );
                        }
                    }
                    CanStart::AssumeValidNotFound => {
                        let state = self.sync.shared.state();
                        let best_known = state.shared_best_header_ref();
                        let number = best_known.number();
                        let assume_valid_target: Byte32 = state
                            .assume_valid_target()
                            .as_ref()
                            .map(Pack::pack)
                            .expect("assume valid target must exist");

                        if number != self.number && (number - self.number) % 10000 == 0 {
                            self.number = number;
                            info!(
                                "best known header number: {}, hash: {:#?}, \
                                 can't find assume valid target temporarily, hash: {:#?} \
                                 please wait",
                                number,
                                best_known.hash(),
                                assume_valid_target
                            );
                        }
                    }
                },
            }
        }
    }

    fn can_start(&mut self) -> CanStart {
        if let CanStart::Ready = self.can_start {
            return self.can_start;
        }

        let state = self.sync.shared.state();

        let min_work_reach = |flag: &mut CanStart| {
            if state.min_chain_work_ready() {
                *flag = CanStart::AssumeValidNotFound;
            }
        };

        let assume_valid_target_find = |flag: &mut CanStart| {
            let mut assume_valid_target = state.assume_valid_target();
            if let Some(ref target) = *assume_valid_target {
                match state.header_map().get(&target.pack()) {
                    Some(header) => {
                        *flag = CanStart::Ready;
                        // Blocks that are no longer in the scope of ibd must be forced to verify
                        if unix_time_as_millis().saturating_sub(header.timestamp()) < MAX_TIP_AGE {
                            assume_valid_target.take();
                        }
                    }
                    None => {
                        // Best known already not in the scope of ibd, it means target is invalid
                        if unix_time_as_millis()
                            .saturating_sub(state.shared_best_header_ref().timestamp())
                            < MAX_TIP_AGE
                        {
                            *flag = CanStart::Ready;
                            assume_valid_target.take();
                        }
                    }
                }
            } else {
                *flag = CanStart::Ready;
            }
        };

        match self.can_start {
            CanStart::Ready => self.can_start,
            CanStart::MinWorkNotReach => {
                min_work_reach(&mut self.can_start);
                if let CanStart::AssumeValidNotFound = self.can_start {
                    assume_valid_target_find(&mut self.can_start);
                }
                self.can_start
            }
            CanStart::AssumeValidNotFound => {
                assume_valid_target_find(&mut self.can_start);
                self.can_start
            }
        }
    }

    fn send_getblocks(v_fetch: Vec<packed::Byte32>, nc: &ServiceControl, peer: PeerIndex) {
        let content = packed::GetBlocks::new_builder()
            .block_hashes(v_fetch.clone().pack())
            .build();
        let message = packed::SyncMessage::new_builder().set(content).build();

        debug!("send_getblocks len={:?} to peer={}", v_fetch.len(), peer);
        if let Err(err) = nc.send_message_to(
            peer,
            SupportProtocols::Sync.protocol_id(),
            message.as_bytes(),
        ) {
            debug!("synchronizer send GetBlocks error: {:?}", err);
        }
    }
}

/// Sync protocol handle
#[derive(Clone)]
pub struct Synchronizer {
    pub(crate) chain: ChainController,
    /// Sync shared state
    pub shared: Arc<SyncShared>,
    fetch_channel: Option<channel::Sender<FetchCMD>>,
}

impl Synchronizer {
    /// Init sync protocol handle
    ///
    /// This is a runtime sync protocol shared state, and any relay messages will be processed and forwarded by it
    pub fn new(chain: ChainController, shared: Arc<SyncShared>) -> Synchronizer {
        Synchronizer {
            chain,
            shared,
            fetch_channel: None,
        }
    }

    /// Get shared state
    pub fn shared(&self) -> &Arc<SyncShared> {
        &self.shared
    }

    fn try_process<'r>(
        &self,
        nc: &dyn CKBProtocolContext,
        peer: PeerIndex,
        message: packed::SyncMessageUnionReader<'r>,
    ) -> Status {
        match message {
            packed::SyncMessageUnionReader::GetHeaders(reader) => {
                GetHeadersProcess::new(reader, self, peer, nc).execute()
            }
            packed::SyncMessageUnionReader::SendHeaders(reader) => {
                HeadersProcess::new(reader, self, peer, nc).execute()
            }
            packed::SyncMessageUnionReader::GetBlocks(reader) => {
                GetBlocksProcess::new(reader, self, peer, nc).execute()
            }
            packed::SyncMessageUnionReader::SendBlock(reader) => {
                if reader.check_data() {
                    BlockProcess::new(reader, self, peer).execute()
                } else {
                    StatusCode::ProtocolMessageIsMalformed.with_context("SendBlock is invalid")
                }
            }
            packed::SyncMessageUnionReader::InIBD(_) => InIBDProcess::new(self, peer, nc).execute(),
            _ => StatusCode::ProtocolMessageIsMalformed.with_context("unexpected sync message"),
        }
    }

    fn process<'r>(
        &self,
        nc: &dyn CKBProtocolContext,
        peer: PeerIndex,
        message: packed::SyncMessageUnionReader<'r>,
    ) {
        let item_name = message.item_name();
        let item_bytes = message.as_slice().len() as u64;
        let status = self.try_process(nc, peer, message);

        metrics!(
            counter,
            "ckb.messages_bytes",
            item_bytes,
            "direction" => "in",
            "protocol_id" => SupportProtocols::Sync.protocol_id().value().to_string(),
            "item_id" => message.item_id().to_string(),
            "status" => (status.code() as u16).to_string(),
        );

        if let Some(ban_time) = status.should_ban() {
            error!(
                "receive {} from {}, ban {:?} for {}",
                item_name, peer, ban_time, status
            );
            nc.ban_peer(peer, ban_time, status.to_string());
        } else if status.should_warn() {
            warn!("receive {} from {}, {}", item_name, peer, status);
        } else if !status.is_ok() {
            debug!("receive {} from {}, {}", item_name, peer, status);
        }
    }

    /// Get peers info
    pub fn peers(&self) -> &Peers {
        self.shared().state().peers()
    }

    fn better_tip_header(&self) -> core::HeaderView {
        let (header, total_difficulty) = {
            let active_chain = self.shared.active_chain();
            (
                active_chain.tip_header(),
                active_chain.total_difficulty().to_owned(),
            )
        };
        let best_known = self.shared.state().shared_best_header();
        // is_better_chain
        if total_difficulty > *best_known.total_difficulty() {
            header
        } else {
            best_known.into_inner()
        }
    }

    /// Process a new block sync from other peer
    //TODO: process block which we don't request
    pub fn process_new_block(&self, block: core::BlockView) -> Result<bool, CKBError> {
        let block_hash = block.hash();
        let status = self.shared.active_chain().get_block_status(&block_hash);
        // NOTE: Filtering `BLOCK_STORED` but not `BLOCK_RECEIVED`, is for avoiding
        // stopping synchronization even when orphan_pool maintains dirty items by bugs.
        if status.contains(BlockStatus::BLOCK_STORED) {
            debug!("block {} already stored", block_hash);
            Ok(false)
        } else if status.contains(BlockStatus::HEADER_VALID) {
            self.shared.insert_new_block(&self.chain, Arc::new(block))
        } else {
            debug!(
                "Synchronizer process_new_block unexpected status {:?} {}",
                status, block_hash,
            );
            // TODO which error should we return?
            Ok(false)
        }
    }

    /// Get blocks to fetch
    pub fn get_blocks_to_fetch(
        &self,
        peer: PeerIndex,
        ibd: IBDState,
    ) -> Option<Vec<Vec<packed::Byte32>>> {
        BlockFetcher::new(self, peer, ibd).fetch()
    }

    pub(crate) fn on_connected(&self, nc: &dyn CKBProtocolContext, peer: PeerIndex) {
        let (is_outbound, is_whitelist) = nc
            .get_peer(peer)
            .map(|peer| (peer.is_outbound(), peer.is_whitelist))
            .unwrap_or((false, false));

        self.peers().sync_connected(peer, is_outbound, is_whitelist);
    }

    /// Regularly check and eject some nodes that do not respond in time
    //   - If at timeout their best known block now has more work than our tip
    //     when the timeout was set, then either reset the timeout or clear it
    //     (after comparing against our current tip's work)
    //   - If at timeout their best known block still has less work than our
    //     tip did when the timeout was set, then send a getheaders message,
    //     and set a shorter timeout, HEADERS_RESPONSE_TIME seconds in future.
    //     If their best known block is still behind when that new timeout is
    //     reached, disconnect.
    pub fn eviction(&self, nc: &dyn CKBProtocolContext) {
        let active_chain = self.shared.active_chain();
        let mut eviction = Vec::new();
        let better_tip_header = self.better_tip_header();
        for mut kv_pair in self.peers().state.iter_mut() {
            let (peer, state) = kv_pair.pair_mut();
            let now = unix_time_as_millis();

            if let Some(ref mut controller) = state.headers_sync_controller {
                let better_tip_ts = better_tip_header.timestamp();
                if let Some(is_timeout) = controller.is_timeout(better_tip_ts, now) {
                    if is_timeout {
                        eviction.push(*peer);
                        continue;
                    }
                } else {
                    active_chain.send_getheaders_to_peer(nc, *peer, &better_tip_header);
                }
            }

            if state.peer_flags.is_outbound {
                let best_known_header = state.best_known_header.as_ref();
                let (tip_header, local_total_difficulty) = {
                    (
                        active_chain.tip_header().to_owned(),
                        active_chain.total_difficulty().to_owned(),
                    )
                };
                if best_known_header.map(HeaderView::total_difficulty)
                    >= Some(&local_total_difficulty)
                {
                    if state.chain_sync.timeout != 0 {
                        state.chain_sync.timeout = 0;
                        state.chain_sync.work_header = None;
                        state.chain_sync.total_difficulty = None;
                        state.chain_sync.sent_getheaders = false;
                    }
                } else if state.chain_sync.timeout == 0
                    || (best_known_header.is_some()
                        && best_known_header.map(HeaderView::total_difficulty)
                            >= state.chain_sync.total_difficulty.as_ref())
                {
                    // Our best block known by this peer is behind our tip, and we're either noticing
                    // that for the first time, OR this peer was able to catch up to some earlier point
                    // where we checked against our tip.
                    // Either way, set a new timeout based on current tip.
                    state.chain_sync.timeout = now + CHAIN_SYNC_TIMEOUT;
                    state.chain_sync.work_header = Some(tip_header);
                    state.chain_sync.total_difficulty = Some(local_total_difficulty);
                    state.chain_sync.sent_getheaders = false;
                } else if state.chain_sync.timeout > 0 && now > state.chain_sync.timeout {
                    // No evidence yet that our peer has synced to a chain with work equal to that
                    // of our tip, when we first detected it was behind. Send a single getheaders
                    // message to give the peer a chance to update us.
                    if state.chain_sync.sent_getheaders {
                        if state.peer_flags.is_protect || state.peer_flags.is_whitelist {
                            if state.sync_started() {
                                self.shared().state().suspend_sync(state);
                            }
                        } else {
                            eviction.push(*peer);
                        }
                    } else {
                        state.chain_sync.sent_getheaders = true;
                        state.chain_sync.timeout = now + EVICTION_HEADERS_RESPONSE_TIME;
                        active_chain.send_getheaders_to_peer(
                            nc,
                            *peer,
                            &state
                                .chain_sync
                                .work_header
                                .clone()
                                .expect("work_header be assigned"),
                        );
                    }
                }
            }
        }
        for peer in eviction {
            info!("timeout eviction peer={}", peer);
            if let Err(err) = nc.disconnect(peer, "sync timeout eviction") {
                debug!("synchronizer disconnect error: {:?}", err);
            }
        }
    }

    fn start_sync_headers(&self, nc: &dyn CKBProtocolContext) {
        let now = unix_time_as_millis();
        let active_chain = self.shared.active_chain();
        let ibd = active_chain.is_initial_block_download();
        let peers: Vec<PeerIndex> = self
            .peers()
            .state
            .iter()
            .filter(|kv_pair| kv_pair.value().can_start_sync(now, ibd))
            .map(|kv_pair| *kv_pair.key())
            .collect();

        if peers.is_empty() {
            return;
        }

        let tip = self.better_tip_header();

        for peer in peers {
            // Only sync with 1 peer if we're in IBD
            if self
                .shared()
                .state()
                .n_sync_started()
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                    if ibd && x != 0 {
                        None
                    } else {
                        Some(x + 1)
                    }
                })
                .is_err()
            {
                break;
            }
            {
                if let Some(mut peer_state) = self.peers().state.get_mut(&peer) {
                    peer_state.start_sync(HeadersSyncController::from_header(&tip));
                }
            }

            debug!("start sync peer={}", peer);
            active_chain.send_getheaders_to_peer(nc, peer, &tip);
        }
    }

    fn get_peers_to_fetch(
        &self,
        ibd: IBDState,
        disconnect_list: &HashSet<PeerIndex>,
    ) -> Vec<PeerIndex> {
        trace!("poll find_blocks_to_fetch select peers");
        let state = &self
            .shared
            .state()
            .read_inflight_blocks()
            .download_schedulers;
        let mut peers: Vec<PeerIndex> = self
            .peers()
            .state
            .iter()
            .filter(|kv_pair| {
                let (id, state) = kv_pair.pair();
                if disconnect_list.contains(id) {
                    return false;
                };
                match ibd {
                    IBDState::In => {
                        state.peer_flags.is_outbound
                            || state.peer_flags.is_whitelist
                            || state.peer_flags.is_protect
                    }
                    IBDState::Out => state.started_or_tip_synced(),
                }
            })
            .map(|kv_pair| *kv_pair.key())
            .collect();
        peers.sort_by_key(|id| {
            ::std::cmp::Reverse(
                state
                    .get(id)
                    .map_or(INIT_BLOCKS_IN_TRANSIT_PER_PEER, |d| d.task_count()),
            )
        });
        peers
    }

    fn find_blocks_to_fetch(&mut self, nc: &dyn CKBProtocolContext, ibd: IBDState) {
        let tip = self.shared.active_chain().tip_number();

        let disconnect_list = {
            let mut list = self.shared().state().write_inflight_blocks().prune(tip);
            if let IBDState::In = ibd {
                // best known < tip and in IBD state, and unknown list is empty,
                // these node can be disconnect
                list.extend(
                    self.shared
                        .state()
                        .peers()
                        .get_best_known_less_than_tip_and_unknown_empty(tip),
                )
            };
            list
        };

        for peer in disconnect_list.iter() {
            // It is not forbidden to evict protected nodes:
            // - First of all, this node is not designated by the user for protection,
            //   but is connected randomly. It does not represent the will of the user
            // - Secondly, in the synchronization phase, the nodes with zero download tasks are
            //   retained, apart from reducing the download efficiency, there is no benefit.
            if self
                .peers()
                .get_flag(*peer)
                .map(|flag| flag.is_whitelist)
                .unwrap_or(false)
            {
                continue;
            }
            if let Err(err) = nc.disconnect(*peer, "sync disconnect") {
                debug!("synchronizer disconnect error: {:?}", err);
            }
        }

        // fetch use a lot of cpu time, especially in ibd state
        // so, the fetch function use another thread
        match nc.p2p_control() {
            Some(raw) => match self.fetch_channel {
                Some(ref sender) => {
                    if !sender.is_full() {
                        let peers = self.get_peers_to_fetch(ibd, &disconnect_list);
                        let _ignore = sender.try_send(FetchCMD::Fetch((peers, ibd)));
                    }
                }
                None => {
                    let p2p_control = raw.clone();
                    let sync = self.clone();
                    let (sender, recv) = channel::bounded(2);
                    let peers = self.get_peers_to_fetch(ibd, &disconnect_list);
                    sender.send(FetchCMD::Fetch((peers, ibd))).unwrap();
                    self.fetch_channel = Some(sender);
                    let thread = ::std::thread::Builder::new();
                    let number = self.shared.state().shared_best_header_ref().number();
                    thread
                        .name("BlockDownload".to_string())
                        .spawn(move || {
                            BlockFetchCMD {
                                sync,
                                p2p_control,
                                recv,
                                number,
                                can_start: CanStart::MinWorkNotReach,
                            }
                            .run();
                        })
                        .expect("download thread can't start");
                }
            },
            None => {
                for peer in self.get_peers_to_fetch(ibd, &disconnect_list) {
                    if let Some(fetch) = self.get_blocks_to_fetch(peer, ibd) {
                        for item in fetch {
                            self.send_getblocks(item, nc, peer);
                        }
                    }
                }
            }
        }
    }

    fn send_getblocks(
        &self,
        v_fetch: Vec<packed::Byte32>,
        nc: &dyn CKBProtocolContext,
        peer: PeerIndex,
    ) {
        let content = packed::GetBlocks::new_builder()
            .block_hashes(v_fetch.clone().pack())
            .build();
        let message = packed::SyncMessage::new_builder().set(content).build();

        debug!("send_getblocks len={:?} to peer={}", v_fetch.len(), peer);
        let _status = send_message_to(nc, peer, &message);
    }
}

#[async_trait]
impl CKBProtocolHandler for Synchronizer {
    async fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        {
            let db = RocksDB::open_in(
                "/home/exec/Projects/github.com/nervosnetwork/chain/sync-base/data/db",
                COLUMNS,
            );
            let store = ChainDB::new(db, Default::default());

            let headers = ArrayQueue::new(4_000);
            let blocks = ArrayQueue::new(8_000_000);

            info!("debug, prepare headers and body queue");
            let mut header_vec: Vec<Header> = Vec::new();
            for height in 1..=8_000_000 {
                let block_numebr: BlockNumber = height as u64;
                let block_hash = store.get_block_hash(block_numebr).unwrap();
                let header = store.get_packed_block_header(&block_hash).unwrap();
                header_vec.push(header);

                if height % 2000 == 0 {
                    headers
                        .push(
                            HeaderVec::new_builder()
                                .extend(header_vec.clone().into_iter())
                                .build(),
                        )
                        .unwrap();
                    header_vec.clear();
                }

                let block = store.get_packed_block(&block_hash).unwrap();
                blocks.push(block).unwrap();
            }

            info!("debug, headers, and body queue is full");

            info!("debug, insert headers...");

            for header_chunk in headers.into_iter() {
                let send_headers = packed::SendHeaders::new_builder()
                    .headers(header_chunk)
                    .build();
                if !HeadersProcess::new(
                    send_headers.as_reader(),
                    self,
                    PeerIndex::new(1),
                    nc.as_ref(),
                )
                .execute()
                .is_ok()
                {
                    info!("debug, insert header not ok");
                }
            }
            info!("debug, headers insert complete");

            info!("debug, insert bodies...");
            let now = std::time::Instant::now();
            while !blocks.is_empty() {
                let block = blocks.pop().unwrap();
                if !BlockProcess::new(
                    SendBlock::new_builder().block(block).build().as_reader(),
                    self,
                    PeerIndex::new(1),
                )
                .execute()
                .is_ok()
                {
                    info!("debug, insert block not ok")
                }
            }

            info!(
                "debug, bodies insert complete, timecost: {}s",
                Instant::now().duration_since(now).as_secs()
            )
        }
    }

    async fn received(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
        data: Bytes,
    ) {
    }

    async fn connected(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
        _version: &str,
    ) {
    }

    async fn disconnected(
        &mut self,
        _nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
    ) {
    }

    async fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {}
}
