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
use ckb_error::Error as CKBError;
use ckb_logger::{debug, error, info, trace, warn};
use ckb_metrics::metrics;
use ckb_network::{
    async_trait, bytes::Bytes, tokio, CKBProtocolContext, CKBProtocolHandler, PeerIndex,
    ServiceControl, SupportProtocols,
};
use ckb_systemtime::unix_time_as_millis;
use ckb_types::{
    core::{self, BlockNumber},
    packed::{self, Byte32},
    prelude::*,
};
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

    fn try_process(
        &self,
        nc: &dyn CKBProtocolContext,
        peer: PeerIndex,
        message: packed::SyncMessageUnionReader<'_>,
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

    fn process(
        &self,
        nc: &dyn CKBProtocolContext,
        peer: PeerIndex,
        message: packed::SyncMessageUnionReader<'_>,
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

            // On ibd, node should only have one peer to sync headers, and it's state can control by
            // headers_sync_controller.
            //
            // The header sync of other nodes does not matter in the ibd phase, and parallel synchronization
            // can be enabled by unknown list, so there is no need to repeatedly download headers with
            // multiple nodes at the same time.
            if active_chain.is_initial_block_download() {
                continue;
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

#[test]
fn test_molecule_block() {
    let src = "030000001b0c000008000000130c000014000000e4000000e8000000e70b00000000000044860919d828bcbb8601000026a38e0000000000321c00c40364040083df1e3d52bfd3892be46cef458f07b310bdd820fd68f804562dcd95f87611bca9285961200df42163fc3fd633e3a415e3642909d8fefc5df6f2c960bfaa1fb286aad1f57b0c12b181245b942411e19e561ccd4a6ea8c71c40a9565fa96c38330000000000000000000000000000000000000000000000000000000000000000680dbd2f768efd4774d2e244bd782700e73101dd5e82ce0400eb0ef7c8192f070006001c030000008de6840db9bada7e04000000ff0a0000100000006f010000a00900005f0100000c000000d9000000cd0000001c00000020000000240000002800000058000000c10000000000000000000000000000000100000026a38e00000000000000000000000000000000000000000000000000000000000000000000000000ffffffff69000000080000006100000010000000180000006100000002735a0529000000490000001000000030000000310000009bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce801140000007164f48d7a5bf2298166f8d81b81ea4e908e16ad0c000000080000000000000086000000080000007a0000007a0000000c00000055000000490000001000000030000000310000009bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce801140000008805eaf629140c223ece7e2ad8e2a01acb8695f9210000000000000020302e3130352e3120286530646433353720323032322d31302d333129310800000c0000000d040000010400001c00000020000000dd000000e1000000690100002703000000000000050000009f8e73e096f1583696760281004d71dc0cebd3c9aa6fb584949facde6e543e670000000000625696834db4320214a8af09de74fd51fc8a83be69d920243f8ccd219071473b0000000000d891e0e8c4864c730cc4b96b450ed1bcaf42e74c9eb1a38fb193caaf490f9a3d00000000002e46a10a67987594d4eaee2d5f9ac96ce651f7bfb44e82c286a12a1950ad4f29000000000071a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c000000000100000000030000004d18076400000040c1865e925da07f269a18498139f6fba44ba7b2c1c1a12ab0008c3ae7e688d717000000000000000000000000c1865e925da07f269a18498139f6fba44ba7b2c1c1a12ab0008c3ae7e688d717010000000000000000000000c1865e925da07f269a18498139f6fba44ba7b2c1c1a12ab0008c3ae7e688d71702000000be01000010000000c80000005d010000b8000000100000001800000063000000007e6d67070000004b000000100000003000000031000000a4398768d87bd17aea1361edc3accd6a0117774dc4ebc813bfa173e8ac0d086d01160000000075c62406c6b180d8ffe96400d7f08e6e89d186dc0055000000100000003000000031000000fef1d086d9f74d143c60bf03bd04bab29200dbf484c801c72774f2056d4c67180120000000ab21bfe2bf85927bb42faaf3006a355222e24d5ea1d4dec0e62f53a8e0c0469095000000100000001800000095000000005847f80d0000007d000000100000003000000031000000b619184ab9142c51b0ee75f4e24bcec3d077eefe513115bad68836d06738fd2c01480000001ca35cb5fda4bd542e71d94a6d5f4c0d255d6d6fba73c41cf45d2693e59b307280d6acef972db4688408a5e196f92eb0457ca003d9aa1c45ff05056b429b35393cc6060000000000610000001000000018000000610000007389999065000000490000001000000030000000310000009bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8011400000075c62406c6b180d8ffe96400d7f08e6e89d186dcda00000010000000d2000000d6000000be000000d6eea74bbbde42287e71e5831f6869d06c3f9711af2f5001546e71f808e69a0d68cc8aaa44b81583ee839dc1a93830773e4224c0b8ba74905ccb5e687f0e95cbab2d0000ad88b6554a5e258d7d8335ec7aaf6fa64fc7b50c6d59b7faec8b943ea16144083dc60600000000000000000000000000000000000000000000000000000000000000000000000000d33f810bdac50e7f216a1ad18ec2b73f199f8c463ffb4d957c30676ac43a97d8f5eab6bb860100009c8406000000000000010000000000000000240400001000000072030000cb0300005e0300005e0300001000000069000000690000005500000055000000100000005500000055000000410000008e7e938c9b12cabd53d78492ce410449002d98f389575024a2fe4237f61064ae15ae764b949583abf8b9c8884421e92c10630243bd50809a545092fdeae86fa600f102000000000000ed02000010000000e5020000e9020000d50200001c0000006c010000700100007401000078010000d1020000500100002c000000340000005400000074000000940000009c000000c0000000e4000000e80000000c0100003cc60600000000001c0000000200000014000000b8cde090e6a4741b6450308fad1dc338c53936a0a8a1a7c027ed33ed0a9e5a43772a8a769d6381a68a79731e46fb3ea1d98417d580d6acef972db4688408a5e196f92eb0457ca003d9aa1c45ff05056b429b3539f5eab6bb8601000068cc8aaa44b81583ee839dc1a93830773e4224c0b8ba74905ccb5e687f0e95cbab2d000068cc8aaa44b81583ee839dc1a93830773e4224c0b8ba74905ccb5e687f0e95cbab2d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007598802d05dd65a77701ad9f8f1be52693553d06df4cee827bbcc5732ce02a4d000000000000000004000000550100004c4f025012d2c56d99cbf95ea9c72aa34be18a25322c52564f81e4580031f7c475d52f0a505091dc647c23a15788dbdf0fa785c06b0dd4d662a9ed44240864716b08e70cbd50c91d14954de895bee868b1121b1d3eef98f8e05892ecb85a354fcfd0563fd515502421eb229fc3f91701e841a069c4a7c6abbfc4184abc7a6abc762135b30846f44f03508f7f8118faf95ea5eeb6ae9c5ad2f33990d8bf43ba315356f55ca96e84ec8e8150a2d58157017875838ea0e5430315ce0cae042365c6e3791b9c0df8de1c96e46e4f0350920015663226267cf29162a0249434af83458ca54b66d762a39c710903f1be925069eb12aceacf817b9b37db719001ad3b361a06c56eda41adbb7200e917dbc8ec4f01509982be151557e660c3a8248362cb433e547eb02545b82c0163afd797fa5c7f2f5081da9391d91a6ed38f5fa7e6b1a6857735ea8cabea89f6166d37b1d1d0fe4d094fed04000000000000000000000055000000550000001000000055000000550000004100000091165805c5d5e9e83e8604bd5e1900a99e643faae2f02e04ad079db286a294477ee915cceef774446f8972556d383a39a31afbc65c806d728fcc8bbbd73ad9bf0055000000550000001000000055000000550000004100000091165805c5d5e9e83e8604bd5e1900a99e643faae2f02e04ad079db286a294477ee915cceef774446f8972556d383a39a31afbc65c806d728fcc8bbbd73ad9bf005f0100000c000000fe000000f20000001c00000020000000490000004d0000007d000000e6000000000000000100000071a7ba8fc96349fea0ed3a5c47992e3b4084b031a42264a018e0072e8172e46c000000000100000000010000000000000000000000e8f5c3ec2cdbc1e7c67bdf4b409121e5b2c33ef53a547b4b89896a6c7dd5feb10000000069000000080000006100000010000000180000006100000007a72872df060000490000001000000030000000310000009bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce80114000000841e8ed59d20ebef7c5db7b6d7361e1e2b59cea20c00000008000000000000006100000008000000550000005500000010000000550000005500000041000000674131722b441201cf483a4bd3f32d212b4c5d733c04f1c24bea7fc15799341c464c46f3070057a361941fb86513b46d34f5307d511a3f1c0f100536b1eecc71000400000007f91cff9faca18bbf14a2f8089a02ae7239a4f989227cf176bd561794088dc92cede12acdff24c3";

    let mut data: Vec<u8> = vec![0_u8; src.len() / 2];
    faster_hex::hex_decode(src.as_bytes(), &mut data).unwrap();
    assert_ne!(data.len(), 0);

    let msg = match packed::SyncMessageReader::from_compatible_slice(&data) {
        Ok(msg) => {
            let item = msg.to_enum();
            if let packed::SyncMessageUnionReader::SendBlock(ref reader) = item {
                if reader.count_extra_fields() > 1 {
                    panic!("mailformed");
                    return;
                } else {
                    item
                }
            } else {
                match packed::SyncMessageReader::from_slice(&data) {
                    Ok(msg) => msg.to_enum(),
                    _ => {
                        panic!("mailformed");
                        return;
                    }
                }
            }
        }
        Err(e) => {
            panic!("mailformed, {:?}", e);
            return;
        }
    };

    dbg!("received msg {}", msg.item_name());
    dbg!(msg);
}

#[async_trait]
impl CKBProtocolHandler for Synchronizer {
    async fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        // NOTE: 100ms is what bitcoin use.
        nc.set_notify(SYNC_NOTIFY_INTERVAL, SEND_GET_HEADERS_TOKEN)
            .await
            .expect("set_notify at init is ok");
        nc.set_notify(SYNC_NOTIFY_INTERVAL, TIMEOUT_EVICTION_TOKEN)
            .await
            .expect("set_notify at init is ok");
        nc.set_notify(IBD_BLOCK_FETCH_INTERVAL, IBD_BLOCK_FETCH_TOKEN)
            .await
            .expect("set_notify at init is ok");
        nc.set_notify(NOT_IBD_BLOCK_FETCH_INTERVAL, NOT_IBD_BLOCK_FETCH_TOKEN)
            .await
            .expect("set_notify at init is ok");
        nc.set_notify(Duration::from_secs(2), NO_PEER_CHECK_TOKEN)
            .await
            .expect("set_notify at init is ok");
    }

    async fn received(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
        data: Bytes,
    ) {
        let msg = match packed::SyncMessageReader::from_compatible_slice(&data) {
            Ok(msg) => {
                let item = msg.to_enum();
                if let packed::SyncMessageUnionReader::SendBlock(ref reader) = item {
                    if reader.count_extra_fields() > 1 {
                        info!(
                            "Peer {} sends us a malformed message: \
                             too many fields in SendBlock",
                            peer_index
                        );
                        nc.ban_peer(
                            peer_index,
                            BAD_MESSAGE_BAN_TIME,
                            String::from(
                                "send us a malformed message: \
                                 too many fields in SendBlock",
                            ),
                        );
                        return;
                    } else {
                        item
                    }
                } else {
                    match packed::SyncMessageReader::from_slice(&data) {
                        Ok(msg) => msg.to_enum(),
                        _ => {
                            info!(
                                "Peer {} sends us a malformed message: \
                                 too many fields",
                                peer_index
                            );
                            nc.ban_peer(
                                peer_index,
                                BAD_MESSAGE_BAN_TIME,
                                String::from(
                                    "send us a malformed message: \
                                     too many fields",
                                ),
                            );
                            return;
                        }
                    }
                }
            }
            _ => {
                info!("Peer {} sends us a malformed message", peer_index);
                nc.ban_peer(
                    peer_index,
                    BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed message"),
                );
                return;
            }
        };
        if msg.item_name().eq("SendBlock") {
            info!("{}", hex_string(&data));
        }

        debug!("received msg {} from {}", msg.item_name(), peer_index);
        #[cfg(feature = "with_sentry")]
        {
            let sentry_hub = sentry::Hub::current();
            let _scope_guard = sentry_hub.push_scope();
            sentry_hub.configure_scope(|scope| {
                scope.set_tag("p2p.protocol", "synchronizer");
                scope.set_tag("p2p.message", msg.item_name());
            });
        }

        let start_time = Instant::now();
        tokio::task::block_in_place(|| self.process(nc.as_ref(), peer_index, msg));
        debug!(
            "process message={}, peer={}, cost={:?}",
            msg.item_name(),
            peer_index,
            Instant::now().saturating_duration_since(start_time),
        );
    }

    async fn connected(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
        _version: &str,
    ) {
        info!("SyncProtocol.connected peer={}", peer_index);
        self.on_connected(nc.as_ref(), peer_index);
    }

    async fn disconnected(
        &mut self,
        _nc: Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
    ) {
        let sync_state = self.shared().state();
        sync_state.disconnected(peer_index);
    }

    async fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        if !self.peers().state.is_empty() {
            let start_time = Instant::now();
            trace!("start notify token={}", token);
            match token {
                SEND_GET_HEADERS_TOKEN => {
                    self.start_sync_headers(nc.as_ref());
                }
                IBD_BLOCK_FETCH_TOKEN => {
                    if self.shared.active_chain().is_initial_block_download() {
                        self.find_blocks_to_fetch(nc.as_ref(), IBDState::In);
                    } else {
                        {
                            self.shared.state().write_inflight_blocks().adjustment = false;
                        }
                        self.shared.state().peers().clear_unknown_list();
                        if nc.remove_notify(IBD_BLOCK_FETCH_TOKEN).await.is_err() {
                            trace!("remove ibd block fetch fail");
                        }
                    }
                }
                NOT_IBD_BLOCK_FETCH_TOKEN => {
                    if !self.shared.active_chain().is_initial_block_download() {
                        self.find_blocks_to_fetch(nc.as_ref(), IBDState::Out);
                    }
                }
                TIMEOUT_EVICTION_TOKEN => {
                    self.eviction(nc.as_ref());
                }
                // Here is just for NO_PEER_CHECK_TOKEN token, only handle it when there is no peer.
                _ => {}
            }

            trace!(
                "finished notify token={} cost={:?}",
                token,
                Instant::now().saturating_duration_since(start_time)
            );
        } else if token == NO_PEER_CHECK_TOKEN {
            debug!("no peers connected");
        }
    }
}
