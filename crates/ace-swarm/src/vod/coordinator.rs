//! Private VOD scheduling. Piece credits belong to the coordinator from assignment through
//! ordered emission; workers own sockets and never hold a caller sender.
use super::{verify_piece, PieceBuffer, VodError, VodInfo, VodTimeouts};
use ace_peer::session::{connect, PeerSession};
use ace_wire::{handshake::random_peer_id, message::PeerMessage};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, Notify},
    task::JoinSet,
    time::Instant,
};

const WINDOW: u64 = 4;
const REQUESTS: usize = 8;
const ATTEMPTS: u8 = 3;
const PIECE_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Default)]
struct Availability {
    unchoked: bool,
    bits: Option<Vec<u8>>,
}
impl Availability {
    fn has(&self, piece: u64) -> bool {
        self.bits
            .as_ref()
            .is_none_or(|bits| bits[piece as usize / 8] & (0x80 >> (piece % 8)) != 0)
    }
    fn control(&mut self, message: &PeerMessage, count: u64) -> Result<(), Failure> {
        match message {
            PeerMessage::Choke => self.unchoked = false,
            PeerMessage::Unchoke => self.unchoked = true,
            PeerMessage::Bitfield(bits) => {
                if bits.len() as u64 != count.div_ceil(8)
                    || (!count.is_multiple_of(8)
                        && bits
                            .last()
                            .is_some_and(|last| last & ((1 << (8 - count % 8)) - 1) != 0))
                {
                    return Err(Failure::Peer);
                }
                self.bits = Some(bits.clone());
            }
            PeerMessage::Have(index) => {
                if u64::from(*index) >= count {
                    return Err(Failure::Peer);
                }
                if let Some(bits) = &mut self.bits {
                    bits[*index as usize / 8] |= 0x80 >> (*index % 8);
                }
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct Job {
    piece: u64,
    generation: u64,
}
#[derive(Clone, Copy)]
enum Failure {
    Peer,
    Hash,
}
struct Completion {
    worker: u64,
    job: Job,
    bytes: Bytes,
}
struct Worker {
    address: usize,
    commands: mpsc::Sender<Job>,
    state: Arc<Mutex<Availability>>,
    job: Option<Job>,
}
struct Credit {
    attempts: Vec<u8>,
    owner: Option<u64>,
    bytes: Option<Bytes>,
}
impl Credit {
    fn new(peers: usize) -> Self {
        Self {
            attempts: vec![0; peers],
            owner: None,
            bytes: None,
        }
    }
}

pub(super) async fn download_vod_pieces_with_policy(
    info: VodInfo,
    peers: Vec<SocketAddrV4>,
    tx: mpsc::Sender<Bytes>,
    first: u64,
    end: u64,
    timeouts: VodTimeouts,
    concurrency: usize,
) -> Result<(), VodError> {
    let end = end.min(info.piece_count());
    if first >= end {
        return Ok(());
    }
    let mut unique = HashSet::new();
    let peers: Vec<_> = peers
        .into_iter()
        .filter(|peer| unique.insert(*peer))
        .collect();
    let info = Arc::new(info);
    let notify = Arc::new(Notify::new());
    let (results, mut completions) = mpsc::channel::<Completion>(4);
    let mut tasks = JoinSet::new();
    let mut workers = BTreeMap::<u64, Worker>::new();
    let mut banned = vec![false; peers.len()];
    let mut credits = BTreeMap::<u64, Credit>::new();
    let mut cursor = first;
    let mut generation = 0u64;
    let mut next_worker = 0u64;
    let mut round_robin = 0u64;
    let limit = concurrency.clamp(1, 4);
    let outcome = loop {
        if cursor == end {
            break Ok(());
        }
        if tx.is_closed() {
            break Err(VodError::ConsumerGone);
        }
        // Do not admit more work while caller capacity is unavailable. Bytes move, never clone.
        if credits
            .get(&cursor)
            .is_some_and(|credit| credit.bytes.is_some())
        {
            let bytes = credits.get_mut(&cursor).unwrap().bytes.take().unwrap();
            if tx.send(bytes).await.is_err() {
                break Err(VodError::ConsumerGone);
            }
            credits.remove(&cursor);
            cursor += 1;
            continue;
        }
        for piece in cursor..end.min(cursor.saturating_add(WINDOW)) {
            credits
                .entry(piece)
                .or_insert_with(|| Credit::new(peers.len()));
        }
        // Connect only addresses with remaining frontier budget. Idle/unavailable failures
        // consume that budget; later successful pieces never reset it.
        while workers.len() < limit {
            let frontier = &credits[&cursor];
            let address = (0..peers.len())
                .filter(|&address| {
                    !banned[address]
                        && frontier.attempts[address] < ATTEMPTS
                        && !workers.values().any(|worker| worker.address == address)
                })
                .min_by_key(|&address| frontier.attempts[address]);
            let Some(address) = address else {
                break;
            };
            let id = next_worker;
            next_worker += 1;
            let (commands, receiver) = mpsc::channel(1);
            let state = Arc::new(Mutex::new(Availability::default()));
            let worker_info = info.clone();
            let worker_state = state.clone();
            let worker_notify = notify.clone();
            let worker_results = results.clone();
            let addr = peers[address];
            tasks.spawn(async move {
                let result = run_worker(
                    WorkerContext {
                        id,
                        address: addr,
                        info: worker_info,
                        results: worker_results,
                        state: worker_state,
                        notify: worker_notify,
                        timeouts,
                    },
                    receiver,
                )
                .await;
                (id, result)
            });
            workers.insert(
                id,
                Worker {
                    address,
                    commands,
                    state,
                    job: None,
                },
            );
        }
        for (&piece, credit) in &mut credits {
            if credit.owner.is_some() || credit.bytes.is_some() {
                continue;
            }
            let eligible = workers
                .iter()
                .filter(|(_, worker)| {
                    let state = worker.state.lock().unwrap();
                    worker.job.is_none()
                        && state.unchoked
                        && state.has(piece)
                        && !banned[worker.address]
                        && credit.attempts[worker.address] < ATTEMPTS
                })
                .min_by_key(|(&id, worker)| (credit.attempts[worker.address], id < round_robin, id))
                .map(|(&id, _)| id);
            if let Some(id) = eligible {
                generation += 1;
                let job = Job { piece, generation };
                let worker = workers.get_mut(&id).unwrap();
                if worker.commands.try_send(job).is_ok() {
                    credit.attempts[worker.address] += 1;
                    credit.owner = Some(id);
                    worker.job = Some(job);
                    round_robin = id + 1;
                }
            }
        }
        if workers.is_empty() {
            break Err(VodError::Unrecoverable(cursor));
        }
        tokio::select! {
            biased;
            _ = tx.closed() => break Err(VodError::ConsumerGone),
            Some(completion) = completions.recv() => {
                if let Some(worker) = workers.get_mut(&completion.worker) {
                    if worker.job.is_some_and(|job| job.piece == completion.job.piece && job.generation == completion.job.generation) {
                        if let Some(credit) = credits.get_mut(&completion.job.piece) {
                            if credit.owner == Some(completion.worker) {
                                credit.owner = None;
                                credit.bytes = Some(completion.bytes);
                            }
                        }
                        worker.job = None;
                    }
                }
            }
            Some(joined) = tasks.join_next() => {
                match joined {
                    Ok((id, failure)) => {
                        if let Some(worker) = workers.remove(&id) {
                            if matches!(failure, Err(Failure::Hash)) { banned[worker.address] = true; }
                            if let Some(job) = worker.job {
                                if let Some(credit) = credits.get_mut(&job.piece) {
                                    if credit.owner == Some(id) { credit.owner = None; }
                                }
                            } else {
                                credits.get_mut(&cursor).unwrap().attempts[worker.address] += 1;
                            }
                        }
                    }
                    Err(_) => break Err(VodError::Unrecoverable(cursor)),
                }
            }
            _ = notify.notified() => {}
        }
    };
    // JoinSet also aborts on parent-future drop. Every explicit return joins socket owners.
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    outcome
}

struct WorkerContext {
    id: u64,
    address: SocketAddrV4,
    info: Arc<VodInfo>,
    results: mpsc::Sender<Completion>,
    state: Arc<Mutex<Availability>>,
    notify: Arc<Notify>,
    timeouts: VodTimeouts,
}

async fn run_worker(
    context: WorkerContext,
    mut commands: mpsc::Receiver<Job>,
) -> Result<(), Failure> {
    let WorkerContext {
        id,
        address,
        info,
        results,
        state,
        notify,
        timeouts,
    } = context;
    let mut session = tokio::time::timeout(timeouts.connect, connect(&address.to_string()))
        .await
        .map_err(|_| Failure::Peer)?
        .map_err(|_| Failure::Peer)?;
    tokio::time::timeout(
        timeouts.handshake,
        session.perform_handshake(info.infohash, random_peer_id()),
    )
    .await
    .map_err(|_| Failure::Peer)?
    .map_err(|_| Failure::Peer)?;
    write(&mut session, &PeerMessage::Interested, timeouts.write).await?;
    let mut idle_deadline = Instant::now() + timeouts.unchoke;
    loop {
        tokio::select! {
            biased;
            job = commands.recv() => {
                let Some(job) = job else { return Ok(()); };
                let bytes = fetch_piece(&mut session, &info, job.piece, &state, timeouts).await?;
                results.send(Completion { worker: id, job, bytes }).await.map_err(|_| Failure::Peer)?;
                idle_deadline = Instant::now() + timeouts.unchoke;
            }
            message = tokio::time::timeout_at(idle_deadline, session.read_message()) => {
                let message = message.map_err(|_| Failure::Peer)?.map_err(|_| Failure::Peer)?;
                state.lock().unwrap().control(&message, info.piece_count())?;
                notify.notify_one();
            }
        }
    }
}

async fn write<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    message: &PeerMessage,
    timeout: Duration,
) -> Result<(), Failure> {
    tokio::time::timeout(timeout, session.send(message))
        .await
        .map_err(|_| Failure::Peer)?
        .map_err(|_| Failure::Peer)
}

async fn fetch_piece<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    piece: u64,
    state: &Mutex<Availability>,
    timeouts: VodTimeouts,
) -> Result<Bytes, Failure> {
    tokio::time::timeout(
        PIECE_DEADLINE,
        assemble_piece(session, info, piece, state, timeouts),
    )
    .await
    .map_err(|_| Failure::Peer)?
}

async fn assemble_piece<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    piece: u64,
    state: &Mutex<Availability>,
    timeouts: VodTimeouts,
) -> Result<Bytes, Failure> {
    let len = info.piece_size(piece) as usize;
    let mut buffer = PieceBuffer::new(len);
    let mut sent = 0usize;
    let mut outstanding = Vec::<(usize, usize)>::with_capacity(REQUESTS);
    let mut deadline = Instant::now() + timeouts.block_progress;
    loop {
        let unchoked = state.lock().unwrap().unchoked;
        if unchoked {
            while outstanding.len() < REQUESTS && sent < len {
                let length = (info.chunk_length as usize).min(len - sent);
                write(
                    session,
                    &PeerMessage::Request {
                        index: piece as u32,
                        begin: sent as u32,
                        length: length as u32,
                    },
                    timeouts
                        .write
                        .min(deadline.saturating_duration_since(Instant::now())),
                )
                .await?;
                outstanding.push((sent, sent + length));
                sent += length;
            }
        }
        let message = tokio::time::timeout_at(deadline, session.read_message())
            .await
            .map_err(|_| Failure::Peer)?
            .map_err(|_| Failure::Peer)?;
        match &message {
            PeerMessage::Piece {
                index,
                begin,
                block,
            } if u64::from(*index) == piece => {
                let end = (*begin as usize)
                    .checked_add(block.len())
                    .ok_or(Failure::Peer)?;
                if end > sent {
                    return Err(Failure::Peer);
                }
                let progress = buffer.add_block(*begin, block).map_err(|_| Failure::Peer)?;
                outstanding.retain(|&(start, end)| !buffer.covers(start, end));
                if buffer.is_complete() {
                    if !verify_piece(&info.piece_hashes[piece as usize], &buffer.bytes) {
                        return Err(Failure::Hash);
                    }
                    return Ok(Bytes::from(buffer.bytes));
                }
                if progress {
                    deadline = Instant::now() + timeouts.block_progress;
                }
            }
            _ => state
                .lock()
                .unwrap()
                .control(&message, info.piece_count())?,
        }
    }
}

#[cfg(test)]
mod coordinator_tests;
