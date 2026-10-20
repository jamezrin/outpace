use super::*;

pub type PeerDiscovery = Arc<
    dyn Fn(DiscoveryOptions, mpsc::Sender<SocketAddrV4>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

pub struct DiscoveryRun {
    pub(super) receiver: mpsc::Receiver<SocketAddrV4>,
    _worker: OwnedTask,
}
impl DiscoveryRun {
    pub async fn next_candidate(&mut self) -> Option<SocketAddrV4> {
        self.receiver.recv().await
    }
    pub fn try_next_candidate(&mut self) -> Result<SocketAddrV4, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
    pub fn start(discovery: &PeerDiscovery, options: DiscoveryOptions) -> Self {
        let (sender, receiver) = mpsc::channel(MAX_DISCOVERY_PEERS);
        Self {
            receiver,
            _worker: OwnedTask(tokio::spawn(discovery(options, sender))),
        }
    }
}
