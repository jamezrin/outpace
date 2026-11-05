//! Desired support state, with no media or worker ownership.
use std::fmt;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SupportIntent {
    pub epoch: u64,
    pub running: bool,
}

pub struct SupportControl {
    pub(crate) key: (String, String),
    desired: watch::Sender<SupportIntent>,
}
impl fmt::Debug for SupportControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupportControl")
            .field("intent", &self.intent())
            .finish()
    }
}
impl SupportControl {
    pub(crate) fn new(key: (String, String)) -> Self {
        let (desired, _) = watch::channel(SupportIntent {
            epoch: 1,
            running: true,
        });
        Self { key, desired }
    }
    pub fn intent(&self) -> SupportIntent {
        *self.desired.borrow()
    }
    pub fn subscribe(&self) -> watch::Receiver<SupportIntent> {
        self.desired.subscribe()
    }
    pub(crate) fn restart_if_epoch(&self, expected: u64) -> SupportIntent {
        self.desired.send_if_modified(|intent| {
            if intent.epoch != expected || !intent.running {
                return false;
            }
            intent.epoch = intent
                .epoch
                .checked_add(1)
                .expect("support intent epoch overflow");
            true
        });
        self.intent()
    }
    pub(crate) fn set_running(&self, running: bool) -> SupportIntent {
        self.desired.send_if_modified(|intent| {
            if intent.running == running {
                return false;
            }
            intent.epoch = intent
                .epoch
                .checked_add(1)
                .expect("support intent epoch overflow");
            intent.running = running;
            true
        });
        self.intent()
    }
}

pub(crate) struct SupportAttempt {
    pub control: std::sync::Arc<SupportControl>,
    pub epoch: u64,
}
impl SupportAttempt {
    pub fn current(&self) -> Option<watch::Ref<'_, SupportIntent>> {
        let intent = self.control.desired.borrow();
        (intent.running && intent.epoch == self.epoch).then_some(intent)
    }
}
