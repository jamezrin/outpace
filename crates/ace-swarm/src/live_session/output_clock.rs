use super::*;

/// Playback timestamps never participate in socket or candidate arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EffectiveInstant(pub(super) Instant);
impl EffectiveInstant {
    pub(super) fn duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}
impl std::ops::Add<Duration> for EffectiveInstant {
    type Output = Self;
    fn add(self, duration: Duration) -> Self {
        Self(self.0 + duration)
    }
}
pub(super) struct OutputBudgetClock {
    epoch: Instant,
    excluded: Duration,
    active_since: Option<Instant>,
}
impl OutputBudgetClock {
    pub(super) fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            excluded: Duration::ZERO,
            active_since: None,
        }
    }
    pub(super) fn now(&self, real: Instant) -> EffectiveInstant {
        let excluded = self.excluded
            + self
                .active_since
                .map(|start| real.saturating_duration_since(start))
                .unwrap_or_default();
        EffectiveInstant(real.checked_sub(excluded).unwrap_or(self.epoch))
    }
    pub(super) fn begin(&mut self, real: Instant) {
        self.active_since.get_or_insert(real);
    }
    pub(super) fn end(&mut self, real: Instant) {
        if let Some(start) = self.active_since.take() {
            self.excluded += real.saturating_duration_since(start);
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FirstOutput {
    AwaitCorroboration,
    Armed(EffectiveInstant),
    Done,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum OutputBudget {
    Suspended,
    Remaining(Duration),
    Expired,
}
impl Continuity {
    /// Suspended output expiry contributes no timer to the real wait.
    pub(super) fn output_budget(&mut self, real: Instant) -> OutputBudget {
        let timeout = self.live_recovery.stale_upstream_timeout();
        let now = self.output_clock.now(real);
        if self.first_output == FirstOutput::AwaitCorroboration && self.startup_deadline.is_none() {
            self.first_output = FirstOutput::Armed(now + timeout);
        }
        if self.output_clock.active_since.is_some()
            || self.first_output == FirstOutput::AwaitCorroboration
        {
            return OutputBudget::Suspended;
        }
        let remaining = match self.first_output {
            FirstOutput::Armed(deadline) => deadline.duration_since(now),
            FirstOutput::Done => timeout.saturating_sub(now.duration_since(self.last_progress)),
            FirstOutput::AwaitCorroboration => unreachable!(),
        };
        if remaining.is_zero() {
            OutputBudget::Expired
        } else {
            OutputBudget::Remaining(remaining)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preconsumed_and_repeated_holds_exclude_active_interval_once() {
        let base = Instant::now();
        let mut clock = OutputBudgetClock::new(base);
        let progress = clock.now(base);
        clock.begin(base + Duration::from_secs(3));
        clock.begin(base + Duration::from_secs(5));
        assert_eq!(
            clock
                .now(base + Duration::from_secs(100))
                .duration_since(progress),
            Duration::from_secs(3)
        );
        clock.end(base + Duration::from_secs(100));
        clock.end(base + Duration::from_secs(101));
        assert_eq!(
            clock
                .now(base + Duration::from_secs(102))
                .duration_since(progress),
            Duration::from_secs(5)
        );
        clock.begin(base + Duration::from_secs(103));
        clock.end(base + Duration::from_secs(200));
        assert_eq!(
            clock
                .now(base + Duration::from_secs(201))
                .duration_since(progress),
            Duration::from_secs(7)
        );
    }
    #[test]
    fn confirmation_and_expiry_arm_once_while_held_withheld_is_not_done() {
        for expiry in [false, true] {
            let info = StreamInfo {
                infohash: [0; 20],
                piece_length: 4,
                chunk_length: 2,
                trackers: vec![],
                metadata: Default::default(),
                sig_len: 0,
                source_pubkey: vec![],
            };
            let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default());
            let real = Instant::now();
            c.startup_deadline = Some(real + Duration::from_secs(1));
            c.output_clock.begin(real);
            assert_eq!(c.output_budget(real), OutputBudget::Suspended);
            assert_eq!(c.first_output, FirstOutput::AwaitCorroboration);
            // Both real expiry and independent confirmation clear this same state.
            let clear = real + Duration::from_secs(if expiry { 2 } else { 0 });
            c.startup_deadline = None;
            c.output_budget(clear);
            let armed = c.first_output;
            c.output_budget(real + Duration::from_secs(500));
            assert_eq!(c.first_output, armed);
            c.output_clock.end(real + Duration::from_secs(500));
            assert_eq!(
                c.output_budget(real + Duration::from_secs(500)),
                OutputBudget::Remaining(c.live_recovery.stale_upstream_timeout())
            );
            assert_eq!(
                c.output_budget(
                    real + Duration::from_secs(500) + c.live_recovery.stale_upstream_timeout()
                ),
                OutputBudget::Expired
            );
            assert_eq!(c.first_output, armed);
        }
    }
}
