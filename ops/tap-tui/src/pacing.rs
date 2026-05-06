//! Reader-side pacing — the "slowdown" feature.
//!
//! The tap's educational posture is that **TCP flow control backpressures the
//! DBLog pump whenever the subscriber can't keep up**. We lean into that: the
//! reader thread calls [`Pacer::wait`] between events, and the UI thread can
//! adjust the delay or toggle step-mode at runtime. A slow reader → full
//! OS socket buffer → full tap queue → blocked `queue.put()` on the pump →
//! `stream.standby` emitted out-of-band. That's the whole mechanism.
//!
//! Two knobs, independently adjustable:
//!
//! 1. **Continuous delay** — `delay_ms` milliseconds slept between each
//!    `recv`+`send` pair. `0` means full speed. `n`/`m` at runtime steps by
//!    `STEP_MS`; `f` resets to no delay.
//! 2. **Step mode** — when `step_mode=true`, the reader blocks on a
//!    single-slot channel. The UI fires `advance()` on spacebar; each advance
//!    releases exactly one event. Toggle with spacebar (long press or
//!    re-enable via the `Step` key).
//!
//! Both are stored in atomics + a bounded channel, so the UI thread never
//! touches the reader's thread state directly.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{sync_channel, Receiver, SyncSender, TrySendError},
    Arc,
};
use std::time::Duration;

pub const STEP_MS: u64 = 100;

/// Reader-side gate: call [`wait`] between events.
pub struct Pacer {
    delay_ms: Arc<AtomicU64>,
    step_mode: Arc<AtomicBool>,
    step_rx: Receiver<()>,
}

/// UI-side handle: adjust delay, toggle step, fire advance.
#[derive(Clone)]
pub struct PaceHandle {
    delay_ms: Arc<AtomicU64>,
    step_mode: Arc<AtomicBool>,
    step_tx: SyncSender<()>,
}

/// Build a (handle, pacer) pair with initial `delay_ms` and step mode.
pub fn channel(initial_delay_ms: u64, initial_step: bool) -> (PaceHandle, Pacer) {
    // Bounded capacity 1 — at most one "advance" may be buffered. Multiple
    // presses during a blocked read coalesce, which is the intended UX.
    let (tx, rx) = sync_channel::<()>(1);
    let delay_ms = Arc::new(AtomicU64::new(initial_delay_ms));
    let step_mode = Arc::new(AtomicBool::new(initial_step));
    (
        PaceHandle {
            delay_ms: delay_ms.clone(),
            step_mode: step_mode.clone(),
            step_tx: tx,
        },
        Pacer {
            delay_ms,
            step_mode,
            step_rx: rx,
        },
    )
}

impl Pacer {
    /// Block according to the current pacing mode. Returns immediately on
    /// full-speed. Returns when the advance channel receives in step mode.
    pub fn wait(&self) {
        if self.step_mode.load(Ordering::Acquire) {
            // Drain any buffered advance first so one press = one event.
            let _ = self.step_rx.recv();
            return;
        }
        let ms = self.delay_ms.load(Ordering::Acquire);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

impl PaceHandle {
    pub fn delay_ms(&self) -> u64 {
        self.delay_ms.load(Ordering::Acquire)
    }
    pub fn step_mode(&self) -> bool {
        self.step_mode.load(Ordering::Acquire)
    }
    pub fn set_delay_ms(&self, ms: u64) {
        self.delay_ms.store(ms, Ordering::Release);
    }
    /// Add `delta` (may be negative) clamped to `[0, 10_000]`.
    pub fn bump_delay(&self, delta: i64) {
        let current = self.delay_ms() as i64;
        let next = (current + delta).clamp(0, 10_000) as u64;
        self.set_delay_ms(next);
    }
    pub fn toggle_step(&self) {
        let now = self.step_mode.load(Ordering::Acquire);
        self.step_mode.store(!now, Ordering::Release);
        // If we're leaving step mode, release a pending wait so the reader
        // doesn't sit indefinitely.
        if now {
            let _ = self.step_tx.try_send(());
        }
    }
    /// Release one event from step mode. No-op in continuous mode. If a
    /// previous advance is still buffered, the press is coalesced.
    pub fn advance(&self) {
        match self.step_tx.try_send(()) {
            Ok(_) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn full_speed_does_not_sleep() {
        let (_h, p) = channel(0, false);
        let t0 = Instant::now();
        p.wait();
        assert!(t0.elapsed() < Duration::from_millis(10));
    }

    #[test]
    fn delay_respected() {
        let (_h, p) = channel(50, false);
        let t0 = Instant::now();
        p.wait();
        assert!(t0.elapsed() >= Duration::from_millis(45));
    }

    #[test]
    fn bump_clamps() {
        let (h, _p) = channel(0, false);
        h.bump_delay(-100);
        assert_eq!(h.delay_ms(), 0);
        h.bump_delay(15_000);
        assert_eq!(h.delay_ms(), 10_000);
    }

    #[test]
    fn step_blocks_until_advance() {
        let (h, p) = channel(0, true);
        let joined = thread::spawn(move || {
            p.wait();
        });
        thread::sleep(Duration::from_millis(20));
        assert!(!joined.is_finished());
        h.advance();
        joined.join().unwrap();
    }

    #[test]
    fn toggle_step_off_releases_waiter() {
        let (h, p) = channel(0, true);
        let joined = thread::spawn(move || {
            p.wait();
        });
        thread::sleep(Duration::from_millis(20));
        h.toggle_step();
        joined.join().unwrap();
    }
}
