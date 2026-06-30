//! Serving peers: a pure unchoke policy (`Choker`) and the `SeederSession` serve loop.

/// Decides which interested peers to unchoke. Live-appropriate: unchoke up to `max_unchoked`
/// interested peers (stable order) plus one rotating "optimistic" peer so newcomers get a turn.
pub struct Choker {
    max_unchoked: usize,
}

impl Choker {
    pub fn new(max_unchoked: usize) -> Self {
        Choker { max_unchoked }
    }

    /// Peers to unchoke now. `interested` is the current interested set (caller-stable order);
    /// `tick` rotates the optimistic slot over time.
    pub fn choose(&self, interested: &[u64], tick: u64) -> Vec<u64> {
        let mut out: Vec<u64> = interested.iter().take(self.max_unchoked).copied().collect();
        let rest = &interested[out.len()..];
        if !rest.is_empty() {
            out.push(rest[(tick as usize) % rest.len()]);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchokes_up_to_max_plus_one_optimistic() {
        let c = Choker::new(2);
        // first 2 always unchoked; the 3rd slot rotates through the remainder by tick.
        assert_eq!(c.choose(&[10, 20, 30, 40], 0), vec![10, 20, 30]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 1), vec![10, 20, 40]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 2), vec![10, 20, 30]); // wraps
    }

    #[test]
    fn fewer_interested_than_max_unchokes_all() {
        let c = Choker::new(4);
        assert_eq!(c.choose(&[10, 20], 0), vec![10, 20]);
        assert_eq!(c.choose(&[], 0), Vec::<u64>::new());
    }
}
