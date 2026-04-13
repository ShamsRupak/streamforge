use std::collections::{BTreeSet, HashMap};

/// Tracks consumer group membership and committed offsets.
///
/// Member registry uses a `BTreeSet` so iteration order is deterministic —
/// this is required for the range partition-assignment strategy to be stable
/// across calls.
#[derive(Default)]
pub struct ConsumerGroupCoordinator {
    /// (group_id, topic, partition) → committed offset
    offsets: HashMap<(String, String, u32), u64>,
    /// group_id → sorted set of consumer_ids
    members: HashMap<String, BTreeSet<String>>,
}

impl ConsumerGroupCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Member registry ───────────────────────────────────────────────────────

    /// Add `consumer_id` to `group`. Idempotent.
    pub fn register_consumer(&mut self, group: &str, consumer_id: &str) {
        self.members
            .entry(group.to_string())
            .or_default()
            .insert(consumer_id.to_string());
    }

    /// Remove `consumer_id` from `group`. No-op if not present.
    pub fn deregister_consumer(&mut self, group: &str, consumer_id: &str) {
        if let Some(m) = self.members.get_mut(group) {
            m.remove(consumer_id);
        }
    }

    /// Return the sorted list of consumer IDs in `group`.
    pub fn group_members(&self, group: &str) -> Vec<String> {
        self.members
            .get(group)
            .map(|m| m.iter().cloned().collect())
            .unwrap_or_default()
    }

    // ── Range partition assignment ─────────────────────────────────────────────
    //
    // Consumers are sorted alphabetically; partitions are divided into
    // contiguous ranges assigned left-to-right.  Extra partitions (when
    // `num_partitions % num_consumers != 0`) go to the first consumers.
    //
    // Example: 7 partitions, 3 consumers [A, B, C]
    //   A → [0, 1, 2]   (base 2 + 1 extra)
    //   B → [3, 4]      (base 2)
    //   C → [5, 6]      (base 2)

    /// Assign `num_partitions` partitions across all registered consumers in
    /// `group` using the range strategy.
    ///
    /// Returns `HashMap<consumer_id, Vec<partition_id>>`.
    /// Returns an empty map when the group has no members.
    pub fn assign_partitions(
        &self,
        group: &str,
        num_partitions: u32,
    ) -> HashMap<String, Vec<u32>> {
        let members: Vec<String> = match self.members.get(group) {
            Some(m) if !m.is_empty() => m.iter().cloned().collect(),
            _ => return HashMap::new(),
        };

        let n = num_partitions as usize;
        let m = members.len();
        let base = n / m;
        let extra = n % m; // first `extra` consumers get one extra partition

        let mut result = HashMap::with_capacity(m);
        let mut start = 0usize;
        for (i, member) in members.into_iter().enumerate() {
            let count = base + usize::from(i < extra);
            let partitions = (start..start + count).map(|p| p as u32).collect();
            result.insert(member, partitions);
            start += count;
        }
        result
    }

    // ── Offset tracking ───────────────────────────────────────────────────────

    pub fn commit(&mut self, group: &str, topic: &str, partition: u32, offset: u64) {
        self.offsets
            .insert((group.to_string(), topic.to_string(), partition), offset);
    }

    pub fn fetch_offset(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        self.offsets
            .get(&(group.to_string(), topic.to_string(), partition))
            .copied()
    }
}
