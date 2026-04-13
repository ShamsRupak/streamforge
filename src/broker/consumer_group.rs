use std::collections::HashMap;

/// Tracks the committed offset for each (group, topic, partition) triple.
#[derive(Default)]
pub struct ConsumerGroupCoordinator {
    // (group_id, topic, partition) -> committed offset
    offsets: HashMap<(String, String, u32), u64>,
}

impl ConsumerGroupCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn commit(&mut self, group: &str, topic: &str, partition: u32, offset: u64) {
        self.offsets.insert(
            (group.to_string(), topic.to_string(), partition),
            offset,
        );
    }

    pub fn fetch_offset(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        self.offsets
            .get(&(group.to_string(), topic.to_string(), partition))
            .copied()
    }
}
