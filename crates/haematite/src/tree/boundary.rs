pub const DEFAULT_TARGET_SIZE: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryDetector {
    target_size: usize,
}

impl BoundaryDetector {
    pub const fn new(target_size: usize) -> Self {
        Self { target_size }
    }

    pub const fn target_size(&self) -> usize {
        self.target_size
    }

    pub fn is_boundary(&self, key: &[u8]) -> bool {
        let Ok(target_size) = u64::try_from(self.target_size) else {
            return false;
        };

        if target_size == 0 {
            return false;
        }

        let digest = blake3::hash(key);
        let [
            byte_0,
            byte_1,
            byte_2,
            byte_3,
            byte_4,
            byte_5,
            byte_6,
            byte_7,
            ..,
        ] = *digest.as_bytes();
        let sample = u64::from_le_bytes([
            byte_0, byte_1, byte_2, byte_3, byte_4, byte_5, byte_6, byte_7,
        ]);

        sample % target_size == 0
    }
}

impl Default for BoundaryDetector {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundaryDetector, DEFAULT_TARGET_SIZE};

    fn pseudo_random_keys(count: usize) -> impl Iterator<Item = [u8; 8]> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        (0..count).map(move |_| {
            state = state
                .wrapping_mul(0xbf58_476d_1ce4_e5b9)
                .wrapping_add(0x94d0_49bb_1331_11eb);
            state.to_le_bytes()
        })
    }

    #[test]
    fn default_target_is_four_kib() {
        assert_eq!(
            BoundaryDetector::default().target_size(),
            DEFAULT_TARGET_SIZE
        );
    }

    #[test]
    fn is_boundary_matches_threshold_rule() {
        let detector = BoundaryDetector::new(1);

        assert!(detector.is_boundary(b"any key meets modulo-one threshold"));
    }

    #[test]
    fn same_key_sequence_has_same_boundaries() {
        let detector = BoundaryDetector::new(16);
        let keys: Vec<Vec<u8>> = (0_u64..256).map(u64::to_le_bytes).map(Vec::from).collect();

        let first: Vec<usize> = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| detector.is_boundary(key).then_some(index))
            .collect();
        let second: Vec<usize> = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| detector.is_boundary(key).then_some(index))
            .collect();

        assert_eq!(first, second);
    }

    #[test]
    fn target_size_changes_boundary_positions() {
        let keys: Vec<Vec<u8>> = (0_u64..1_000)
            .map(u64::to_le_bytes)
            .map(Vec::from)
            .collect();
        let small = BoundaryDetector::new(8);
        let large = BoundaryDetector::new(64);

        let small_boundaries: Vec<usize> = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| small.is_boundary(key).then_some(index))
            .collect();
        let large_boundaries: Vec<usize> = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| large.is_boundary(key).then_some(index))
            .collect();

        assert_ne!(small_boundaries, large_boundaries);
    }

    #[test]
    fn zero_target_never_places_boundaries() {
        let detector = BoundaryDetector::new(0);
        assert!(!detector.is_boundary(b"key"));
    }

    #[test]
    fn thousand_key_stream_has_boundaries_roughly_at_target_interval() {
        let target = 32;
        let detector = BoundaryDetector::new(target);
        let boundary_count = pseudo_random_keys(1_000)
            .filter(|key| detector.is_boundary(key.as_slice()))
            .count();

        assert!((10..=60).contains(&boundary_count));
    }
}
