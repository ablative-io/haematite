use std::fmt;

pub type CustomMergeFn = fn(&[u8], Option<&[u8]>, Option<&[u8]>, Option<&[u8]>) -> Option<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictInput {
    pub key: Vec<u8>,
    pub ancestor_value: Option<Vec<u8>>,
    pub parent_value: Option<Vec<u8>>,
    pub branch_value: Option<Vec<u8>>,
}

impl ConflictInput {
    pub const fn new(
        key: Vec<u8>,
        ancestor_value: Option<Vec<u8>>,
        parent_value: Option<Vec<u8>>,
        branch_value: Option<Vec<u8>>,
    ) -> Self {
        Self {
            key,
            ancestor_value,
            parent_value,
            branch_value,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictPolicy {
    Lww,
    VectorClock,
    Custom(CustomMergeFn),
}

impl ConflictPolicy {
    pub fn resolve(&self, conflict: &ConflictInput) -> Result<Option<Vec<u8>>, ConflictError> {
        match self {
            Self::Lww => Ok(conflict.branch_value.clone()),
            Self::VectorClock => Err(ConflictError::Unimplemented {
                feature: "vector-clock conflict resolution",
            }),
            Self::Custom(function) => Ok(function(
                conflict.key.as_slice(),
                conflict.ancestor_value.as_deref(),
                conflict.parent_value.as_deref(),
                conflict.branch_value.as_deref(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictError {
    Unimplemented { feature: &'static str },
    Unresolved { key: Vec<u8> },
}

impl fmt::Display for ConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unimplemented { feature } => write!(formatter, "{feature} is not implemented"),
            Self::Unresolved { key } => write!(
                formatter,
                "conflict on key {} is unresolved",
                String::from_utf8_lossy(key)
            ),
        }
    }
}

impl std::error::Error for ConflictError {}

#[cfg(test)]
mod tests {
    use super::{ConflictError, ConflictInput, ConflictPolicy};

    fn custom_resolution(
        key: &[u8],
        ancestor_value: Option<&[u8]>,
        parent_value: Option<&[u8]>,
        branch_value: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        if key.is_empty()
            && ancestor_value.is_none()
            && parent_value.is_none()
            && branch_value.is_none()
        {
            return None;
        }

        let mut resolved = Vec::new();
        resolved.extend_from_slice(key);
        resolved.push(b'|');
        resolved.extend_from_slice(ancestor_value.unwrap_or(b"none"));
        resolved.push(b'|');
        resolved.extend_from_slice(parent_value.unwrap_or(b"none"));
        resolved.push(b'|');
        resolved.extend_from_slice(branch_value.unwrap_or(b"none"));
        Some(resolved)
    }

    #[test]
    fn lww_takes_branch_value() -> Result<(), ConflictError> {
        let conflict = ConflictInput::new(
            b"k".to_vec(),
            Some(b"base".to_vec()),
            Some(b"parent".to_vec()),
            Some(b"branch".to_vec()),
        );

        assert_eq!(
            ConflictPolicy::Lww.resolve(&conflict)?,
            Some(b"branch".to_vec())
        );
        Ok(())
    }

    #[test]
    fn lww_takes_branch_delete() -> Result<(), ConflictError> {
        let conflict = ConflictInput::new(
            b"k".to_vec(),
            Some(b"base".to_vec()),
            Some(b"parent".to_vec()),
            None,
        );

        assert_eq!(ConflictPolicy::Lww.resolve(&conflict)?, None);
        Ok(())
    }

    #[test]
    fn custom_function_receives_all_values() -> Result<(), ConflictError> {
        let conflict = ConflictInput::new(
            b"k".to_vec(),
            Some(b"base".to_vec()),
            Some(b"parent".to_vec()),
            Some(b"branch".to_vec()),
        );

        assert_eq!(
            ConflictPolicy::Custom(custom_resolution).resolve(&conflict)?,
            Some(b"k|base|parent|branch".to_vec())
        );
        Ok(())
    }

    #[test]
    fn vector_clock_is_exposed_but_deferred() {
        let conflict = ConflictInput::new(
            b"k".to_vec(),
            Some(b"base".to_vec()),
            Some(b"parent".to_vec()),
            Some(b"branch".to_vec()),
        );

        assert_eq!(
            ConflictPolicy::VectorClock.resolve(&conflict),
            Err(ConflictError::Unimplemented {
                feature: "vector-clock conflict resolution"
            })
        );
    }

    #[test]
    fn conflict_policy_is_debuggable() {
        assert!(format!("{:?}", ConflictPolicy::Lww).contains("Lww"));
        assert!(format!("{:?}", ConflictPolicy::VectorClock).contains("VectorClock"));
        assert!(format!("{:?}", ConflictPolicy::Custom(custom_resolution)).contains("Custom"));
    }
}
