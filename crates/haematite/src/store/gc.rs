use crate::tree::Hash;

pub trait DeleteNode {
    type Error: std::error::Error;

    fn delete(&self, hash: &Hash) -> Result<(), Self::Error>;
}
