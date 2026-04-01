pub mod conflict;
pub mod protocol;
pub use conflict::ConflictResolver;
pub use protocol::{DataChange, SyncMessage};
