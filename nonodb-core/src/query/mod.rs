pub mod matcher;
pub mod path;
pub mod updater;
pub(crate) mod validate;

pub use matcher::matches;
pub use updater::{
    apply as apply_update, apply_upsert, apply_upsert_with_options, apply_with_options,
    seed_from_filter,
};
