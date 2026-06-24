pub mod archetype;
pub mod bundle;
pub mod commands;
pub mod component;
pub mod database;
pub mod entity;
pub mod event;
pub mod multiverse;
pub mod observer;
pub mod prefab;
pub mod processor;
pub mod query;
pub mod resources;
pub mod scene_graph;
pub mod scheduler;
pub mod systems;
pub mod universe;
pub mod view;
pub mod world;

pub mod third_party {
    pub use intuicio_core;
    pub use intuicio_data;
    pub use intuicio_derive;
    pub use intuicio_framework_serde;
    pub use moirai;

    pub mod time {
        #[cfg(target_arch = "wasm32")]
        pub use instant::{Duration, Instant};
        #[cfg(not(target_arch = "wasm32"))]
        pub use std::time::{Duration, Instant};
    }
}
