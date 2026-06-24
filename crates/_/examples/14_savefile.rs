use anput::{
    commands::{CommandBuffer, DespawnCommand},
    entity::Entity,
    prefab::Prefab,
    processor::WorldProcessor,
    universe::Universe,
};
use intuicio_core::{IntuicioStruct, registry::Registry};
use intuicio_derive::IntuicioStruct;
use intuicio_framework_serde::SerializationRegistry;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(IntuicioStruct, Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct Health {
    value: usize,
}

#[derive(IntuicioStruct, Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct Strength {
    value: usize,
}

const FILENAME: &str = "./crates/_/examples/snapshot.save";

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_basics(10240, 10240)?;

    // Register components to type registry (holds reflection info of each type).
    let registry = &mut *universe.resources.get_mut::<true, Registry>()?;
    registry.add_type(Health::define_struct(registry));
    registry.add_type(Strength::define_struct(registry));

    // Register components to serialization registry (tells how to (de)serialize each type).
    let serialization = &mut *universe
        .resources
        .get_mut::<true, SerializationRegistry>()?;
    serialization.register_serde::<Health>();
    serialization.register_serde::<Strength>();

    // Grab access to world processor (here does nothing, but knows how to remap entities in components).
    let processor = &*universe.resources.get::<true, WorldProcessor>()?;

    if let Ok(serialized) = std::fs::read_to_string(FILENAME) {
        // deserialize stored world snapshot to world instance if present.
        let deserialized = serde_json::from_str::<Prefab>(&serialized)?;
        let world = deserialized
            .to_world::<true>(processor, serialization, registry, ())?
            .0;
        universe.simulation = world;
        println!("Game loaded from file!");
    } else {
        // Populate world if world snapshot is not present.
        universe
            .simulation
            .spawn((Health { value: 100 }, Strength { value: 20 }))?;
        universe
            .simulation
            .spawn((Health { value: 120 }, Strength { value: 15 }))?;
        println!("Fresh game created!");
    }

    // Perform entities battle.
    for (entity_a, health) in universe.simulation.query::<true, (Entity, &mut Health)>() {
        if health.value == 0 {
            continue;
        }

        let damage = universe
            .simulation
            .query::<true, (Entity, &Strength)>()
            .filter(|(entity_b, _)| entity_a != *entity_b)
            .map(|(_, strength)| strength.value)
            .sum();

        health.value = health.value.saturating_sub(damage);
    }

    // Despawn dead entities.
    for (entity, health) in universe.simulation.query::<true, (Entity, &Health)>() {
        if health.value == 0 {
            universe
                .resources
                .get_mut::<true, CommandBuffer>()?
                .command(DespawnCommand::new(entity));
            println!("Entity {entity} is dead!");
        }
    }
    universe
        .resources
        .get_mut::<true, CommandBuffer>()?
        .execute(&mut universe.simulation)
        .unwrap();

    // Print game state.
    for (entity, health, strength) in universe
        .simulation
        .query::<true, (Entity, &Health, &Strength)>()
    {
        println!(
            "Entity: {} | Health: {} | Strength: {}",
            entity, health.value, strength.value
        );
    }

    // Serialize game snapshot to JSON and store it in a file.
    let prefab = Prefab::from_world::<true>(&universe.simulation, serialization, registry)?;
    let serialized = serde_json::to_string_pretty(&prefab)?;
    std::fs::write(FILENAME, serialized)?;
    println!("Game saved to file!");

    Ok(())
}
