use anput::{
    multiverse::Multiverse,
    systems::{SystemContext, Systems},
    universe::Universe,
    world::World,
};
use intuicio_derive::IntuicioStruct;
use rand::{RngExt, rng};
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

#[derive(IntuicioStruct, Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct Monster;

#[derive(IntuicioStruct, Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct Animal;

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_basics(10240, 10240)?;

    println!("--- Game started with chunk #0");
    let world_chunk = make_world_chunk()?;
    // We can spawn entities with other worlds in simulation to build worlds hierarchy.
    let chunk_entity = universe.simulation.spawn((world_chunk,))?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    println!("--- Loaded chunk #1");
    let world_chunk = make_world_chunk()?;
    universe.simulation.spawn((world_chunk,))?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    println!("--- Unloaded chunk #0");
    universe.simulation.despawn(chunk_entity)?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    Ok(())
}

fn make_world_chunk() -> Result<World, Box<dyn Error>> {
    let mut world = World::default();
    let mut rng = rng();

    for _ in 0..rng.random_range(2..4) {
        let health = Health {
            value: rng.random_range(1..10),
        };
        let strength = Strength {
            value: rng.random_range(1..10),
        };
        if rng.random_bool(0.5) {
            world.spawn((Monster, health, strength))?;
        } else {
            world.spawn((Animal, health, strength))?;
        }
    }

    Ok(world)
}

fn report_world_state(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let world = context.fetch::<&World>()?;

    // Multiverse world interface allows us to query components in nested worlds.
    for (monster, animal, health, strength) in
        Multiverse::new(world)
            .query::<true, (Option<&Monster>, Option<&Animal>, &Health, &Strength)>()
    {
        println!(
            "Monster: {monster:?} | Animal: {animal:?} | Health: {health:?} | Strength: {strength:?}"
        );
    }

    Ok(())
}
