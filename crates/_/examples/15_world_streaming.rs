use anput::{
    entity::Entity,
    processor::WorldProcessor,
    query::Query,
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

// Component we slap on entities that belong to specific world chunk, so we can easily remove them.
#[derive(IntuicioStruct, Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct WorldChunk {
    index: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_basics(10240, 10240)?;

    println!("--- Game started with chunk #0");
    let world_chunk = make_world_chunk(0)?;
    apply_world_chunk(&mut universe, world_chunk)?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    println!("--- Loaded chunk #1");
    let world_chunk = make_world_chunk(1)?;
    apply_world_chunk(&mut universe, world_chunk)?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    println!("--- Unloaded chunk #0");
    release_world_chunk(&mut universe, 0)?;
    Systems::run_one_shot::<true>(&universe, report_world_state)?;

    Ok(())
}

fn make_world_chunk(index: usize) -> Result<World, Box<dyn Error>> {
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
            world.spawn((Monster, health, strength, WorldChunk { index }))?;
        } else {
            world.spawn((Animal, health, strength, WorldChunk { index }))?;
        }
    }

    Ok(world)
}

fn apply_world_chunk(universe: &mut Universe, world: World) -> Result<(), Box<dyn Error>> {
    // World can be merged, which allows us to load them partially.
    Ok(universe
        .simulation
        .merge::<true>(world, &*universe.resources.get::<true, WorldProcessor>()?)?)
}

fn release_world_chunk(universe: &mut Universe, index: usize) -> Result<(), Box<dyn Error>> {
    let to_remove = universe
        .simulation
        .query::<true, (Entity, &WorldChunk)>()
        .filter(|(_, chunk)| chunk.index == index)
        .map(|(entity, _)| entity)
        .collect::<Vec<_>>();

    for entity in to_remove {
        universe.simulation.despawn(entity)?;
    }

    Ok(())
}

fn report_world_state(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, query) = context.fetch::<(
        &World,
        Query<
            true,
            (
                &WorldChunk,
                Option<&Monster>,
                Option<&Animal>,
                &Health,
                &Strength,
            ),
        >,
    )>()?;

    for (chunk, monster, animal, health, strength) in query.query(world) {
        println!(
            "Chunk: {chunk:?} | Monster: {monster:?} | Animal: {animal:?} | Health: {health:?} | Strength: {strength:?}"
        );
    }

    Ok(())
}
