use anput::{
    commands::Command,
    database::{WorldDestroyIteratorExt, WorldJoinIteratorExt},
    entity::Entity,
    query::Include,
    universe::Universe,
    world::Relation,
};
use std::error::Error;

#[derive(Debug, Default, Clone, Copy)]
struct Health(pub usize);

struct Commands;

struct Enemy;

struct Monster;

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default();

    let a = universe.simulation.spawn(("a", Monster, Health(10)))?;
    let b = universe.simulation.spawn(("b", Monster, Health(3)))?;
    let c = universe.simulation.spawn(("c", Monster, Health(26)))?;
    universe.simulation.spawn((
        "enemy",
        Enemy,
        Relation::new(Commands, a)
            .with(Commands, b)
            .with(Commands, c),
    ))?;

    // Deal collective damage.
    let iter = universe
        .simulation
        .query::<true, (&&str, &Relation<Commands>, Include<Enemy>)>()
        .join(
            universe
                .simulation
                .lookup_access::<true, (&&str, &mut Health, Include<Monster>)>(),
            |(_, relation, _)| relation.entities(),
        );
    for ((enemy_name, _, _), (monster_name, health, _)) in iter {
        health.0 = health.0.saturating_sub(10);

        println!(
            "Damage dealt to {:?} monster of {:?} enemy. Health after: {}",
            monster_name, enemy_name, health.0
        );
    }

    // Remove dead monsters.
    universe
        .simulation
        .query::<true, (Entity, &Health, Include<Monster>)>()
        .filter(|(_, health, _)| health.0 == 0)
        .map(|(entity, _, _)| entity)
        .to_despawn_command()
        .execute(&mut universe.simulation)
        .unwrap();

    // Report alive monsters.
    for (name, _) in universe
        .simulation
        .query::<true, (&&str, Include<Monster>)>()
    {
        println!("Monster still alive: {name:?}");
    }

    Ok(())
}
