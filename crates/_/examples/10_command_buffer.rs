use anput::{
    commands::{CommandBuffer, DespawnCommand, SpawnCommand},
    entity::Entity,
    query::Include,
    scheduler::GraphSchedulerPlugin,
    universe::Universe,
};
use std::error::Error;

#[derive(Debug, Default, Clone, Copy)]
struct Villager;

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default()
        .with_plugin(GraphSchedulerPlugin::<true>::default().resource(CommandBuffer::default()));

    // Issue spawn command to create a villager and immediatelly execute the buffer.
    {
        let mut commands = universe.resources.get_mut::<true, CommandBuffer>()?;
        commands.command(SpawnCommand::new((Villager,)));
        commands.execute(&mut universe.simulation).unwrap();
    }

    // Since changes from commands buffer are applied to the world, we can search for
    // spawned villager entity.
    let villager = universe
        .simulation
        .query::<true, (Entity, Include<Villager>)>()
        .next()
        .unwrap()
        .0;

    // Issue despawn command and execute.
    {
        let mut commands = universe.resources.get_mut::<true, CommandBuffer>()?;
        commands.command(DespawnCommand::new(villager));
        commands.execute(&mut universe.simulation).unwrap();
    }

    // Confirm villager no longer exists.
    println!(
        "Is villager still alive: {}",
        universe.simulation.has_entity(villager)
    );

    Ok(())
}
