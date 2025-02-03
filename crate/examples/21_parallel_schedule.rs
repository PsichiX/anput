use anput::prelude::*;
use std::error::Error;

#[derive(Debug, Default, Clone, Copy)]
struct Energy(pub usize);

#[derive(Debug, Default, Clone, Copy)]
struct Water(pub usize);

#[derive(Debug, Default, Clone, Copy)]
struct Age(pub usize);

struct Parent;

fn main() -> Result<(), Box<dyn Error>> {
    struct MyPlugin;
    let plugin = GraphSchedulerQuickPlugin::<true, MyPlugin>::default()
        // We define sequenced group for tree progression.
        .group("progress", (), |group| {
            // In that group we run all its systems in parallel, because we know
            // they don't interact with same components mutably.
            // Group will wait for all its parallelized systems to complete and
            // only then this group completes.
            group
                .system(
                    consume_energy,
                    "consume_energy",
                    (SystemParallelize::AnyWorker,),
                )
                .system(
                    consume_water,
                    "consume_water",
                    (SystemParallelize::AnyWorker,),
                )
                .system(age, "age", (SystemParallelize::AnyWorker,))
        })
        // After entire progression group completes, we then run reproduction
        // system sequenced, because reproduction needs to mutate all components
        // to be able to spawn new trees.
        .system(reproduce, "reproduce", ())
        .commit();

    let mut universe = Universe::default()
        .with_basics(10240, 10240)
        .with_plugin(plugin);
    let mut scheduler = GraphScheduler::<true>::default();

    // Spawn first tree that will start chain of new generations.
    universe.simulation.spawn((
        Energy::default(),
        Water::default(),
        Age::default(),
        Relation::<Parent>::default(),
    ))?;

    // Run few frames to get few generations.
    for _ in 0..5 {
        scheduler.run(&mut universe)?;
    }

    // Report forest population.
    for (entity, energy, water, age, parent) in
        universe
            .simulation
            .query::<true, (Entity, &Energy, &Water, &Age, &Relation<Parent>)>()
    {
        println!(
            "- Tree: {} | Energy: {} | Water: {} | Age: {} | Parent: {}",
            entity,
            energy.0,
            water.0,
            age.0,
            parent.entities().next().unwrap_or_default()
        );
    }

    Ok(())
}

fn consume_energy(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, query) = context.fetch::<(&World, Query<true, &mut Energy>)>()?;

    for energy in query.query(world) {
        energy.0 += 2;
    }

    Ok(())
}

fn consume_water(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, query) = context.fetch::<(&World, Query<true, &mut Water>)>()?;

    for water in query.query(world) {
        water.0 += 1;
    }

    Ok(())
}

fn age(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, query) = context.fetch::<(&World, Query<true, &mut Age>)>()?;

    for age in query.query(world) {
        age.0 += 1;
    }

    Ok(())
}

fn reproduce(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut commands, query) = context.fetch::<(
        &World,
        Res<true, &mut CommandBuffer>,
        Query<true, (Entity, &mut Energy, &mut Water)>,
    )>()?;

    for (entity, energy, water) in query.query(world) {
        while energy.0 >= 4 && water.0 >= 2 {
            energy.0 -= 4;
            water.0 -= 2;

            commands.command(SpawnCommand::new((
                Energy(2),
                Water(1),
                Age::default(),
                Relation::new(Parent, entity),
            )));
        }
    }

    Ok(())
}
