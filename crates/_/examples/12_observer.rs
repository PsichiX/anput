use anput::{
    commands::{CommandBuffer, DespawnCommand, SpawnCommand},
    observer::ChangeObserver,
    scheduler::{GraphScheduler, GraphSchedulerPlugin},
    systems::SystemContext,
    universe::{Res, Universe},
};
use moirai::jobs::Jobs;
use rand::{RngExt, rng};
use std::error::Error;

#[derive(Debug, Default, Clone, Copy)]
struct Temperature(pub isize);

#[derive(Debug, Default, Clone, Copy)]
struct Heat;

#[derive(Debug, Default, Clone, Copy)]
struct Cold;

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_plugin(
        GraphSchedulerPlugin::<true>::default()
            .resource(CommandBuffer::default())
            .system_setup(spawn_temperature_change, |system| {
                system.name("spawn_temperature_change")
            }),
    );
    let jobs = Jobs::default();
    let scheduler = GraphScheduler::<true>;

    let temperature = universe.simulation.spawn((Temperature::default(),))?;

    let mut observer = ChangeObserver::default();
    observer.on_added::<Heat>(move |world, commands, entity| {
        let mut temperature = world
            .component_mut::<true, Temperature>(temperature)
            .unwrap();
        temperature.0 += 1;
        println!("Temperature increase");

        commands.command(DespawnCommand::new(entity));
    });
    observer.on_added::<Cold>(move |world, commands, entity| {
        let mut temperature = world
            .component_mut::<true, Temperature>(temperature)
            .unwrap();
        temperature.0 -= 1;
        println!("Temperature decrease");

        commands.command(DespawnCommand::new(entity));
    });

    for index in 0..10 {
        println!("* Iteration: {index}");
        scheduler.run(&jobs, &mut universe)?;
        observer.process_execute(&mut universe.simulation);

        let temperature = universe
            .simulation
            .component::<true, Temperature>(temperature)?;
        println!("Temperature: {}", temperature.0);
    }

    Ok(())
}

fn spawn_temperature_change(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let mut commands = context.fetch::<Res<true, &mut CommandBuffer>>()?;

    if rng().random_bool(0.5) {
        commands.command(SpawnCommand::new((Heat,)));
    } else {
        commands.command(SpawnCommand::new((Cold,)));
    }

    Ok(())
}
