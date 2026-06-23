use anput::{
    commands::{CommandBuffer, DespawnCommand, SpawnCommand},
    entity::Entity,
    query::{Query, Update},
    scheduler::{GraphScheduler, GraphSchedulerPlugin},
    systems::SystemContext,
    universe::{Res, Universe},
    world::World,
};
use moirai::jobs::Jobs;
use rand::{RngExt, rng};
use std::error::Error;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum MonsterEvolution {
    #[default]
    Puppy,
    Wolf,
}

#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    created: usize,
    updated: usize,
    destroyed: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_plugin(
        GraphSchedulerPlugin::<true>::default()
            .resource(CommandBuffer::default())
            .resource(Stats::default())
            .system_setup(evolve_monster, |system| system.name("evolve_monster"))
            .system_setup(spawn_monster, |system| system.name("spawn_monster"))
            .system_setup(stats_react, |system| system.name("stats_react")),
    );
    let jobs = Jobs::default();
    let scheduler = GraphScheduler::<true>;

    for index in 0..10 {
        println!("* Iteration: {index}");
        scheduler.run(&jobs, &mut universe)?;
    }

    Ok(())
}

fn spawn_monster(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let mut commands = context.fetch::<Res<true, &mut CommandBuffer>>()?;

    for _ in 0..rng().random_range(0..3) {
        commands.command(SpawnCommand::new((MonsterEvolution::default(),)));
    }

    Ok(())
}

fn evolve_monster(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut commands, monster_query) = context.fetch::<(
        &World,
        Res<true, &mut CommandBuffer>,
        Query<true, (Entity, Update<MonsterEvolution>)>,
    )>()?;

    for (entity, mut monster) in monster_query.query(world) {
        match *monster.read() {
            MonsterEvolution::Puppy => {
                *monster.write_notified(world) = MonsterEvolution::Wolf;
            }
            MonsterEvolution::Wolf => {
                commands.command(DespawnCommand::new(entity));
            }
        };
    }

    Ok(())
}

fn stats_react(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut stats) = context.fetch::<(&World, Res<true, &mut Stats>)>()?;

    for entity in world.added().iter_of::<MonsterEvolution>() {
        println!("Monster created: {entity}");
        stats.created += 1;
    }

    for entity in world.updated().unwrap().iter_of::<MonsterEvolution>() {
        println!("Monster updated: {entity}");
        stats.updated += 1;
    }

    for entity in world.removed().iter_of::<MonsterEvolution>() {
        println!("Monster destroyed: {entity}");
        stats.destroyed += 1;
    }

    Ok(())
}
