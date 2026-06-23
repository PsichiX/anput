use anput::{
    commands::{CommandBuffer, DespawnCommand, SpawnCommand},
    component::Component,
    entity::Entity,
    query::{Include, Query},
    scheduler::{GraphScheduler, GraphSchedulerPlugin},
    systems::{SystemContext, Systems},
    universe::{Res, Universe},
    world::World,
};
use moirai::jobs::Jobs;
use rand::{RngExt, rng};
use std::{
    error::Error,
    io::{Write, stdout},
    thread::sleep,
    time::Duration,
};

const INITIAL_POPULATION: usize = 5;
const LIFETIME: usize = 20;
const REPRODUCTION: usize = 5;

trait Kind: Component + Default {
    fn symbol(repro: &Reproduction) -> char;
}

#[derive(Debug, Default)]
struct Bunny;

impl Kind for Bunny {
    fn symbol(repro: &Reproduction) -> char {
        if repro.0 > 0 { 'b' } else { 'B' }
    }
}

#[derive(Debug, Default)]
struct Fox;

impl Kind for Fox {
    fn symbol(repro: &Reproduction) -> char {
        if repro.0 > 0 { 'f' } else { 'F' }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Position(usize, usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lifetime(usize);

impl Default for Lifetime {
    fn default() -> Self {
        Self(LIFETIME)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reproduction(usize);

impl Default for Reproduction {
    fn default() -> Self {
        Self(REPRODUCTION)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut universe = Universe::default().with_plugin(
        GraphSchedulerPlugin::<true>::default()
            .resource(CommandBuffer::default())
            .resource(ScreenGrid::new(10, 6))
            .resource(Contacts::default())
            .plugin_setup(|plugin| {
                plugin
                    .system_setup(movement, |system| system.name("movement"))
                    .system_setup(contacts, |system| system.name("contacts"))
                    .system_setup(reproduction::<Bunny>, |system| {
                        system.name("reproduction:bunny")
                    })
                    .system_setup(reproduction::<Fox>, |system| {
                        system.name("reproduction:fox")
                    })
                    .system_setup(kill::<Fox, Bunny>, |system| system.name("kill:fox:bunny"))
                    .system_setup(death, |system| system.name("death"))
            })
            .plugin_setup(|plugin| {
                plugin
                    .system_setup(render::<Bunny>, |system| system.name("render:bunny"))
                    .system_setup(render::<Fox>, |system| system.name("render:fox"))
            })
            .system_setup(display_screen, |system| system.name("display_screen")),
    );

    Systems::run_one_shot::<true>(&universe, init)?;

    let jobs = Jobs::default();
    let scheduler = GraphScheduler::<true>;
    loop {
        scheduler.run(&jobs, &mut universe)?;
        sleep(Duration::from_millis(500));
    }
}

fn init(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (mut commands, screen) =
        context.fetch::<(Res<true, &mut CommandBuffer>, Res<true, &ScreenGrid>)>()?;

    for _ in 0..INITIAL_POPULATION {
        let position = Position(
            rng().random_range(0..screen.width),
            rng().random_range(0..screen.height),
        );
        let lifetime = Lifetime::default();
        let reproduction = Reproduction::default();

        if rng().random_bool(0.5) {
            commands.command(SpawnCommand::new((Fox, position, lifetime, reproduction)));
        } else {
            commands.command(SpawnCommand::new((Bunny, position, lifetime, reproduction)));
        }
    }

    Ok(())
}

fn death(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut commands, query) = context.fetch::<(
        &World,
        Res<true, &mut CommandBuffer>,
        Query<true, (Entity, &mut Lifetime)>,
    )>()?;

    for (entity, lifetime) in query.query(world) {
        if lifetime.0 > 0 {
            lifetime.0 -= 1;
        } else {
            commands.command(DespawnCommand::new(entity));
        }
    }

    Ok(())
}

fn contacts(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut contacts, query) = context.fetch::<(
        &World,
        Res<true, &mut Contacts>,
        Query<true, (Entity, &Position)>,
    )>()?;

    contacts.pairs.clear();

    for (entity_a, position_a) in query.query(world) {
        for (entity_b, position_b) in query.query(world) {
            if entity_a != entity_b && position_a == position_b {
                let mut pair = [entity_a, entity_b];
                pair.sort();
                if !contacts.pairs.contains(&pair) {
                    contacts.pairs.push(pair);
                }
            }
        }
    }

    Ok(())
}

fn movement(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, screen, query) =
        context.fetch::<(&World, Res<true, &ScreenGrid>, Query<true, &mut Position>)>()?;

    let mut rng = rng();
    for Position(col, row) in query.query(world) {
        match rng.random_range(0..4) {
            0 => *col = (*col + screen.width - 1) % screen.width,
            1 => *row = (*row + screen.height - 1) % screen.height,
            2 => *col = (*col + 1) % screen.width,
            3 => *row = (*row + 1) % screen.height,
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn reproduction<T: Kind>(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut commands, contacts, query) = context.fetch::<(
        &World,
        Res<true, &mut CommandBuffer>,
        Res<true, &Contacts>,
        Query<true, &mut Reproduction>,
    )>()?;

    let selected = contacts
        .iter()
        .filter(|&(a, b)| {
            world.has_entity_component::<T>(a)
                && world.has_entity_component::<T>(b)
                && world
                    .component::<true, Reproduction>(a)
                    .map(|reproduction| reproduction.0 == 0)
                    .unwrap_or_default()
                && world
                    .component::<true, Reproduction>(b)
                    .map(|reproduction| reproduction.0 == 0)
                    .unwrap_or_default()
        })
        .collect::<Vec<_>>();

    for (a, b) in selected {
        world.component_mut::<true, Reproduction>(a)?.0 = REPRODUCTION;
        world.component_mut::<true, Reproduction>(b)?.0 = REPRODUCTION;
        let position = *world.component::<true, Position>(a)?;

        commands.command(SpawnCommand::new((
            T::default(),
            position,
            Lifetime::default(),
            Reproduction::default(),
        )));
    }

    for reproduction in query.query(world) {
        reproduction.0 = reproduction.0.saturating_sub(1);
    }

    Ok(())
}

fn kill<Predator: Kind, Pray: Kind>(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut commands, contacts) =
        context.fetch::<(&World, Res<true, &mut CommandBuffer>, Res<true, &Contacts>)>()?;

    for (a, b) in contacts.iter() {
        if world.has_entity_component::<Predator>(a) && world.has_entity_component::<Pray>(b) {
            commands.command(DespawnCommand::new(b));
        } else if world.has_entity_component::<Predator>(b) && world.has_entity_component::<Pray>(a)
        {
            commands.command(DespawnCommand::new(a));
        }
    }

    Ok(())
}

fn render<T: Kind>(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut screen, query) = context.fetch::<(
        &World,
        Res<true, &mut ScreenGrid>,
        Query<true, (&Position, &Reproduction, Include<T>)>,
    )>()?;

    for (&Position(col, row), repro, _) in query.query(world) {
        screen.set(col, row, T::symbol(repro));
    }

    Ok(())
}

fn display_screen(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let mut screen = context.fetch::<Res<true, &mut ScreenGrid>>()?;

    screen.render();
    screen.clear();

    Ok(())
}

#[derive(Debug, Default)]
struct Contacts {
    pairs: Vec<[Entity; 2]>,
}

impl Contacts {
    fn iter(&self) -> impl Iterator<Item = (Entity, Entity)> + '_ {
        self.pairs.iter().map(|[a, b]| (*a, *b))
    }
}

#[derive(Debug)]
struct ScreenGrid {
    width: usize,
    height: usize,
    buffer: Vec<char>,
}

impl ScreenGrid {
    fn new(width: usize, height: usize) -> Self {
        Self {
            width,
            height,
            buffer: vec!['.'; width * height],
        }
    }

    fn position_to_index(&self, col: usize, row: usize) -> usize {
        row.min(self.height) * self.width + col.min(self.width)
    }

    fn clear(&mut self) {
        for item in &mut self.buffer {
            *item = '.';
        }
    }

    fn set(&mut self, col: usize, row: usize, value: char) {
        let index = self.position_to_index(col, row);
        if let Some(item) = self.buffer.get_mut(index) {
            *item = value;
        }
    }

    fn render(&self) {
        print!("\x1B[2J\x1B[H");
        stdout().flush().unwrap();
        let mut column = 0;
        for item in &self.buffer {
            print!("{item}");
            column += 1;
            if column >= self.width {
                column = 0;
                println!();
            }
        }
        stdout().flush().unwrap();
    }
}
