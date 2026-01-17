use criterion::{Criterion, criterion_group, criterion_main};
use rand::{rng, seq::SliceRandom};

const ITERATIONS: usize = 10000;

fn spawn_entities(c: &mut Criterion) {
    c.bench_function("Anput - spawn entities", |b| {
        use anput::world::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();

        b.iter(|| {
            for _ in 0..ITERATIONS {
                let _ = world.spawn((Position::default(), Velocity::default()));
            }
        })
    });

    c.bench_function("Hecs - spawn entities", |b| {
        use hecs::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();

        b.iter(|| {
            for _ in 0..ITERATIONS {
                world.spawn((Position::default(), Velocity::default()));
            }
        })
    });

    c.bench_function("Shipyard - spawn entities", |b| {
        use shipyard::{Component, World};

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();

        b.iter(|| {
            for _ in 0..ITERATIONS {
                world.add_entity((Position::default(), Velocity::default()));
            }
        })
    });

    c.bench_function("Anput - iterate components locking", |b| {
        use anput::world::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        for _ in 0..ITERATIONS {
            let _ = world.spawn((Position::default(), Velocity::default()));
        }

        b.iter(|| {
            for (pos, vel) in world.query::<true, (&mut Position, &Velocity)>() {
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Anput - iterate components lock-free", |b| {
        use anput::world::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        for _ in 0..ITERATIONS {
            let _ = world.spawn((Position::default(), Velocity::default()));
        }

        b.iter(|| {
            for (pos, vel) in world.query::<false, (&mut Position, &Velocity)>() {
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Hecs - iterate components", |b| {
        use hecs::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        for _ in 0..ITERATIONS {
            world.spawn((Position::default(), Velocity::default()));
        }

        b.iter(|| {
            for (pos, vel) in world.query::<(&mut Position, &Velocity)>().iter() {
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Shipyard - iterate components", |b| {
        use shipyard::{Component, World};

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        for _ in 0..ITERATIONS {
            world.add_entity((Position::default(), Velocity::default()));
        }

        b.iter(|| {
            for (pos, vel) in &mut world.iter::<(&mut Position, &Velocity)>() {
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Anput - random access components locking", |b| {
        use anput::world::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        let mut entities = (0..ITERATIONS)
            .map(|_| {
                world
                    .spawn((Position::default(), Velocity::default()))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        entities.shuffle(&mut rng());
        let mut access = world.lookup_access::<true, (&mut Position, &Velocity)>();

        b.iter(|| {
            for entity in entities.iter().copied() {
                let (pos, vel) = access.access(entity).unwrap();
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Anput - random access components lock-free", |b| {
        use anput::world::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        let mut entities = (0..ITERATIONS)
            .map(|_| {
                world
                    .spawn((Position::default(), Velocity::default()))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        entities.shuffle(&mut rng());
        let mut access = world.lookup_access::<false, (&mut Position, &Velocity)>();

        b.iter(|| {
            for entity in entities.iter().copied() {
                let (pos, vel) = access.access(entity).unwrap();
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Hecs - random access components", |b| {
        use hecs::World;

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        let mut entities = (0..ITERATIONS)
            .map(|_| world.spawn((Position::default(), Velocity::default())))
            .collect::<Vec<_>>();
        entities.shuffle(&mut rng());
        let mut view = world.view::<(&mut Position, &Velocity)>();

        b.iter(|| {
            for entity in entities.iter().copied() {
                let (pos, vel) = view.get_mut(entity).unwrap();
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });

    c.bench_function("Shipyard - random access components", |b| {
        use shipyard::{Component, World};

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Position([f32; 2]);

        #[derive(Component, Default, Clone, Copy, PartialEq)]
        struct Velocity([f32; 2]);

        let mut world = World::default();
        let mut entities = (0..ITERATIONS)
            .map(|_| world.add_entity((Position::default(), Velocity::default())))
            .collect::<Vec<_>>();
        entities.shuffle(&mut rng());

        b.iter(|| {
            for entity in entities.iter().copied() {
                let (mut pos, vel) = world.get::<(&mut Position, &Velocity)>(entity).unwrap();
                pos.0[0] += vel.0[0];
                pos.0[1] += vel.0[1];
            }
        })
    });
}

criterion_group!(benches, spawn_entities);
criterion_main!(benches);
