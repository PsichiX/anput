use anput::{
    entity::Entity,
    query::Include,
    scheduler::{GraphScheduler, GraphSchedulerPlugin},
    systems::SystemContext,
    third_party::moirai::jobs::Jobs,
    universe::{Res, Universe},
    world::World,
};
use anput_spatial::{third_party::rstar::AABB, *};
use std::error::Error;
use vek::Vec2;

fn main() -> Result<(), Box<dyn Error>> {
    // Setup universe with spatial partitioning plugin and game plugin.
    let mut universe = Universe::default()
        .with_basics(10240, 10240)?
        .with_plugin(anput_spatial::make_plugin::<true, MySpatialExtractor>())
        .with_plugin(
            GraphSchedulerPlugin::<true>::default()
                .system_setup(report_nearest, |system| system.name("report_nearest")),
        );

    // Spawn entities with positions and spatial component except one,
    // to show only entities marked with Spatial component will be reported.
    universe
        .simulation
        .spawn((Spatial, Vec2::<f32>::new(0.0, 1.0)))?;
    universe.simulation.spawn((Vec2::<f32>::new(0.0, -2.0),))?;
    universe
        .simulation
        .spawn((Spatial, Vec2::<f32>::new(3.0, 0.0)))?;
    universe
        .simulation
        .spawn((Spatial, Vec2::<f32>::new(-4.0, 0.0)))?;

    // Run single simulation frame.
    let jobs = Jobs::default();
    let scheduler = GraphScheduler::<true>;
    scheduler.run(&jobs, &mut universe)?;

    Ok(())
}

fn report_nearest(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, spatial) =
        context.fetch::<(&World, Res<true, &SpatialPartitioning<MySpatialExtractor>>)>()?;

    // Print all spatial entities and their positions and distances
    // in order by their distance to queried point.
    println!("Nearest entities to [0, 0]:");
    for (entity, position) in spatial.nearest_query::<true, (Entity, &Vec2<f32>)>(world, [0.0, 0.0])
    {
        let distance = position.magnitude();
        println!("Entity: {entity} | Position: {position} | Distance: {distance}");
    }

    // Print all spatial entities and their positions
    // contained in the positive X and Y area.
    println!("Entities contained in AABB from [0, 0] to [inf, inf]:");
    for (entity, position) in spatial.locate_contained_query::<true, (Entity, &Vec2<f32>)>(
        world,
        AABB::from_corners([0.0, 0.0], [f32::INFINITY, f32::INFINITY]),
    ) {
        println!("Entity: {entity} | Position: {position}");
    }

    Ok(())
}

struct Spatial;

struct MySpatialExtractor;

// Spatial extractor tells how to extract spatial objects from the world
// with user-made world queries.
impl SpatialExtractor for MySpatialExtractor {
    type SpatialObject = [f32; 2];

    fn extract<const LOCKING: bool>(
        world: &World,
    ) -> impl Iterator<Item = (Entity, Self::SpatialObject)> {
        world
            .query::<LOCKING, (Entity, &Vec2<f32>, Include<Spatial>)>()
            .map(|(entity, point, _)| (entity, [point.x, point.y]))
    }
}
