use anput::{
    entity::Entity,
    view::WorldView,
    world::{Relation, World},
};
use moirai::{job::JobLocation, jobs::Jobs};
use rand::{Rng, rng};
use std::{collections::HashSet, error::Error};

struct Next;

fn main() -> Result<(), Box<dyn Error>> {
    let jobs = Jobs::default();
    let mut world = World::default();

    // Prepare nodes.
    for _ in 0..100 {
        world.spawn((Relation::<Next>::default(),))?;
    }

    // Build graph.
    let mut rng = rng();
    for source in world.entities().collect::<Vec<_>>() {
        let index = rng.random_range(0..world.len());
        if let Some(target) = world.entity_by_index(index)
            && source != target
        {
            world.relate::<true, _>(Next, source, target)?;
        }
    }

    // Find if there are cycles in graph.
    let view = WorldView::new::<(Relation<Next>,)>(&world);
    let job = jobs.broadcast_n(view.len(), move |ctx| {
        fn dfs(
            source: Entity,
            view: &WorldView,
            visited: &mut HashSet<Entity>,
            stack: &mut Vec<Entity>,
        ) -> Option<Vec<Entity>> {
            if let Some(index) = stack.iter().position(|entity| *entity == source) {
                stack.push(source);
                return Some(stack[index..].to_vec());
            }
            if visited.contains(&source) {
                return None;
            }
            visited.insert(source);
            stack.push(source);
            for target in view
                .entity::<true, &Relation<Next>>(source)
                .unwrap()
                .entities()
            {
                if let Some(found) = dfs(target, view, visited, stack) {
                    return Some(found);
                }
            }
            stack.pop();
            None
        }

        let mut visited = HashSet::with_capacity(view.len());
        let mut stack = Vec::with_capacity(view.len());

        let entity = view.entity_by_index(ctx.work_group_index)?;
        if let Some(found) = dfs(entity, &view, &mut visited, &mut stack) {
            return Some(found);
        }
        None
    });
    let result = job.wait().unwrap_or_default();

    // Present job results.
    if let Some(found) = result.into_iter().flatten().next() {
        println!("* Found cycle in graph: {found:#?}");
    } else {
        println!("* There is no cycle in graph!");
    }

    // Check if two nodes are connected.
    let from = world
        .entity_by_index(rng.random_range(0..world.len()))
        .unwrap();
    let to = world
        .entity_by_index(rng.random_range(0..world.len()))
        .unwrap();
    let view = WorldView::new::<(Relation<Next>,)>(&world);
    let job = jobs.spawn(JobLocation::UnnamedWorker, async move {
        fn search(
            source: Entity,
            target: Entity,
            view: &WorldView,
            visited: &mut HashSet<Entity>,
        ) -> bool {
            if source == target {
                return true;
            }
            if visited.contains(&source) {
                return false;
            }
            visited.insert(source);
            for to in view
                .entity::<true, &Relation<Next>>(source)
                .unwrap()
                .entities()
            {
                if search(to, target, view, visited) {
                    return true;
                }
            }
            false
        }

        let mut visited = HashSet::with_capacity(view.len());
        search(from, to, &view, &mut visited)
    });
    let result = job.wait().unwrap_or_default();

    println!("Are entities {from} and {to} connected: {result}");

    Ok(())
}
