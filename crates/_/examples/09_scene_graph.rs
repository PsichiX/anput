use anput::{
    scene_graph::{SceneChild, SceneNode},
    world::World,
};
use std::error::Error;

#[derive(Debug, Default, Clone, Copy)]
struct Gold(pub usize);

fn main() -> Result<(), Box<dyn Error>> {
    let mut world = World::default();

    // Setup capital city.
    let eldoria = SceneNode::spawn(&mut world, ("Eldoria", Gold(1000)))?;
    // Setup dependent cities.
    eldoria.spawn_child::<true>(&mut world, ("Thalnar", Gold(230)))?;
    eldoria.spawn_child::<true>(&mut world, ("Virella", Gold(50)))?;

    // Pay taxes.
    {
        let mut collected_money = 0;
        for city in eldoria.children::<true>(&world) {
            let mut gold = city.component_mut::<true, Gold>(&world)?;
            let tax = gold.0 / 10;
            collected_money += tax;
            gold.0 -= tax;
        }
        let mut gold = eldoria.component_mut::<true, Gold>(&world)?;
        gold.0 += collected_money;
    }

    // Show cities treasury report.
    for (name, gold) in eldoria.traverse::<true, SceneChild, (&&'static str, &Gold)>(&world) {
        println!("City {:?} has {} gold", *name, gold.0);
    }

    Ok(())
}
