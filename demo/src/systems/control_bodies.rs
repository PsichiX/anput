use crate::{
    components::{Disposable, Visible},
    resources::{Globals, Inputs, SpawnMode},
};
use anput::{
    bundle::DynamicBundle,
    commands::CommandBuffer,
    systems::SystemContext,
    third_party::intuicio_data::managed::ManagedLazy,
    universe::{Local, Res},
};
use anput_physics::{
    collisions::{CollisionMask, CollisionProfile, ContactDetection},
    components::{
        BodyDensityFieldRelation, BodyParentRelation, BodyParticleRelation, ExternalForces,
        LinearVelocity, Mass, ParticleMaterial, PhysicsBody, PhysicsParticle, Position,
    },
    density_fields::{DensityFieldBox, cube::CubeDensityField, sphere::SphereDensityField},
    third_party::vek::{Rgba, Vec2},
};
use send_wrapper::SendWrapper;
use spitfire_draw::utils::Vertex;
use spitfire_glow::graphics::Graphics;
use std::error::Error;

pub fn control_bodies(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (mut commands, graphics, inputs, globals, mut spawn_bodies) = context.fetch::<(
        Res<true, &mut CommandBuffer>,
        Res<true, &SendWrapper<ManagedLazy<Graphics<Vertex>>>>,
        Res<true, &Inputs>,
        Res<true, &Globals>,
        Local<true, &mut SpawnBodies>,
    )>()?;

    let trigger = inputs.mouse_trigger.get();
    if trigger.is_idle() {
        return Ok(());
    }

    let graphics = graphics.read().unwrap();
    let xy = inputs.mouse_xy.get();
    let screen_coords = graphics
        .state
        .main_camera
        .screen_matrix()
        .mul_point(xy.into());
    let world_coords = graphics
        .state
        .main_camera
        .world_matrix()
        .inverted()
        .mul_point(screen_coords);

    if trigger.is_pressed() {
        spawn_bodies.drag_position = Some(world_coords);
    }
    if trigger.is_released()
        && let Some(drag_position) = spawn_bodies.drag_position.take()
    {
        let direction = world_coords - drag_position;
        let spawn_mode = globals.spawn_mode;

        commands.schedule(move |world| {
            let mut bundle = DynamicBundle::default();
            bundle.add_component(PhysicsBody).ok().unwrap();
            bundle.add_component(PhysicsParticle).ok().unwrap();
            bundle
                .add_component(CollisionProfile::default().with_block(CollisionMask::flag(0)))
                .ok()
                .unwrap();
            bundle
                .add_component(ContactDetection::default())
                .ok()
                .unwrap();
            bundle.add_component(Mass::new(1.0)).ok().unwrap();
            bundle
                .add_component(Position::new(drag_position))
                .ok()
                .unwrap();
            bundle
                .add_component(LinearVelocity::new(direction))
                .ok()
                .unwrap();
            bundle
                .add_component(ExternalForces::default())
                .ok()
                .unwrap();
            bundle
                .add_component(ParticleMaterial::default())
                .ok()
                .unwrap();
            bundle.add_component(Rgba::<f32>::red()).ok().unwrap();
            bundle.add_component(Visible).ok().unwrap();
            bundle.add_component(Disposable).ok().unwrap();
            match spawn_mode {
                SpawnMode::Sphere => {
                    bundle
                        .add_component(DensityFieldBox::new(SphereDensityField::<true>::new_hard(
                            1.0, 50.0,
                        )))
                        .ok()
                        .unwrap();
                }
                SpawnMode::Cube => {
                    bundle
                        .add_component(DensityFieldBox::new(CubeDensityField::<true>::new_hard(
                            1.0,
                            50.0.into(),
                        )))
                        .ok()
                        .unwrap();
                }
            }
            let object = world.spawn(bundle)?;
            world.relate::<true, _>(BodyParentRelation, object, object)?;
            world.relate::<true, _>(BodyDensityFieldRelation, object, object)?;
            world.relate::<true, _>(BodyParticleRelation, object, object)?;
            Ok(())
        });
    }

    Ok(())
}

#[derive(Default)]
pub struct SpawnBodies {
    drag_position: Option<Vec2<f32>>,
}
