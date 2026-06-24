use crate::{
    bundle::Bundle,
    commands::CommandBuffer,
    component::{Component, ComponentRef, ComponentRefMut},
    entity::Entity,
    processor::WorldProcessor,
    query::{Lookup, Query, TypedLookupFetch, TypedQueryFetch},
    resources::Resources,
    systems::{System, SystemContext, Systems},
    world::World,
};
use intuicio_core::{context::Context, registry::Registry};
use intuicio_framework_serde::SerializationRegistry;
use std::{error::Error, marker::PhantomData};

pub trait UniverseFetch<'a> {
    type Value;

    fn fetch(universe: &'a Universe, system: Entity) -> Result<Self::Value, Box<dyn Error>>;
}

impl UniverseFetch<'_> for Entity {
    type Value = Entity;

    fn fetch(_: &Universe, entity: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(entity)
    }
}

impl<'a> UniverseFetch<'a> for &'a Universe {
    type Value = &'a Universe;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe)
    }
}

impl<'a> UniverseFetch<'a> for &'a World {
    type Value = &'a World;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(&universe.simulation)
    }
}

impl<'a> UniverseFetch<'a> for &'a Resources {
    type Value = &'a Resources;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(&universe.resources)
    }
}

impl<'a> UniverseFetch<'a> for &'a Systems {
    type Value = &'a Systems;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(&universe.systems)
    }
}

pub struct Res<const LOCKING: bool, T>(PhantomData<fn() -> T>);

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Res<LOCKING, &'a T> {
    type Value = ComponentRef<'a, LOCKING, T>;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        universe.resources.get()
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Res<LOCKING, &'a mut T> {
    type Value = ComponentRefMut<'a, LOCKING, T>;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        universe.resources.get_mut()
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Res<LOCKING, Option<&'a T>> {
    type Value = Option<ComponentRef<'a, LOCKING, T>>;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.resources.get().ok())
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Res<LOCKING, Option<&'a mut T>> {
    type Value = Option<ComponentRefMut<'a, LOCKING, T>>;

    fn fetch(universe: &'a Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.resources.get_mut().ok())
    }
}

pub struct Local<const LOCKING: bool, T>(PhantomData<fn() -> T>);

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Local<LOCKING, &'a T> {
    type Value = ComponentRef<'a, LOCKING, T>;

    fn fetch(universe: &'a Universe, system: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.systems.component(system)?)
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Local<LOCKING, &'a mut T> {
    type Value = ComponentRefMut<'a, LOCKING, T>;

    fn fetch(universe: &'a Universe, system: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.systems.component_mut(system)?)
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a> for Local<LOCKING, Option<&'a T>> {
    type Value = Option<ComponentRef<'a, LOCKING, T>>;

    fn fetch(universe: &'a Universe, system: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.systems.component(system).ok())
    }
}

impl<'a, const LOCKING: bool, T: Component> UniverseFetch<'a>
    for Local<LOCKING, Option<&'a mut T>>
{
    type Value = Option<ComponentRefMut<'a, LOCKING, T>>;

    fn fetch(universe: &'a Universe, system: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(universe.systems.component_mut(system).ok())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> UniverseFetch<'a>
    for Query<'a, LOCKING, Fetch>
{
    type Value = Query<'a, LOCKING, Fetch>;

    fn fetch(_: &Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(Query::<LOCKING, Fetch>::default())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> UniverseFetch<'a>
    for Lookup<'a, LOCKING, Fetch>
{
    type Value = Lookup<'a, LOCKING, Fetch>;

    fn fetch(_: &Universe, _: Entity) -> Result<Self::Value, Box<dyn Error>> {
        Ok(Lookup::<LOCKING, Fetch>::default())
    }
}

macro_rules! impl_universe_fetch_tuple {
    ($($type:ident),+) => {
        impl<'a, $($type: UniverseFetch<'a>),+> UniverseFetch<'a> for ($($type,)+) {
            type Value = ($($type::Value,)+);

            fn fetch(universe: &'a Universe, entity: Entity) -> Result<Self::Value, Box<dyn Error>> {
                Ok(($($type::fetch(universe, entity)?,)+))
            }
        }
    };
}

impl_universe_fetch_tuple!(A);
impl_universe_fetch_tuple!(A, B);
impl_universe_fetch_tuple!(A, B, C);
impl_universe_fetch_tuple!(A, B, C, D);
impl_universe_fetch_tuple!(A, B, C, D, E);
impl_universe_fetch_tuple!(A, B, C, D, E, F);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_universe_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

pub trait UniverseCondition {
    fn evaluate(context: SystemContext) -> bool;
}

pub struct NegateUniverseCondition<T: UniverseCondition>(PhantomData<fn() -> T>);

impl<T: UniverseCondition> UniverseCondition for NegateUniverseCondition<T> {
    fn evaluate(context: SystemContext) -> bool {
        !T::evaluate(context)
    }
}

pub struct ResourceDidChanged<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ResourceDidChanged<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.resources.did_changed::<T>()
    }
}

pub struct ResourceAdded<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ResourceAdded<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.resources.added().has_component::<T>()
    }
}

pub struct ResourceRemoved<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ResourceRemoved<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.resources.removed().has_component::<T>()
    }
}

pub struct ResourceUpdated<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ResourceUpdated<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .resources
            .updated()
            .map(|changes| changes.has_component::<T>())
            .unwrap_or_default()
    }
}

pub struct ComponentDidChanged<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ComponentDidChanged<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.simulation.component_did_changed::<T>()
    }
}

pub struct ComponentAdded<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ComponentAdded<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.simulation.added().has_component::<T>()
    }
}

pub struct ComponentRemoved<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ComponentRemoved<T> {
    fn evaluate(context: SystemContext) -> bool {
        context.universe.simulation.removed().has_component::<T>()
    }
}

pub struct ComponentUpdated<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for ComponentUpdated<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .simulation
            .updated()
            .map(|changes| changes.has_component::<T>())
            .unwrap_or_default()
    }
}

pub struct SystemLocalDidChanged<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for SystemLocalDidChanged<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .systems
            .entity_component_did_changed::<T>(context.entity())
    }
}

pub struct SystemLocalAdded<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for SystemLocalAdded<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .systems
            .added()
            .has_entity_component::<T>(context.entity())
    }
}

pub struct SystemLocalRemoved<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for SystemLocalRemoved<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .systems
            .removed()
            .has_entity_component::<T>(context.entity())
    }
}

pub struct SystemLocalUpdated<T: Component>(PhantomData<fn() -> T>);

impl<T: Component> UniverseCondition for SystemLocalUpdated<T> {
    fn evaluate(context: SystemContext) -> bool {
        context
            .universe
            .systems
            .updated()
            .map(|changes| changes.has_entity_component::<T>(context.entity()))
            .unwrap_or_default()
    }
}

macro_rules! impl_universe_condition_tuple {
    ($($type:ident),+) => {
        impl<$($type: UniverseCondition),+> UniverseCondition for ($($type,)+) {
            fn evaluate(context: SystemContext) -> bool {
                $($type::evaluate(context))&&+
            }
        }
    };
}

impl_universe_condition_tuple!(A);
impl_universe_condition_tuple!(A, B);
impl_universe_condition_tuple!(A, B, C);
impl_universe_condition_tuple!(A, B, C, D);
impl_universe_condition_tuple!(A, B, C, D, E);
impl_universe_condition_tuple!(A, B, C, D, E, F);
impl_universe_condition_tuple!(A, B, C, D, E, F, G);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_universe_condition_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

#[derive(Default)]
pub struct Universe {
    pub simulation: World,
    pub systems: Systems,
    pub resources: Resources,
}

impl Universe {
    pub fn new(simulation: World) -> Self {
        Self {
            simulation,
            resources: Default::default(),
            systems: Default::default(),
        }
    }

    pub fn with_plugin<T: Plugin + 'static>(mut self, plugin: T) -> Self {
        plugin.install(&mut self.simulation, &mut self.systems, &mut self.resources);
        self
    }

    pub fn with_basics(
        self,
        stack_capacity: usize,
        registers_capacity: usize,
    ) -> Result<Self, Box<dyn Error>> {
        self.with_resource(CommandBuffer::default())?
            .with_resource(Registry::default().with_basic_types())?
            .with_resource(Context::new(stack_capacity, registers_capacity))?
            .with_resource(WorldProcessor::default())?
            .with_resource(SerializationRegistry::default().with_basic_types())
    }

    pub fn with_resource(mut self, resource: impl Component) -> Result<Self, Box<dyn Error>> {
        self.resources.add((resource,))?;
        Ok(self)
    }

    pub fn with_system(
        mut self,
        system: impl System,
        locals: impl Bundle,
    ) -> Result<Self, Box<dyn Error>> {
        self.systems.add(system, locals)?;
        Ok(self)
    }

    pub fn clear_changes(&mut self) {
        self.simulation.clear_changes();
        self.resources.clear_changes();
        self.systems.clear_changes();
    }

    pub fn execute_commands<const LOCKING: bool>(&mut self) -> Result<(), Box<dyn Error>> {
        for commands in self.resources.query::<LOCKING, &mut CommandBuffer>() {
            commands.execute(&mut self.simulation)?;
        }
        for commands in self.systems.query::<LOCKING, &mut CommandBuffer>() {
            commands.execute(&mut self.simulation)?;
        }
        Ok(())
    }
}

pub trait Plugin: Send + Sync {
    fn install(self, simulation: &mut World, systems: &mut Systems, resources: &mut Resources);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{GraphScheduler, GraphSchedulerPlugin, SystemParallelize};
    use moirai::jobs::Jobs;

    #[test]
    fn test_universe_parallelized_scheduler() {
        struct A(f32);
        struct B(f32);
        struct C(f32);
        struct D(f32);
        struct E(f32);

        fn ab(context: SystemContext) -> Result<(), Box<dyn Error>> {
            let (world, query) = context.fetch::<(&World, Query<true, (&mut A, &mut B)>)>()?;

            for (a, b) in query.query(world) {
                std::mem::swap(&mut a.0, &mut b.0);
            }

            Ok(())
        }

        fn cd(context: SystemContext) -> Result<(), Box<dyn Error>> {
            let (world, query) = context.fetch::<(&World, Query<true, (&mut C, &mut D)>)>()?;

            for (c, d) in query.query(world) {
                std::mem::swap(&mut c.0, &mut d.0);
            }

            Ok(())
        }

        fn ce(context: SystemContext) -> Result<(), Box<dyn Error>> {
            let (world, query) = context.fetch::<(&World, Query<true, (&mut C, &mut E)>)>()?;

            for (c, e) in query.query(world) {
                std::mem::swap(&mut c.0, &mut e.0);
            }

            Ok(())
        }

        let mut universe = Universe::default().with_plugin(
            GraphSchedulerPlugin::<true>::default()
                .plugin_setup(|plugin| {
                    plugin
                        .name("root")
                        .system_setup(ab, |system| {
                            system.name("ab").local(SystemParallelize::AnyWorker)
                        })
                        .system_setup(cd, |system| {
                            system.name("cd").local(SystemParallelize::AnyWorker)
                        })
                })
                .system_setup(ce, |system| {
                    system.name("ce").local(SystemParallelize::AnyWorker)
                }),
        );

        for _ in 0..10 {
            universe.simulation.spawn((A(0.0), B(0.0))).unwrap();
        }
        for _ in 0..10 {
            universe.simulation.spawn((A(0.0), B(0.0), C(0.0))).unwrap();
        }
        for _ in 0..10 {
            universe
                .simulation
                .spawn((A(0.0), B(0.0), C(0.0), D(0.0)))
                .unwrap();
        }
        for _ in 0..10 {
            universe
                .simulation
                .spawn((A(0.0), B(0.0), C(0.0), E(0.0)))
                .unwrap();
        }

        let jobs = Jobs::default();
        GraphScheduler::<true>.run(&jobs, &mut universe).unwrap();
    }
}
