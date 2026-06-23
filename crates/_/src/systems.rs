use crate::{
    bundle::Bundle,
    component::Component,
    entity::Entity,
    query::{TypedLookupAccess, TypedLookupFetch, TypedQueryFetch, TypedQueryIter},
    scheduler::{SystemGroupChild, SystemName},
    universe::{Res, Universe, UniverseCondition, UniverseFetch},
    world::{World, WorldError},
};
use intuicio_core::{
    context::Context,
    function::{FunctionHandle, FunctionQuery},
    registry::Registry,
    types::TypeQuery,
};
use intuicio_data::managed::{DynamicManaged, DynamicManagedRef};
use std::{
    borrow::Cow,
    error::Error,
    ops::{Deref, DerefMut},
    sync::RwLock,
};

#[derive(Clone, Copy)]
pub struct SystemContext<'a> {
    pub universe: &'a Universe,
    entity: Entity,
}

impl<'a> SystemContext<'a> {
    pub fn new(universe: &'a Universe, entity: Entity) -> Self {
        Self { universe, entity }
    }

    pub fn new_unknown(universe: &'a Universe) -> Self {
        Self {
            universe,
            entity: Default::default(),
        }
    }

    pub fn entity(&self) -> Entity {
        self.entity
    }

    pub fn fetch<Fetch: UniverseFetch<'a>>(&'a self) -> Result<Fetch::Value, Box<dyn Error>> {
        Fetch::fetch(self.universe, self.entity)
    }
}

pub trait System: Component {
    fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>>;

    fn should_run(&self, context: SystemContext) -> bool {
        context
            .universe
            .systems
            .component::<true, SystemRunCondition>(context.entity)
            .map(|condition| condition.evaluate(context))
            .unwrap_or(true)
    }

    fn try_run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        if self.should_run(context) {
            self.run(context)
        } else {
            Ok(())
        }
    }
}

impl<T: Fn(SystemContext) -> Result<(), Box<dyn Error>> + Component> System for T {
    fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        (self)(context)
    }
}

pub struct ScriptedFunctionSystem<const LOCKING: bool> {
    run: FunctionHandle,
}

impl<const LOCKING: bool> ScriptedFunctionSystem<LOCKING> {
    pub fn new(run: FunctionHandle) -> Self {
        Self { run }
    }
}

impl<const LOCKING: bool> System for ScriptedFunctionSystem<LOCKING> {
    fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        let (registry, mut ctx) =
            context.fetch::<(Res<LOCKING, &Registry>, Res<LOCKING, &mut Context>)>()?;
        let entity = DynamicManaged::new(context.entity()).map_err::<Box<dyn Error>, _>(|_| {
            "Could not make managed object out of entity!".into()
        })?;
        let (universe, _universe_lifetime) = DynamicManagedRef::make(context.universe);
        ctx.stack().push(entity);
        ctx.stack().push(universe);
        self.run.invoke(&mut ctx, &registry);
        Ok(())
    }
}

enum ScriptedObjectFunction {
    Name(Cow<'static, str>),
    Handle(FunctionHandle),
}

pub struct ScriptedObjectSystem<const LOCKING: bool> {
    object: DynamicManaged,
    function: RwLock<ScriptedObjectFunction>,
}

impl<const LOCKING: bool> ScriptedObjectSystem<LOCKING> {
    pub fn new(object: DynamicManaged) -> Self {
        Self {
            object,
            function: RwLock::new(ScriptedObjectFunction::Name("run".into())),
        }
    }

    pub fn new_custom(object: DynamicManaged, name: Cow<'static, str>) -> Self {
        Self {
            object,
            function: RwLock::new(ScriptedObjectFunction::Name(name)),
        }
    }
}

impl<const LOCKING: bool> System for ScriptedObjectSystem<LOCKING> {
    fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        let (registry, mut ctx) =
            context.fetch::<(Res<LOCKING, &Registry>, Res<LOCKING, &mut Context>)>()?;
        let mut function = self.function.write().map_err::<Box<dyn Error>, _>(|_| {
            "Could not get write access to scripted object function!".into()
        })?;
        if let ScriptedObjectFunction::Name(name) = &*function {
            *function = ScriptedObjectFunction::Handle(
                registry
                    .find_function(FunctionQuery {
                        name: Some(name.clone()),
                        type_query: Some(TypeQuery {
                            type_hash: Some(*self.object.type_hash()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .ok_or_else::<Box<dyn Error>, _>(|| {
                        "Could not find type of scripted object!".into()
                    })?,
            );
        }
        if let ScriptedObjectFunction::Handle(function) = &*function {
            let entity =
                DynamicManaged::new(context.entity()).map_err::<Box<dyn Error>, _>(|_| {
                    "Could not make managed object out of entity!".into()
                })?;
            let (universe, _universe_lifetime) = DynamicManagedRef::make(context.universe);
            let this = self
                .object
                .borrow()
                .ok_or_else::<Box<dyn Error>, _>(|| "Could not borrow scripted object!".into())?;
            ctx.stack().push(entity);
            ctx.stack().push(universe);
            ctx.stack().push(this);
            function.invoke(&mut ctx, &registry);
            Ok(())
        } else {
            Err("Scripted object function is not resolved into a handle!".into())
        }
    }
}

pub struct SystemObject(Box<dyn System>);

impl SystemObject {
    pub fn new(system: impl System) -> Self {
        Self(Box::new(system))
    }

    pub fn should_run(&self, context: SystemContext) -> bool {
        self.0.should_run(context)
    }

    pub fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        self.0.run(context)
    }

    pub fn try_run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        self.0.try_run(context)
    }
}

pub struct SystemRunCondition(Box<dyn Fn(SystemContext) -> bool + Send + Sync>);

impl SystemRunCondition {
    pub fn new<T: UniverseCondition>() -> Self {
        Self(Box::new(|context| T::evaluate(context)))
    }

    pub fn evaluate(&self, context: SystemContext) -> bool {
        (self.0)(context)
    }
}

#[derive(Default)]
pub struct Systems {
    world: World,
}

impl Deref for Systems {
    type Target = World;

    fn deref(&self) -> &Self::Target {
        &self.world
    }
}

impl DerefMut for Systems {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.world
    }
}

impl Systems {
    pub fn add(
        &mut self,
        system: impl System,
        locals: impl Bundle,
    ) -> Result<Entity, Box<dyn Error>> {
        let result = self.world.spawn((SystemObject::new(system),))?;
        WorldError::allow(
            self.world.insert(result, locals),
            [WorldError::EmptyColumnSet],
            (),
        )?;
        Ok(result)
    }

    pub fn add_locals(
        &mut self,
        entity: Entity,
        bundle: impl Bundle,
    ) -> Result<(), Box<dyn Error>> {
        WorldError::allow(
            self.world.insert(entity, bundle),
            [WorldError::EmptyColumnSet],
            (),
        )?;
        Ok(())
    }

    pub fn run<const LOCKING: bool>(
        &self,
        universe: &Universe,
        entity: Entity,
    ) -> Result<(), Box<dyn Error>> {
        self.world
            .component::<LOCKING, SystemObject>(entity)?
            .run(SystemContext::new(universe, entity))
    }

    pub fn try_run<const LOCKING: bool>(
        &self,
        universe: &Universe,
        entity: Entity,
    ) -> Result<(), Box<dyn Error>> {
        self.world
            .component::<LOCKING, SystemObject>(entity)?
            .try_run(SystemContext::new(universe, entity))
    }

    pub fn run_one_shot<const LOCKING: bool>(
        universe: &Universe,
        system: impl System,
    ) -> Result<(), Box<dyn Error>> {
        system.run(SystemContext::new(universe, Default::default()))
    }

    pub fn try_run_one_shot<const LOCKING: bool>(
        universe: &Universe,
        system: impl System,
    ) -> Result<(), Box<dyn Error>> {
        system.try_run(SystemContext::new(universe, Default::default()))
    }

    pub fn query<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>>(
        &'a self,
    ) -> TypedQueryIter<'a, LOCKING, Fetch> {
        self.world.query::<LOCKING, Fetch>()
    }

    pub fn lookup<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
        &'a self,
    ) -> TypedLookupAccess<'a, LOCKING, Fetch> {
        self.world.lookup_access::<LOCKING, Fetch>()
    }

    pub fn lookup_one<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
        &'a self,
        entity: Entity,
    ) -> Option<Fetch::ValueOne> {
        self.world.lookup_one::<LOCKING, Fetch>(entity)
    }

    pub fn find_by<const LOCKING: bool, T: Component + PartialEq>(
        &self,
        data: &T,
    ) -> Option<Entity> {
        self.world.find_by::<LOCKING, T>(data)
    }

    pub fn find_with<const LOCKING: bool, T: Component>(
        &self,
        f: impl Fn(&T) -> bool,
    ) -> Option<Entity> {
        self.world.find_with::<LOCKING, T>(f)
    }

    pub fn find_by_path<'a, const LOCKING: bool>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Option<Entity> {
        let mut path = path.into_iter();
        let part = path.next()?;
        let mut entity = self
            .world
            .find_with::<LOCKING, SystemName>(|name| name.as_str() == part)?;
        for part in path {
            entity = self
                .world
                .relations_outgoing::<LOCKING, SystemGroupChild>(entity)
                .find_map(|(_, _, to)| {
                    if self
                        .world
                        .component::<LOCKING, SystemName>(to)
                        .ok()?
                        .as_str()
                        == part
                    {
                        Some(to)
                    } else {
                        None
                    }
                })?;
        }
        Some(entity)
    }
}
