use crate::{
    archetype::{
        Archetype, ArchetypeColumnAccess, ArchetypeDynamicColumnAccess, ArchetypeDynamicColumnItem,
        ArchetypeDynamicColumnIter, ArchetypeError,
    },
    component::{Component, ComponentRef, ComponentRefMut},
    entity::{Entity, EntityDenseMap},
    view::WorldView,
    world::World,
};
use intuicio_data::type_hash::TypeHash;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    marker::PhantomData,
    sync::Arc,
};

#[derive(Debug)]
pub enum QueryError {
    Archetype(ArchetypeError),
    TryingToReadUnavailableType { type_hash: TypeHash },
    TryingToWriteUnavailableType { type_hash: TypeHash },
}

impl Error for QueryError {}

impl From<ArchetypeError> for QueryError {
    fn from(value: ArchetypeError) -> Self {
        Self::Archetype(value)
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archetype(archetype) => write!(f, "World archetype: {archetype}"),
            Self::TryingToReadUnavailableType { type_hash } => {
                write!(f, "Trying to read unavailable type: {type_hash:?}")
            }
            Self::TryingToWriteUnavailableType { type_hash } => {
                write!(f, "Trying to write unavailable type: {type_hash:?}")
            }
        }
    }
}

pub trait TypedQueryFetch<'a, const LOCKING: bool> {
    type Value;
    type Access;

    fn does_accept_archetype(archetype: &Archetype) -> bool;
    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError>;
    fn fetch(access: &mut Self::Access) -> Option<Self::Value>;

    #[allow(unused_variables)]
    fn unique_access(output: &mut HashSet<TypeHash>) {}
}

pub trait TypedLookupFetch<'a, const LOCKING: bool> {
    type Value;
    type ValueOne;
    type Access;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access>;
    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value>;
    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne>;

    #[allow(unused_variables)]
    fn unique_access(output: &mut HashSet<TypeHash>) {}
}

pub trait TypedRelationLookupFetch<'a> {
    type Value;
    type Access;

    fn access(world: &'a World, entity: Entity) -> Self::Access;
    fn fetch(access: &mut Self::Access) -> Option<Self::Value>;
}

pub trait TypedRelationLookupTransform<'a> {
    type Input;
    type Output;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output>;
}

pub struct Query<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>>(
    PhantomData<fn() -> &'a Fetch>,
);

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> Default
    for Query<'a, LOCKING, Fetch>
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> Clone
    for Query<'a, LOCKING, Fetch>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> Copy
    for Query<'a, LOCKING, Fetch>
{
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> Query<'a, LOCKING, Fetch> {
    pub fn query(&self, world: &'a World) -> TypedQueryIter<'a, LOCKING, Fetch> {
        world.query::<'a, LOCKING, Fetch>()
    }

    pub fn query_view(&self, view: &'a WorldView) -> TypedQueryIter<'a, LOCKING, Fetch> {
        view.query::<'a, LOCKING, Fetch>()
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> TypedQueryFetch<'a, LOCKING>
    for Query<'a, LOCKING, Fetch>
{
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        Fetch::does_accept_archetype(archetype)
    }

    fn access(_: &Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> TypedLookupFetch<'a, LOCKING>
    for Query<'a, LOCKING, Fetch>
{
    type Value = ();
    type ValueOne = ();
    type Access = ();

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if Fetch::does_accept_archetype(archetype) {
            Some(())
        } else {
            None
        }
    }

    fn fetch(_: &mut Self::Access, _: Entity) -> Option<Self::Value> {
        Some(())
    }

    fn fetch_one(_: &World, _: Entity) -> Option<Self::ValueOne> {
        Some(())
    }
}

pub struct Lookup<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
    PhantomData<fn() -> &'a Fetch>,
);

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> Default
    for Lookup<'a, LOCKING, Fetch>
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> Clone
    for Lookup<'a, LOCKING, Fetch>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> Copy
    for Lookup<'a, LOCKING, Fetch>
{
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> Lookup<'a, LOCKING, Fetch> {
    pub fn lookup(
        &self,
        world: &'a World,
        entities: impl IntoIterator<Item = Entity> + 'a,
    ) -> TypedLookupIter<'a, LOCKING, Fetch> {
        world.lookup::<'a, LOCKING, Fetch>(entities)
    }

    pub fn lookup_view(
        &self,
        view: &'a WorldView,
        entities: impl IntoIterator<Item = Entity> + 'a,
    ) -> TypedLookupIter<'a, LOCKING, Fetch> {
        view.lookup::<'a, LOCKING, Fetch>(entities)
    }

    pub fn lookup_access(&self, world: &'a World) -> TypedLookupAccess<'a, LOCKING, Fetch> {
        world.lookup_access::<'a, LOCKING, Fetch>()
    }

    pub fn lookup_access_view(&self, view: &'a WorldView) -> TypedLookupAccess<'a, LOCKING, Fetch> {
        view.lookup_access::<'a, LOCKING, Fetch>()
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedLookupFetch<'a, LOCKING>
    for Lookup<'a, LOCKING, Fetch>
{
    type Value = ();
    type ValueOne = ();
    type Access = ();

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if Fetch::try_access(archetype).is_some() {
            Some(())
        } else {
            None
        }
    }

    fn fetch(_: &mut Self::Access, _: Entity) -> Option<Self::Value> {
        Some(())
    }

    fn fetch_one(_: &World, _: Entity) -> Option<Self::ValueOne> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedQueryFetch<'a, LOCKING>
    for Lookup<'a, LOCKING, Fetch>
{
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        let archetype = unsafe { std::mem::transmute::<&Archetype, &Archetype>(archetype) };
        Fetch::try_access(archetype).is_some()
    }

    fn access(_: &'a Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<const LOCKING: bool> TypedQueryFetch<'_, LOCKING> for () {
    type Value = ();
    type Access = ();

    fn does_accept_archetype(_: &Archetype) -> bool {
        true
    }

    fn access(_: &Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<const LOCKING: bool> TypedLookupFetch<'_, LOCKING> for () {
    type Value = ();
    type ValueOne = ();
    type Access = ();

    fn try_access(_: &Archetype) -> Option<Self::Access> {
        Some(())
    }

    fn fetch(_: &mut Self::Access, _: Entity) -> Option<Self::Value> {
        Some(())
    }

    fn fetch_one(_: &World, _: Entity) -> Option<Self::ValueOne> {
        Some(())
    }
}

impl<'a, const LOCKING: bool> TypedQueryFetch<'a, LOCKING> for Entity {
    type Value = Entity;
    type Access = Box<dyn Iterator<Item = Entity> + 'a>;

    fn does_accept_archetype(_: &Archetype) -> bool {
        true
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        Ok(Box::new(archetype.entities().iter()))
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

impl<'a, const LOCKING: bool> TypedLookupFetch<'a, LOCKING> for Entity {
    type Value = Entity;
    type ValueOne = Entity;
    type Access = &'a EntityDenseMap;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        Some(archetype.entities())
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if access.contains(entity) {
            Some(entity)
        } else {
            None
        }
    }

    fn fetch_one(_: &World, entity: Entity) -> Option<Self::ValueOne> {
        Some(entity)
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedQueryFetch<'a, LOCKING> for &'a T {
    type Value = &'a T;
    type Access = Box<dyn Iterator<Item = &'a T> + 'a>;

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        archetype.has_type(TypeHash::of::<T>())
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        Ok(Box::new(archetype.column_read_iter::<LOCKING, T>()?))
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for &'a T {
    type Value = &'a T;
    type ValueOne = ComponentRef<'a, LOCKING, T>;
    type Access = (&'a EntityDenseMap, ArchetypeColumnAccess<'a, LOCKING, T>);

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if archetype.has_type(TypeHash::of::<T>()) {
            Some((
                archetype.entities(),
                archetype.column::<LOCKING, T>(false).ok()?,
            ))
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if let Some(index) = access.0.index_of(entity) {
            access
                .1
                .read(index)
                .map(|value| unsafe { std::mem::transmute(value) })
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        world.component::<LOCKING, T>(entity).ok()
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedQueryFetch<'a, LOCKING> for &'a mut T {
    type Value = &'a mut T;
    type Access = Box<dyn Iterator<Item = &'a mut T> + 'a>;

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        archetype.has_type(TypeHash::of::<T>())
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        Ok(Box::new(archetype.column_write_iter::<LOCKING, T>()?))
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for &'a mut T {
    type Value = &'a mut T;
    type ValueOne = ComponentRefMut<'a, LOCKING, T>;
    type Access = (&'a EntityDenseMap, ArchetypeColumnAccess<'a, LOCKING, T>);

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if archetype.has_type(TypeHash::of::<T>()) {
            Some((
                archetype.entities(),
                archetype.column::<LOCKING, T>(true).ok()?,
            ))
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if let Some(index) = access.0.index_of(entity) {
            access
                .1
                .write(index)
                .map(|value| unsafe { std::mem::transmute(value) })
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        world.component_mut::<LOCKING, T>(entity).ok()
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedQueryFetch<'a, LOCKING> for Option<&'a T> {
    type Value = Option<&'a T>;
    type Access = Option<Box<dyn Iterator<Item = &'a T> + 'a>>;

    fn does_accept_archetype(_: &Archetype) -> bool {
        true
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        match archetype.column_read_iter::<LOCKING, T>().ok() {
            Some(value) => Ok(Some(Box::new(value))),
            None => Ok(None),
        }
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        match access {
            // TODO: might be fucked up here.
            Some(access) => Some(access.next()),
            None => Some(None),
        }
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for Option<&'a T> {
    type Value = Option<&'a T>;
    type ValueOne = Option<ComponentRef<'a, LOCKING, T>>;
    type Access = Option<(&'a EntityDenseMap, ArchetypeColumnAccess<'a, LOCKING, T>)>;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        match archetype.column::<LOCKING, T>(false).ok() {
            Some(value) => Some(Some((archetype.entities(), value))),
            None => Some(None),
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        match access {
            // TODO: might be fucked up here.
            Some(access) => Some(if let Some(index) = access.0.index_of(entity) {
                access
                    .1
                    .read(index)
                    .map(|value| unsafe { std::mem::transmute(value) })
            } else {
                None
            }),
            None => Some(None),
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        Some(world.component::<LOCKING, T>(entity).ok())
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedQueryFetch<'a, LOCKING> for Option<&'a mut T> {
    type Value = Option<&'a mut T>;
    type Access = Option<Box<dyn Iterator<Item = &'a mut T> + 'a>>;

    fn does_accept_archetype(_: &Archetype) -> bool {
        true
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        match archetype.column_write_iter::<LOCKING, T>().ok() {
            Some(value) => Ok(Some(Box::new(value))),
            None => Ok(None),
        }
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        match access {
            // TODO: might be fucked up here.
            Some(access) => Some(access.next()),
            None => Some(None),
        }
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for Option<&'a mut T> {
    type Value = Option<&'a mut T>;
    type ValueOne = Option<ComponentRefMut<'a, LOCKING, T>>;
    type Access = Option<(&'a EntityDenseMap, ArchetypeColumnAccess<'a, LOCKING, T>)>;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        match archetype.column::<LOCKING, T>(true).ok() {
            Some(value) => Some(Some((archetype.entities(), value))),
            None => Some(None),
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        match access {
            // TODO: might be fucked up here.
            Some(access) => Some(if let Some(index) = access.0.index_of(entity) {
                access
                    .1
                    .write(index)
                    .map(|value| unsafe { std::mem::transmute(value) })
            } else {
                None
            }),
            None => Some(None),
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        Some(world.component_mut::<LOCKING, T>(entity).ok())
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

pub struct Satisfy<T>(PhantomData<fn() -> T>);

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> TypedQueryFetch<'a, LOCKING>
    for Satisfy<Fetch>
{
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        Fetch::does_accept_archetype(archetype)
    }

    fn access(_: &'_ Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedLookupFetch<'a, LOCKING>
    for Satisfy<Fetch>
{
    type Value = ();
    type ValueOne = ();
    type Access = &'a EntityDenseMap;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if Fetch::try_access(archetype).is_some() {
            Some(archetype.entities())
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if access.contains(entity) {
            Some(())
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        if Fetch::fetch_one(world, entity).is_some() {
            Some(())
        } else {
            None
        }
    }
}

pub struct DoesNotSatisfy<T>(PhantomData<fn() -> T>);

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> TypedQueryFetch<'a, LOCKING>
    for DoesNotSatisfy<Fetch>
{
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        !Fetch::does_accept_archetype(archetype)
    }

    fn access(_: &'_ Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedLookupFetch<'a, LOCKING>
    for DoesNotSatisfy<Fetch>
{
    type Value = ();
    type ValueOne = ();
    type Access = &'a EntityDenseMap;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if Fetch::try_access(archetype).is_none() {
            Some(archetype.entities())
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if access.contains(entity) {
            Some(())
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        if Fetch::fetch_one(world, entity).is_none() {
            Some(())
        } else {
            None
        }
    }
}

pub struct Include<T: Component>(PhantomData<fn() -> T>);

impl<const LOCKING: bool, T: Component> TypedQueryFetch<'_, LOCKING> for Include<T> {
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        archetype.has_type(TypeHash::of::<T>())
    }

    fn access(_: &Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for Include<T> {
    type Value = ();
    type ValueOne = ();
    type Access = &'a EntityDenseMap;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if archetype.has_type(TypeHash::of::<T>()) {
            Some(archetype.entities())
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if access.contains(entity) {
            Some(())
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        if world.has_entity_component::<T>(entity) {
            Some(())
        } else {
            None
        }
    }
}

pub struct Exclude<T: Component>(PhantomData<fn() -> T>);

impl<const LOCKING: bool, T: Component> TypedQueryFetch<'_, LOCKING> for Exclude<T> {
    type Value = ();
    type Access = ();

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        !archetype.has_type(TypeHash::of::<T>())
    }

    fn access(_: &Archetype) -> Result<Self::Access, QueryError> {
        Ok(())
    }

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for Exclude<T> {
    type Value = ();
    type ValueOne = ();
    type Access = &'a EntityDenseMap;

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if !archetype.has_type(TypeHash::of::<T>()) {
            Some(archetype.entities())
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if access.contains(entity) {
            Some(())
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        if !world.has_entity_component::<T>(entity) {
            Some(())
        } else {
            None
        }
    }
}

pub struct Update<T: Component>(PhantomData<fn() -> T>);

pub struct UpdatedAccess<'a, T>(Entity, &'a mut T);

impl<'a, T> UpdatedAccess<'a, T> {
    pub fn entity(&self) -> Entity {
        self.0
    }

    pub fn read(&'a self) -> &'a T {
        self.1
    }

    pub fn write(&'a mut self) -> &'a mut T {
        self.1
    }

    pub fn notify(&self, world: &World) {
        world.update::<T>(self.0);
    }

    pub fn write_notified(&'a mut self, world: &World) -> &'a mut T {
        self.notify(world);
        self.write()
    }
}

pub struct UpdatedAccessComponent<'a, const LOCKING: bool, T: Component>(
    Entity,
    ComponentRefMut<'a, LOCKING, T>,
);

impl<'a, const LOCKING: bool, T: Component> UpdatedAccessComponent<'a, LOCKING, T> {
    pub fn entity(&self) -> Entity {
        self.0
    }

    pub fn read(&'a self) -> &'a T {
        &self.1
    }

    pub fn write(&'a mut self) -> &'a mut T {
        &mut self.1
    }

    pub fn notify(&self, world: &World) {
        world.update::<T>(self.0);
    }

    pub fn write_notified(&'a mut self, world: &World) -> &'a mut T {
        self.notify(world);
        self.write()
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedQueryFetch<'a, LOCKING> for Update<T> {
    type Value = UpdatedAccess<'a, T>;
    type Access = Box<dyn Iterator<Item = (Entity, &'a mut T)> + 'a>;

    fn does_accept_archetype(archetype: &Archetype) -> bool {
        archetype.has_type(TypeHash::of::<T>())
    }

    fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
        Ok(Box::new(
            archetype
                .entities()
                .iter()
                .zip(archetype.column_write_iter::<LOCKING, T>()?),
        ))
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access
            .next()
            .map(|(entity, data)| UpdatedAccess(entity, data))
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

impl<'a, const LOCKING: bool, T: Component> TypedLookupFetch<'a, LOCKING> for Update<T> {
    type Value = UpdatedAccess<'a, T>;
    type ValueOne = UpdatedAccessComponent<'a, LOCKING, T>;
    type Access = (&'a EntityDenseMap, ArchetypeColumnAccess<'a, LOCKING, T>);

    fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
        if archetype.has_type(TypeHash::of::<T>()) {
            Some((
                archetype.entities(),
                archetype.column::<LOCKING, T>(true).ok()?,
            ))
        } else {
            None
        }
    }

    fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
        if let Some(index) = access.0.index_of(entity) {
            access
                .1
                .write(index)
                .map(|value| unsafe { std::mem::transmute(value) })
                .map(|data| UpdatedAccess(entity, data))
        } else {
            None
        }
    }

    fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
        world
            .component_mut::<LOCKING, T>(entity)
            .ok()
            .map(|data| UpdatedAccessComponent(entity, data))
    }

    fn unique_access(output: &mut HashSet<TypeHash>) {
        output.insert(TypeHash::of::<T>());
    }
}

impl<'a> TypedRelationLookupFetch<'a> for () {
    type Value = ();
    type Access = ();

    fn access(_: &'a World, _: Entity) -> Self::Access {}

    fn fetch(_: &mut Self::Access) -> Option<Self::Value> {
        Some(())
    }
}

impl<'a> TypedRelationLookupFetch<'a> for Entity {
    type Value = Entity;
    type Access = Option<Entity>;

    fn access(_: &'a World, entity: Entity) -> Self::Access {
        Some(entity)
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.take()
    }
}

impl<'a, const LOCKING: bool, Fetch> TypedRelationLookupFetch<'a> for Lookup<'a, LOCKING, Fetch>
where
    Fetch: TypedLookupFetch<'a, LOCKING>,
{
    type Value = Fetch::ValueOne;
    type Access = Option<Fetch::ValueOne>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        world.lookup_one::<LOCKING, Fetch>(entity)
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.take()
    }
}

pub struct Related<'a, const LOCKING: bool, T, Transform>(PhantomData<fn() -> &'a (T, Transform)>)
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = Entity>;

impl<'a, const LOCKING: bool, T, Transform> TypedRelationLookupFetch<'a>
    for Related<'a, LOCKING, T, Transform>
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = Entity>,
{
    type Value = Transform::Output;
    type Access = Box<dyn Iterator<Item = Self::Value> + 'a>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        Box::new(
            world
                .relations_outgoing::<LOCKING, T>(entity)
                .flat_map(|(_, _, entity)| Transform::transform(world, entity)),
        )
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

pub struct RelatedPair<'a, const LOCKING: bool, T, Transform>(
    PhantomData<fn() -> &'a (T, Transform)>,
)
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = (Entity, Entity)>;

impl<'a, const LOCKING: bool, T, Transform> TypedRelationLookupFetch<'a>
    for RelatedPair<'a, LOCKING, T, Transform>
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = (Entity, Entity)>,
{
    type Value = Transform::Output;
    type Access = Box<dyn Iterator<Item = Self::Value> + 'a>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        Box::new(
            world
                .relations_outgoing::<LOCKING, T>(entity)
                .flat_map(|(from, _, to)| Transform::transform(world, (from, to))),
        )
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

pub struct Traverse<'a, const LOCKING: bool, T, Transform>(PhantomData<fn() -> &'a (T, Transform)>)
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = Entity>;

impl<'a, const LOCKING: bool, T, Transform> TypedRelationLookupFetch<'a>
    for Traverse<'a, LOCKING, T, Transform>
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = Entity>,
{
    type Value = Transform::Output;
    type Access = Box<dyn Iterator<Item = Self::Value> + 'a>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        Box::new(
            world
                .traverse_outgoing::<LOCKING, T>([entity])
                .flat_map(|(_, to)| Transform::transform(world, to)),
        )
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

pub struct TraversePair<'a, const LOCKING: bool, T, Transform>(
    PhantomData<fn() -> &'a (T, Transform)>,
)
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = (Entity, Entity)>;

impl<'a, const LOCKING: bool, T, Transform> TypedRelationLookupFetch<'a>
    for TraversePair<'a, LOCKING, T, Transform>
where
    T: Component,
    Transform: TypedRelationLookupTransform<'a, Input = (Entity, Entity)>,
{
    type Value = Transform::Output;
    type Access = Box<dyn Iterator<Item = Self::Value> + 'a>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        Box::new(
            world
                .traverse_outgoing::<LOCKING, T>([entity])
                .flat_map(|(from, to)| Transform::transform(world, (from, to))),
        )
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

pub struct Join<'a, A, B>(PhantomData<fn() -> &'a (A, B)>)
where
    A: TypedRelationLookupFetch<'a>,
    B: TypedRelationLookupFetch<'a>;

impl<'a, A, B> TypedRelationLookupFetch<'a> for Join<'a, A, B>
where
    A: TypedRelationLookupFetch<'a>,
    B: TypedRelationLookupFetch<'a>,
{
    type Value = (Arc<A::Value>, B::Value);
    type Access = Box<dyn Iterator<Item = Self::Value> + 'a>;

    fn access(world: &'a World, entity: Entity) -> Self::Access {
        Box::new(
            TypedRelationLookupIter::<A>::access(A::access(world, entity)).flat_map(move |a| {
                let a = Arc::new(a);
                TypedRelationLookupIter::<B>::access(B::access(world, entity))
                    .map(move |b| (a.clone(), b))
            }),
        )
    }

    fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
        access.next()
    }
}

impl<'a> TypedRelationLookupTransform<'a> for Entity {
    type Input = Entity;
    type Output = Entity;

    fn transform(_: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        std::iter::once(input)
    }
}

pub struct Is<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
    PhantomData<fn() -> &'a Fetch>,
);

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedRelationLookupTransform<'a>
    for Is<'a, LOCKING, Fetch>
{
    type Input = Entity;
    type Output = Entity;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        world
            .lookup_one::<LOCKING, Fetch>(input)
            .is_some()
            .then_some(input)
            .into_iter()
    }
}

pub struct IsNot<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
    PhantomData<fn() -> &'a Fetch>,
);

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedRelationLookupTransform<'a>
    for IsNot<'a, LOCKING, Fetch>
{
    type Input = Entity;
    type Output = Entity;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        world
            .lookup_one::<LOCKING, Fetch>(input)
            .is_none()
            .then_some(input)
            .into_iter()
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> TypedRelationLookupTransform<'a>
    for Lookup<'a, LOCKING, Fetch>
{
    type Input = Entity;
    type Output = Fetch::ValueOne;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        world.lookup_one::<LOCKING, Fetch>(input).into_iter()
    }
}

pub struct Limit<'a, const COUNT: usize, Transform: TypedRelationLookupTransform<'a>>(
    PhantomData<fn() -> &'a Transform>,
);

impl<'a, const COUNT: usize, Transform: TypedRelationLookupTransform<'a>>
    TypedRelationLookupTransform<'a> for Limit<'a, COUNT, Transform>
{
    type Input = Transform::Input;
    type Output = Transform::Output;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        Transform::transform(world, input).take(COUNT)
    }
}

pub type Single<'a, Transform> = Limit<'a, 1, Transform>;

pub struct SelectEntity<const INDEX: usize>;

impl<'a, const INDEX: usize> TypedRelationLookupTransform<'a> for SelectEntity<INDEX> {
    type Input = (Entity, Entity);
    type Output = Entity;

    fn transform(_: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        if INDEX == 0 {
            std::iter::once(input.0)
        } else {
            std::iter::once(input.1)
        }
    }
}

pub struct Follow<'a, const LOCKING: bool, Transform, Fetch>(
    PhantomData<fn() -> &'a (Transform, Fetch)>,
)
where
    Transform: TypedRelationLookupTransform<'a, Input = Entity, Output = Entity>,
    Fetch: TypedRelationLookupFetch<'a>;

impl<'a, const LOCKING: bool, Transform, Fetch> TypedRelationLookupTransform<'a>
    for Follow<'a, LOCKING, Transform, Fetch>
where
    Transform: TypedRelationLookupTransform<'a, Input = Entity, Output = Entity>,
    Fetch: TypedRelationLookupFetch<'a>,
{
    type Input = Entity;
    type Output = Fetch::Value;

    fn transform(world: &'a World, input: Self::Input) -> impl Iterator<Item = Self::Output> {
        Transform::transform(world, input)
            .flat_map(|entity| world.relation_lookup::<LOCKING, Fetch>(entity))
    }
}

macro_rules! impl_typed_query_fetch_tuple {
    ($($type:ident),+) => {
        impl<'a, const LOCKING: bool, $($type: TypedQueryFetch<'a, LOCKING>),+> TypedQueryFetch<'a, LOCKING> for ($($type,)+) {
            type Value = ($($type::Value,)+);
            type Access = ($($type::Access,)+);

            fn does_accept_archetype(archetype: &Archetype) -> bool {
                $($type::does_accept_archetype(archetype))&&+
            }

            fn access(archetype: &'a Archetype) -> Result<Self::Access, QueryError> {
                Ok(($($type::access(archetype)?,)+))
            }

            fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
                #[allow(non_snake_case)]
                let ($($type,)+) = access;
                Some(($($type::fetch($type)?,)+))
            }

            fn unique_access(output: &mut HashSet<TypeHash>) {
                $(
                    $type::unique_access(output);
                )+
            }
        }
    };
}

impl_typed_query_fetch_tuple!(A);
impl_typed_query_fetch_tuple!(A, B);
impl_typed_query_fetch_tuple!(A, B, C);
impl_typed_query_fetch_tuple!(A, B, C, D);
impl_typed_query_fetch_tuple!(A, B, C, D, E);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_typed_query_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

macro_rules! impl_typed_lookup_fetch_tuple {
    ($($type:ident),+) => {
        impl<'a, const LOCKING: bool, $($type: TypedLookupFetch<'a, LOCKING>),+> TypedLookupFetch<'a, LOCKING> for ($($type,)+) {
            type Value = ($($type::Value,)+);
            type ValueOne = ($($type::ValueOne,)+);
            type Access = ($($type::Access,)+);

            fn try_access(archetype: &'a Archetype) -> Option<Self::Access> {
                Some(($($type::try_access(archetype)?,)+))
            }

            fn fetch(access: &mut Self::Access, entity: Entity) -> Option<Self::Value> {
                #[allow(non_snake_case)]
                let ($($type,)+) = access;
                Some(($($type::fetch($type, entity)?,)+))
            }

            fn fetch_one(world: &'a World, entity: Entity) -> Option<Self::ValueOne> {
                Some(($($type::fetch_one(world, entity)?,)+))
            }

            fn unique_access(output: &mut HashSet<TypeHash>) {
                $(
                    $type::unique_access(output);
                )+
            }
        }
    };
}

impl_typed_lookup_fetch_tuple!(A);
impl_typed_lookup_fetch_tuple!(A, B);
impl_typed_lookup_fetch_tuple!(A, B, C);
impl_typed_lookup_fetch_tuple!(A, B, C, D);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_typed_lookup_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

macro_rules! impl_typed_relation_fetch_tuple {
    ($($type:ident),+) => {
        impl<'a, $($type: TypedRelationLookupFetch<'a>),+> TypedRelationLookupFetch<'a> for ($($type,)+) {
            type Value = ($($type::Value,)+);
            type Access = ($($type::Access,)+);

            fn access(world: &'a World, entity: Entity) -> Self::Access {
                ($($type::access(world, entity),)+)
            }

            fn fetch(access: &mut Self::Access) -> Option<Self::Value> {
                #[allow(non_snake_case)]
                let ($($type,)+) = access;
                Some(($($type::fetch($type)?,)+))
            }
        }
    };
}

impl_typed_relation_fetch_tuple!(A);
impl_typed_relation_fetch_tuple!(A, B);
impl_typed_relation_fetch_tuple!(A, B, C);
impl_typed_relation_fetch_tuple!(A, B, C, D);
impl_typed_relation_fetch_tuple!(A, B, C, D, E);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, O);
impl_typed_relation_fetch_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, O, P);

pub struct TypedQueryIter<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> {
    archetypes: Vec<&'a Archetype>,
    index: usize,
    access: Option<Fetch::Access>,
    _phantom: PhantomData<fn() -> Fetch>,
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>>
    TypedQueryIter<'a, LOCKING, Fetch>
{
    pub fn new(world: &'a World) -> Self {
        Self {
            archetypes: world
                .archetypes()
                .filter(|archetype| Fetch::does_accept_archetype(archetype))
                .collect(),
            index: 0,
            access: None,
            _phantom: PhantomData,
        }
    }

    pub fn new_view(view: &'a WorldView) -> Self {
        Self {
            archetypes: view
                .archetypes()
                .filter(|archetype| Fetch::does_accept_archetype(archetype))
                .collect(),
            index: 0,
            access: None,
            _phantom: PhantomData,
        }
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedQueryFetch<'a, LOCKING>> Iterator
    for TypedQueryIter<'a, LOCKING, Fetch>
{
    type Item = Fetch::Value;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.archetypes.len() {
            match self.access.as_mut() {
                Some(access) => {
                    let item = Fetch::fetch(access);
                    if item.is_none() {
                        self.access = None;
                        self.index += 1;
                        continue;
                    }
                    return item;
                }
                None => {
                    if let Some(archetype) = self.archetypes.get(self.index) {
                        self.access = Some(Fetch::access(archetype).unwrap());
                    } else {
                        self.index += 1;
                    }
                    continue;
                }
            }
        }
        None
    }
}

pub struct TypedLookupIter<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> {
    access: Vec<Fetch::Access>,
    entities: Box<dyn Iterator<Item = Entity> + 'a>,
    _phantom: PhantomData<fn() -> Fetch>,
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>
    TypedLookupIter<'a, LOCKING, Fetch>
{
    pub fn new(world: &'a World, entities: impl IntoIterator<Item = Entity> + 'a) -> Self {
        Self {
            access: world
                .archetypes()
                .filter_map(|archetype| Fetch::try_access(archetype))
                .collect(),
            entities: Box::new(entities.into_iter()),
            _phantom: PhantomData,
        }
    }

    pub fn new_view(view: &'a WorldView, entities: impl IntoIterator<Item = Entity> + 'a) -> Self {
        Self {
            access: view
                .archetypes()
                .filter_map(|archetype| Fetch::try_access(archetype))
                .collect(),
            entities: Box::new(entities.into_iter()),
            _phantom: PhantomData,
        }
    }
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> Iterator
    for TypedLookupIter<'a, LOCKING, Fetch>
{
    type Item = Fetch::Value;

    fn next(&mut self) -> Option<Self::Item> {
        let entity = self.entities.next()?;
        for access in &mut self.access {
            if let Some(result) = Fetch::fetch(access, entity) {
                return Some(result);
            }
        }
        None
    }
}

pub struct TypedLookupAccess<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>> {
    access: Vec<Fetch::Access>,
    _phantom: PhantomData<fn() -> Fetch>,
}

impl<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>
    TypedLookupAccess<'a, LOCKING, Fetch>
{
    pub fn new(world: &'a World) -> Self {
        Self {
            access: world
                .archetypes()
                .filter_map(|archetype| Fetch::try_access(archetype))
                .collect(),
            _phantom: PhantomData,
        }
    }

    pub fn new_view(view: &'a WorldView) -> Self {
        Self {
            access: view
                .archetypes()
                .filter_map(|archetype| Fetch::try_access(archetype))
                .collect(),
            _phantom: PhantomData,
        }
    }

    pub fn access(&mut self, entity: Entity) -> Option<Fetch::Value> {
        for access in &mut self.access {
            if let Some(result) = Fetch::fetch(access, entity) {
                return Some(result);
            }
        }
        None
    }
}

pub struct TypedRelationLookupIter<'a, Fetch: TypedRelationLookupFetch<'a>> {
    access: Fetch::Access,
}

impl<'a, Fetch: TypedRelationLookupFetch<'a>> TypedRelationLookupIter<'a, Fetch> {
    pub fn new(world: &'a World, entity: Entity) -> Self {
        Self {
            access: Fetch::access(world, entity),
        }
    }

    pub fn access(access: Fetch::Access) -> Self {
        Self { access }
    }
}

impl<'a, Fetch: TypedRelationLookupFetch<'a>> Iterator for TypedRelationLookupIter<'a, Fetch> {
    type Item = Fetch::Value;

    fn next(&mut self) -> Option<Self::Item> {
        Fetch::fetch(&mut self.access)
    }
}

#[derive(Debug)]
enum DynamicQueryFilterMode {
    Read,
    Write,
    Include,
    Exclude,
}

#[derive(Debug, Default)]
pub struct DynamicQueryFilter {
    filter: HashMap<TypeHash, DynamicQueryFilterMode>,
}

impl DynamicQueryFilter {
    pub fn from_raw(
        read: &[TypeHash],
        write: &[TypeHash],
        include: &[TypeHash],
        exclude: &[TypeHash],
    ) -> Self {
        Self {
            filter: read
                .iter()
                .copied()
                .map(|type_hash| (type_hash, DynamicQueryFilterMode::Read))
                .chain(
                    write
                        .iter()
                        .copied()
                        .map(|type_hash| (type_hash, DynamicQueryFilterMode::Write)),
                )
                .chain(
                    include
                        .iter()
                        .copied()
                        .map(|type_hash| (type_hash, DynamicQueryFilterMode::Include)),
                )
                .chain(
                    exclude
                        .iter()
                        .copied()
                        .map(|type_hash| (type_hash, DynamicQueryFilterMode::Exclude)),
                )
                .collect(),
        }
    }

    pub fn read<T>(self) -> Self {
        self.read_raw(TypeHash::of::<T>())
    }

    pub fn read_raw(mut self, type_hash: TypeHash) -> Self {
        self.filter.insert(type_hash, DynamicQueryFilterMode::Read);
        self
    }

    pub fn write<T>(self) -> Self {
        self.write_raw(TypeHash::of::<T>())
    }

    pub fn write_raw(mut self, type_hash: TypeHash) -> Self {
        self.filter.insert(type_hash, DynamicQueryFilterMode::Write);
        self
    }

    pub fn include<T>(self) -> Self {
        self.include_raw(TypeHash::of::<T>())
    }

    pub fn include_raw(mut self, type_hash: TypeHash) -> Self {
        self.filter
            .insert(type_hash, DynamicQueryFilterMode::Include);
        self
    }

    pub fn exclude<T>(self) -> Self {
        self.exclude_raw(TypeHash::of::<T>())
    }

    pub fn exclude_raw(mut self, type_hash: TypeHash) -> Self {
        self.filter
            .insert(type_hash, DynamicQueryFilterMode::Exclude);
        self
    }

    pub fn does_accept_archetype(&self, archetype: &Archetype) -> bool {
        self.filter.iter().all(|(type_hash, mode)| match mode {
            DynamicQueryFilterMode::Read
            | DynamicQueryFilterMode::Write
            | DynamicQueryFilterMode::Include => archetype.has_type(*type_hash),
            DynamicQueryFilterMode::Exclude => !archetype.has_type(*type_hash),
        })
    }

    fn columns(&self) -> Vec<(TypeHash, bool)> {
        self.columns_iter().collect()
    }

    fn columns_iter(&self) -> impl Iterator<Item = (TypeHash, bool)> + '_ {
        self.filter
            .iter()
            .filter_map(|(type_hash, mode)| match mode {
                DynamicQueryFilterMode::Read => Some((*type_hash, false)),
                DynamicQueryFilterMode::Write => Some((*type_hash, true)),
                _ => None,
            })
    }

    pub fn unique_access(&self, output: &mut HashSet<TypeHash>) {
        for (type_hash, filter) in &self.filter {
            if matches!(filter, DynamicQueryFilterMode::Write) {
                output.insert(*type_hash);
            }
        }
    }

    pub fn query<'a, const LOCKING: bool>(
        &self,
        world: &'a World,
    ) -> DynamicQueryIter<'a, LOCKING> {
        world.dynamic_query::<LOCKING>(self)
    }

    pub fn lookup<'a, const LOCKING: bool>(
        &self,
        world: &'a World,
        entities: impl IntoIterator<Item = Entity> + 'a,
    ) -> DynamicLookupIter<'a, LOCKING> {
        world.dynamic_lookup::<LOCKING>(self, entities)
    }

    pub fn lookup_access<'a, const LOCKING: bool>(
        &self,
        world: &'a World,
    ) -> DynamicLookupAccess<'a, LOCKING> {
        world.dynamic_lookup_access::<LOCKING>(self)
    }
}

pub struct DynamicQueryItem<'a> {
    entity: Entity,
    columns: Vec<ArchetypeDynamicColumnItem<'a>>,
}

impl<'a> DynamicQueryItem<'a> {
    pub fn entity(&self) -> Entity {
        self.entity
    }

    pub fn read<T>(&self) -> Result<&ArchetypeDynamicColumnItem<'a>, QueryError> {
        self.read_raw(TypeHash::of::<T>())
    }

    pub fn read_raw(
        &self,
        type_hash: TypeHash,
    ) -> Result<&ArchetypeDynamicColumnItem<'a>, QueryError> {
        self.columns
            .iter()
            .find(|column| column.type_hash() == type_hash)
            .ok_or(QueryError::TryingToReadUnavailableType { type_hash })
    }

    pub fn write<T>(&mut self) -> Result<&mut ArchetypeDynamicColumnItem<'a>, QueryError> {
        self.write_raw(TypeHash::of::<T>())
    }

    pub fn write_raw(
        &mut self,
        type_hash: TypeHash,
    ) -> Result<&mut ArchetypeDynamicColumnItem<'a>, QueryError> {
        self.columns
            .iter_mut()
            .find(|column| column.type_hash() == type_hash)
            .ok_or(QueryError::TryingToWriteUnavailableType { type_hash })
    }
}

pub struct DynamicQueryIter<'a, const LOCKING: bool> {
    /// [(column type, unique access)]
    columns: Vec<(TypeHash, bool)>,
    archetypes: Vec<&'a Archetype>,
    index: usize,
    access: Option<(
        Box<dyn Iterator<Item = Entity> + 'a>,
        Vec<ArchetypeDynamicColumnIter<'a, LOCKING>>,
    )>,
}

impl<'a, const LOCKING: bool> DynamicQueryIter<'a, LOCKING> {
    pub fn new(filter: &DynamicQueryFilter, world: &'a World) -> Self {
        Self {
            columns: filter.columns(),
            archetypes: world
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .collect(),
            index: 0,
            access: None,
        }
    }

    pub fn new_view(filter: &DynamicQueryFilter, view: &'a WorldView) -> Self {
        Self {
            columns: filter.columns(),
            archetypes: view
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .collect(),
            index: 0,
            access: None,
        }
    }
}

impl<'a, const LOCKING: bool> Iterator for DynamicQueryIter<'a, LOCKING> {
    type Item = DynamicQueryItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.archetypes.len() {
            match self.access.as_mut() {
                Some((entities, columns)) => {
                    let entity = entities.next();
                    match columns
                        .iter_mut()
                        .map(|access| access.next())
                        .collect::<Option<_>>()
                        .and_then(|columns| Some((entity?, columns)))
                    {
                        Some((entity, columns)) => {
                            return Some(DynamicQueryItem { entity, columns });
                        }
                        None => {
                            self.access = None;
                            self.index += 1;
                            continue;
                        }
                    }
                }
                None => {
                    if let Some(archetype) = self.archetypes.get(self.index) {
                        self.access = Some((
                            Box::new(archetype.entities().iter()),
                            self.columns
                                .iter()
                                .copied()
                                .map(|(type_hash, unique)| {
                                    archetype.dynamic_column_iter(type_hash, unique).unwrap()
                                })
                                .collect(),
                        ));
                    } else {
                        self.index += 1;
                    }
                    continue;
                }
            }
        }
        None
    }
}

pub struct DynamicLookupIter<'a, const LOCKING: bool> {
    /// [(column type, unique access)]
    columns: Vec<(TypeHash, bool)>,
    access: Vec<(
        &'a EntityDenseMap,
        ArchetypeDynamicColumnAccess<'a, LOCKING>,
    )>,
    entities: Box<dyn Iterator<Item = Entity> + 'a>,
}

impl<'a, const LOCKING: bool> DynamicLookupIter<'a, LOCKING> {
    pub fn new(
        filter: &DynamicQueryFilter,
        world: &'a World,
        entities: impl IntoIterator<Item = Entity> + 'a,
    ) -> Self {
        Self {
            columns: filter.columns(),
            access: world
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .flat_map(|archetype| {
                    filter.columns_iter().filter_map(|(type_hash, unique)| {
                        Some((
                            archetype.entities(),
                            archetype.dynamic_column(type_hash, unique).ok()?,
                        ))
                    })
                })
                .collect(),
            entities: Box::new(entities.into_iter()),
        }
    }

    pub fn new_view(
        filter: &DynamicQueryFilter,
        view: &'a WorldView,
        entities: impl IntoIterator<Item = Entity> + 'a,
    ) -> Self {
        Self {
            columns: filter.columns(),
            access: view
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .flat_map(|archetype| {
                    filter.columns_iter().filter_map(|(type_hash, unique)| {
                        Some((
                            archetype.entities(),
                            archetype.dynamic_column(type_hash, unique).ok()?,
                        ))
                    })
                })
                .collect(),
            entities: Box::new(entities.into_iter()),
        }
    }
}

impl<'a, const LOCKING: bool> Iterator for DynamicLookupIter<'a, LOCKING> {
    type Item = DynamicQueryItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let entity = self.entities.next()?;
        let columns = self
            .columns
            .iter()
            .map(|(type_hash, unique)| {
                self.access
                    .iter()
                    .find(|(map, access)| {
                        map.contains(entity)
                            && access.info().type_hash() == *type_hash
                            && access.is_unique() == *unique
                    })
                    .and_then(|(map, access)| unsafe {
                        std::mem::transmute(access.dynamic_item(map.index_of(entity).unwrap()).ok())
                    })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(DynamicQueryItem { entity, columns })
    }
}

pub struct DynamicLookupAccess<'a, const LOCKING: bool> {
    /// [(column type, unique access)]
    columns: Vec<(TypeHash, bool)>,
    access: Vec<(
        &'a EntityDenseMap,
        ArchetypeDynamicColumnAccess<'a, LOCKING>,
    )>,
}

impl<'a, const LOCKING: bool> DynamicLookupAccess<'a, LOCKING> {
    pub fn new(filter: &DynamicQueryFilter, world: &'a World) -> Self {
        Self {
            columns: filter.columns(),
            access: world
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .flat_map(|archetype| {
                    filter.columns_iter().filter_map(|(type_hash, unique)| {
                        Some((
                            archetype.entities(),
                            archetype.dynamic_column(type_hash, unique).ok()?,
                        ))
                    })
                })
                .collect(),
        }
    }

    pub fn new_view(filter: &DynamicQueryFilter, view: &'a WorldView) -> Self {
        Self {
            columns: filter.columns(),
            access: view
                .archetypes()
                .filter(|archetype| filter.does_accept_archetype(archetype))
                .flat_map(|archetype| {
                    filter.columns_iter().filter_map(|(type_hash, unique)| {
                        Some((
                            archetype.entities(),
                            archetype.dynamic_column(type_hash, unique).ok()?,
                        ))
                    })
                })
                .collect(),
        }
    }

    pub fn access(&'_ self, entity: Entity) -> Option<DynamicQueryItem<'_>> {
        let columns = self
            .columns
            .iter()
            .map(|(type_hash, unique)| {
                self.access
                    .iter()
                    .find(|(map, access)| {
                        map.contains(entity)
                            && access.info().type_hash() == *type_hash
                            && access.is_unique() == *unique
                    })
                    .and_then(|(map, access)| unsafe {
                        std::mem::transmute(access.dynamic_item(map.index_of(entity).unwrap()).ok())
                    })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(DynamicQueryItem { entity, columns })
    }
}
