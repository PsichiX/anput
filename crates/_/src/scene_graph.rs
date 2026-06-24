use crate::{
    archetype::ArchetypeColumnInfo,
    bundle::{Bundle, BundleColumns},
    component::{Component, ComponentRef, ComponentRefMut},
    entity::Entity,
    query::{Lookup, Traverse, TypedLookupFetch, TypedRelationLookupFetch},
    world::{Relation, World, WorldError},
};
use intuicio_core::{context::Context, function::FunctionHandle, registry::Registry};
use intuicio_data::{
    data_stack::DataStackPack,
    lifetime::Lifetime,
    managed::{DynamicManaged, DynamicManagedRef},
};
use std::collections::HashMap;

pub use intuicio_core::function::Function as SceneMessageFunction;

pub struct SceneChild;
pub struct SceneParent;

#[derive(Debug, Default, Clone)]
pub struct SceneMessageListeners(HashMap<String, FunctionHandle>);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SceneNode(Entity);

impl SceneNode {
    /// # Safety
    pub unsafe fn new(entity: Entity) -> Self {
        Self(entity)
    }

    pub fn spawn(
        world: &mut World,
        bundle: impl Bundle + Send + Sync + 'static,
    ) -> Result<Self, WorldError> {
        let entity = world.spawn((
            SceneMessageListeners::default(),
            Relation::<SceneChild>::default(),
            Relation::<SceneParent>::default(),
        ))?;
        world.insert(entity, bundle)?;
        Ok(Self(entity))
    }

    pub fn spawn_child<const LOCKING: bool>(
        self,
        world: &mut World,
        bundle: impl Bundle + Send + Sync + 'static,
    ) -> Result<Self, WorldError> {
        let child = Self::spawn(world, bundle)?;
        self.add_child::<LOCKING>(world, child)?;
        Ok(child)
    }

    pub fn despawn<const LOCKING: bool>(self, world: &mut World) -> Result<(), WorldError> {
        for parent in self.parents::<LOCKING>(world).collect::<Vec<_>>() {
            world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(parent.0, self.0)?;
        }
        for child in self.children::<LOCKING>(world).collect::<Vec<_>>() {
            world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(self.0, child.0)?;
        }
        world.despawn(self.0)
    }

    pub fn despawn_hierarchy_with_self<const LOCKING: bool>(
        self,
        world: &mut World,
    ) -> Result<(), WorldError> {
        for parent in self.parents::<LOCKING>(world).collect::<Vec<_>>() {
            world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(parent.0, self.0)?;
        }
        for entity in world
            .traverse_outgoing::<LOCKING, SceneChild>([self.0])
            .map(|(_, entity)| entity)
            .collect::<Vec<_>>()
        {
            world.despawn(entity)?;
        }
        Ok(())
    }

    pub fn despawn_hierarchy<const LOCKING: bool>(
        self,
        world: &mut World,
    ) -> Result<(), WorldError> {
        for child in self.children::<LOCKING>(world).collect::<Vec<_>>() {
            world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(self.0, child.0)?;
        }
        for entity in world
            .traverse_outgoing::<LOCKING, SceneChild>([self.0])
            .skip(1)
            .map(|(_, entity)| entity)
            .collect::<Vec<_>>()
        {
            world.despawn(entity)?;
        }
        Ok(())
    }

    pub fn insert(
        self,
        world: &mut World,
        bundle: impl Bundle + Send + Sync + 'static,
    ) -> Result<(), WorldError> {
        world.insert(self.0, bundle)
    }

    pub fn remove<T: BundleColumns>(self, world: &mut World) -> Result<(), WorldError> {
        world.remove::<T>(self.0)
    }

    pub fn remove_raw(
        self,
        world: &mut World,
        columns: Vec<ArchetypeColumnInfo>,
    ) -> Result<(), WorldError> {
        world.remove_raw(self.0, columns)
    }

    pub fn exists(self, world: &World) -> bool {
        world.has_entity(self.0)
    }

    pub fn entity(self) -> Entity {
        self.0
    }

    pub fn fetch<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
        self,
        world: &'a World,
    ) -> Option<Fetch::Value> {
        world.entity::<LOCKING, Fetch>(self.0)
    }

    pub fn component<const LOCKING: bool, T: Component>(
        self,
        world: &'_ World,
    ) -> Result<ComponentRef<'_, LOCKING, T>, WorldError> {
        world.component::<LOCKING, T>(self.0)
    }

    pub fn component_mut<const LOCKING: bool, T: Component>(
        self,
        world: &'_ World,
    ) -> Result<ComponentRefMut<'_, LOCKING, T>, WorldError> {
        world.component_mut::<LOCKING, T>(self.0)
    }

    pub fn access<'a, const LOCKING: bool, Fetch: TypedLookupFetch<'a, LOCKING>>(
        self,
        world: &'a World,
    ) -> Option<Fetch::ValueOne> {
        world.lookup_one::<LOCKING, Fetch>(self.0)
    }

    pub fn add_child<const LOCKING: bool>(
        self,
        world: &mut World,
        other: Self,
    ) -> Result<(), WorldError> {
        world.relate_pair::<LOCKING, _, _>(SceneParent, SceneChild, self.0, other.0)?;
        Ok(())
    }

    pub fn remove_child<const LOCKING: bool>(
        self,
        world: &mut World,
        other: Self,
    ) -> Result<(), WorldError> {
        world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(self.0, other.0)?;
        Ok(())
    }

    pub fn reparent<const LOCKING: bool>(
        world: &mut World,
        node: Self,
        new_parent: Self,
    ) -> Result<(), WorldError> {
        for parent in node.parents::<LOCKING>(world).collect::<Vec<_>>() {
            world.unrelate_pair::<LOCKING, SceneParent, SceneChild>(parent.0, node.0)?;
        }
        world.relate_pair::<LOCKING, _, _>(SceneParent, SceneChild, new_parent.0, node.0)?;
        Ok(())
    }

    pub fn children<const LOCKING: bool>(self, world: &World) -> impl Iterator<Item = Self> + '_ {
        world
            .relations_outgoing::<LOCKING, SceneChild>(self.0)
            .map(|(_, _, entity)| Self(entity))
    }

    pub fn parents<const LOCKING: bool>(self, world: &World) -> impl Iterator<Item = Self> + '_ {
        world
            .relations_outgoing::<LOCKING, SceneParent>(self.0)
            .map(|(_, _, entity)| Self(entity))
    }

    pub fn has_child<const LOCKING: bool>(self, world: &World, other: Self) -> bool {
        world.has_relation::<LOCKING, SceneChild>(self.0, other.0)
    }

    pub fn has_parent<const LOCKING: bool>(self, world: &World, other: Self) -> bool {
        world.has_relation::<LOCKING, SceneParent>(self.0, other.0)
    }

    pub fn traverse<'a, const LOCKING: bool, Relation, Fetch>(
        self,
        world: &'a World,
    ) -> impl Iterator<Item = Fetch::ValueOne>
    where
        Relation: Component,
        Fetch: TypedLookupFetch<'a, LOCKING> + 'a,
    {
        world
            .relation_lookup::<LOCKING, Traverse<LOCKING, Relation, Lookup<LOCKING, Fetch>>>(self.0)
    }

    pub fn relation_lookup<'a, const LOCKING: bool, Fetch: TypedRelationLookupFetch<'a>>(
        &self,
        world: &'a World,
    ) -> impl Iterator<Item = Fetch::Value> {
        world.relation_lookup::<LOCKING, Fetch>(self.0)
    }

    pub fn register_message_listener<const LOCKING: bool>(
        self,
        world: &World,
        id: impl ToString,
        function: SceneMessageFunction,
    ) -> Result<(), WorldError> {
        let mut listeners = self.component_mut::<LOCKING, SceneMessageListeners>(world)?;
        listeners.0.insert(id.to_string(), function.into_handle());
        Ok(())
    }

    pub fn unregister_message_listener<const LOCKING: bool>(
        self,
        world: &World,
        id: &str,
    ) -> Result<(), WorldError> {
        let mut listeners = self.component_mut::<LOCKING, SceneMessageListeners>(world)?;
        listeners.0.remove(id);
        Ok(())
    }

    pub fn message_listener<const LOCKING: bool>(
        self,
        world: &World,
        id: &str,
    ) -> Result<Option<FunctionHandle>, WorldError> {
        let listeners = self.component::<LOCKING, SceneMessageListeners>(world)?;
        Ok(listeners.0.get(id).cloned())
    }

    pub fn invoke_message<const LOCKING: bool>(
        self,
        world: &World,
        id: &str,
        context: &mut Context,
        registry: &Registry,
    ) -> Result<(), WorldError> {
        let listeners = self.component::<LOCKING, SceneMessageListeners>(world)?;
        if let Some(function) = listeners.0.get(id).cloned() {
            context.stack().push(DynamicManaged::new(self).unwrap());
            let lifetime = Lifetime::default();
            let value = DynamicManagedRef::new(world, lifetime.borrow().unwrap());
            context.stack().push(value);
            function.invoke(context, registry);
        }
        Ok(())
    }

    pub fn dispatch_message<const LOCKING: bool, O: DataStackPack, I: DataStackPack>(
        self,
        world: &World,
        id: &str,
        context: &mut Context,
        registry: &Registry,
        inputs: I,
    ) -> Result<Option<O>, WorldError> {
        let listeners = self.component::<LOCKING, SceneMessageListeners>(world)?;
        if let Some(function) = listeners.0.get(id).cloned() {
            inputs.stack_push_reversed(context.stack());
            context.stack().push(DynamicManaged::new(self).unwrap());
            let lifetime = Lifetime::default();
            let value = DynamicManagedRef::new(world, lifetime.borrow().unwrap());
            context.stack().push(value);
            function.invoke(context, registry);
            Ok(Some(O::stack_pop(context.stack())))
        } else {
            Ok(None)
        }
    }

    pub fn dispatch_message_hierarchy<const LOCKING: bool, I: DataStackPack + Clone>(
        self,
        world: &World,
        id: &str,
        context: &mut Context,
        registry: &Registry,
        inputs: I,
    ) -> Result<(), WorldError> {
        self.dispatch_message::<LOCKING, (), I>(world, id, context, registry, inputs.clone())?;
        for child in self.children::<LOCKING>(world) {
            child.dispatch_message_hierarchy::<LOCKING, I>(
                world,
                id,
                context,
                registry,
                inputs.clone(),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intuicio_core::transformer::{DynamicManagedValueTransformer, ValueTransformer};
    use intuicio_derive::intuicio_function;

    fn is_async<T: Send + Sync>() {}

    struct Attack(usize);

    struct Lives(usize);

    #[derive(Debug, Default, Clone)]
    struct Counter {
        odd: usize,
        even: usize,
    }

    #[intuicio_function(transformer = "DynamicManagedValueTransformer")]
    fn attack(world: &World, this: SceneNode, other: SceneNode) {
        let this_attack = this.component::<true, Attack>(world).unwrap();
        let mut other_lives = other.component_mut::<true, Lives>(world).unwrap();
        other_lives.0 = other_lives.0.saturating_sub(this_attack.0);
    }

    #[test]
    fn test_scene_node() {
        is_async::<SceneNode>();

        let registry = Registry::default().with_basic_types().with_erased_types();
        let mut context = Context::new(10240, 10240);
        let mut world = World::default();

        let player =
            SceneNode::spawn(&mut world, ("player".to_owned(), Attack(2), Lives(1))).unwrap();
        player
            .register_message_listener::<true>(&world, "attack", attack::define_function(&registry))
            .unwrap();
        assert!(player.exists(&world));
        assert_eq!(player.component::<true, Attack>(&world).unwrap().0, 2);
        assert_eq!(player.component::<true, Lives>(&world).unwrap().0, 1);

        let enemy =
            SceneNode::spawn(&mut world, ("enemy".to_owned(), Attack(1), Lives(2))).unwrap();
        assert!(enemy.exists(&world));
        assert_eq!(enemy.component::<true, Attack>(&world).unwrap().0, 1);
        assert_eq!(enemy.component::<true, Lives>(&world).unwrap().0, 2);

        player
            .dispatch_message::<true, (), _>(
                &world,
                "attack",
                &mut context,
                &registry,
                (DynamicManaged::new(enemy).unwrap(),),
            )
            .unwrap();
        assert_eq!(enemy.component::<true, Lives>(&world).unwrap().0, 0);
    }

    #[test]
    fn test_scene_node_singleton() {
        let mut world = World::default();
        let resources = SceneNode::spawn(&mut world, (Counter::default(),)).unwrap();

        for index in 0..5usize {
            world.spawn((index,)).unwrap();
        }

        let mut counter = resources.component_mut::<true, Counter>(&world).unwrap();
        for value in world.query::<true, &usize>() {
            if *value % 2 == 0 {
                counter.even += 1;
            } else {
                counter.odd += 1;
            }
        }
        assert_eq!(counter.odd, 2);
        assert_eq!(counter.even, 3);
    }

    #[test]
    fn test_scene_node_hierarchy() {
        let mut world = World::default();
        let root = SceneNode::spawn(&mut world, ("root",)).unwrap();
        let a = root.spawn_child::<true>(&mut world, ("a",)).unwrap();
        let b = root.spawn_child::<true>(&mut world, ("b",)).unwrap();
        let c = a.spawn_child::<true>(&mut world, ("c",)).unwrap();
        let d = a.spawn_child::<true>(&mut world, ("d",)).unwrap();

        assert!(root.exists(&world));
        assert!(a.exists(&world));
        assert!(b.exists(&world));
        assert!(c.exists(&world));
        assert!(d.exists(&world));

        // d.despawn::<true>(&mut world).unwrap();
        // assert!(!d.exists(&world));
        // assert!(!a.has_child::<true>(&world, d));

        root.despawn_hierarchy_with_self::<true>(&mut world)
            .unwrap();
        assert!(!root.exists(&world));
        assert!(!a.exists(&world));
        assert!(!b.exists(&world));
        assert!(!c.exists(&world));
        assert!(!d.exists(&world));
    }
}
