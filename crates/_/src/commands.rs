use crate::{
    bundle::{Bundle, BundleColumns},
    component::Component,
    entity::Entity,
    world::World,
};
use std::{
    error::Error,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

pub trait Command: Send + Sync + 'static {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>>;
}

#[derive(Default)]
pub struct CommandBuffer {
    #[allow(clippy::type_complexity)]
    commands: Vec<Box<dyn FnOnce(&mut World) -> Result<(), Box<dyn Error>> + Send + Sync>>,
}

impl CommandBuffer {
    pub fn schedule(
        &mut self,
        command: impl FnOnce(&mut World) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
    ) {
        self.commands.push(Box::new(command));
    }

    pub fn command(&mut self, command: impl Command) {
        self.schedule(|world| command.execute(world));
    }

    pub fn commands(&mut self, mut buffer: CommandBuffer) {
        self.schedule(move |world| buffer.execute(world));
    }

    pub fn execute(&mut self, world: &mut World) -> Result<(), Box<dyn Error>> {
        for command in std::mem::take(&mut self.commands) {
            (command)(world)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.commands.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }
}

#[derive(Default, Clone)]
pub struct SharedCommandBuffer {
    inner: Arc<Mutex<CommandBuffer>>,
}

impl SharedCommandBuffer {
    pub fn with<R>(&mut self, f: impl FnOnce(&mut CommandBuffer) -> R) -> Option<R> {
        if let Ok(mut buffer) = self.inner.lock() {
            Some(f(&mut buffer))
        } else {
            None
        }
    }

    pub fn try_with<R>(&mut self, f: impl FnOnce(&mut CommandBuffer) -> R) -> Option<R> {
        if let Ok(mut buffer) = self.inner.try_lock() {
            Some(f(&mut buffer))
        } else {
            None
        }
    }
}

pub struct SpawnCommand<T: Bundle + Send + Sync + 'static> {
    bundle: T,
}

impl<T: Bundle + Send + Sync + 'static> SpawnCommand<T> {
    pub fn new(bundle: T) -> Self {
        Self { bundle }
    }
}

impl<T: Bundle + Send + Sync + 'static> Command for SpawnCommand<T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.spawn(self.bundle)?;
        Ok(())
    }
}

pub struct SpawnManyCommand<T: Bundle + Send + Sync + 'static> {
    bundles: Vec<T>,
}

impl<T: Bundle + Send + Sync + 'static> SpawnManyCommand<T> {
    pub fn new(bundles: impl IntoIterator<Item = T>) -> Self {
        Self {
            bundles: bundles.into_iter().collect(),
        }
    }
}

impl<T: Bundle + Send + Sync + 'static> Command for SpawnManyCommand<T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        for bundle in self.bundles {
            world.spawn(bundle)?;
        }
        Ok(())
    }
}

pub struct DespawnCommand {
    entity: Entity,
}

impl DespawnCommand {
    pub fn new(entity: Entity) -> Self {
        Self { entity }
    }
}

impl Command for DespawnCommand {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.despawn(self.entity)?;
        Ok(())
    }
}

pub struct DespawnManyCommand {
    entities: Vec<Entity>,
}

impl DespawnManyCommand {
    pub fn new(entities: impl IntoIterator<Item = Entity>) -> Self {
        Self {
            entities: entities.into_iter().collect(),
        }
    }
}

impl Command for DespawnManyCommand {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        for entity in self.entities {
            world.despawn(entity)?;
        }
        Ok(())
    }
}

pub struct InsertCommand<T: Bundle + Send + Sync + 'static> {
    entity: Entity,
    bundle: T,
}

impl<T: Bundle + Send + Sync + 'static> InsertCommand<T> {
    pub fn new(entity: Entity, bundle: T) -> Self {
        Self { entity, bundle }
    }
}

impl<T: Bundle + Send + Sync + 'static> Command for InsertCommand<T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.insert(self.entity, self.bundle)?;
        Ok(())
    }
}

pub struct RemoveCommand<T: BundleColumns> {
    entity: Entity,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: Bundle + Send + Sync + 'static> RemoveCommand<T> {
    pub fn new(entity: Entity) -> Self {
        Self {
            entity,
            _phantom: PhantomData,
        }
    }
}

impl<T: Bundle + Send + Sync + 'static> Command for RemoveCommand<T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.remove::<T>(self.entity)?;
        Ok(())
    }
}

pub struct RelateCommand<const LOCKING: bool, T: Component> {
    payload: T,
    from: Entity,
    to: Entity,
}

impl<const LOCKING: bool, T: Component> RelateCommand<LOCKING, T> {
    pub fn new(payload: T, from: Entity, to: Entity) -> Self {
        Self { payload, from, to }
    }
}

impl<const LOCKING: bool, T: Component> Command for RelateCommand<LOCKING, T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.relate::<LOCKING, T>(self.payload, self.from, self.to)?;
        Ok(())
    }
}

pub struct RelateOneCommand<const LOCKING: bool, T: Component> {
    payload: T,
    from: Entity,
    to: Entity,
}

impl<const LOCKING: bool, T: Component> RelateOneCommand<LOCKING, T> {
    pub fn new(payload: T, from: Entity, to: Entity) -> Self {
        Self { payload, from, to }
    }
}

impl<const LOCKING: bool, T: Component> Command for RelateOneCommand<LOCKING, T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.relate_one::<LOCKING, T>(self.payload, self.from, self.to)?;
        Ok(())
    }
}

pub struct RelatePairCommand<const LOCKING: bool, I: Component, O: Component> {
    payload_incoming: I,
    payload_outgoing: O,
    from: Entity,
    to: Entity,
}

impl<const LOCKING: bool, I: Component, O: Component> RelatePairCommand<LOCKING, I, O> {
    pub fn new(payload_incoming: I, payload_outgoing: O, from: Entity, to: Entity) -> Self {
        Self {
            payload_incoming,
            payload_outgoing,
            from,
            to,
        }
    }
}

impl<const LOCKING: bool, I: Component, O: Component> Command for RelatePairCommand<LOCKING, I, O> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.relate_pair::<LOCKING, I, O>(
            self.payload_incoming,
            self.payload_outgoing,
            self.from,
            self.to,
        )?;
        Ok(())
    }
}

pub struct UnrelateCommand<const LOCKING: bool, T: Component> {
    from: Entity,
    to: Entity,
    _phantom: PhantomData<fn() -> T>,
}

impl<const LOCKING: bool, T: Component> UnrelateCommand<LOCKING, T> {
    pub fn new(from: Entity, to: Entity) -> Self {
        Self {
            from,
            to,
            _phantom: PhantomData,
        }
    }
}

impl<const LOCKING: bool, T: Component> Command for UnrelateCommand<LOCKING, T> {
    fn execute(self, world: &mut World) -> Result<(), Box<dyn Error>> {
        world.unrelate::<LOCKING, T>(self.from, self.to)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::World;

    #[test]
    fn test_async() {
        fn is_async<T: Send + Sync>() {}

        is_async::<CommandBuffer>();
        is_async::<SharedCommandBuffer>();
    }

    #[test]
    fn test_command_buffer() {
        let mut world = World::default();
        let mut buffer = CommandBuffer::default();
        assert!(world.is_empty());

        buffer.command(SpawnCommand::new((1u8, 2u16, 3u32)));
        buffer.execute(&mut world).unwrap();
        assert_eq!(world.len(), 1);

        let entity = world.entities().next().unwrap();
        buffer.command(DespawnCommand::new(entity));
        buffer.execute(&mut world).unwrap();
        assert!(world.is_empty());
    }
}
