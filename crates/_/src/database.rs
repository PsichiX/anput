use crate::{
    bundle::Bundle,
    commands::{DespawnManyCommand, SpawnManyCommand},
    entity::Entity,
    query::{TypedLookupAccess, TypedLookupFetch},
};

pub trait WorldCreateIteratorExt: Iterator
where
    Self::Item: Bundle + Send + Sync + 'static,
{
    fn to_spawn_command(self) -> SpawnManyCommand<Self::Item>;
}

impl<I> WorldCreateIteratorExt for I
where
    I: Iterator,
    I::Item: Bundle + Send + Sync + 'static,
{
    fn to_spawn_command(self) -> SpawnManyCommand<Self::Item> {
        SpawnManyCommand::new(self)
    }
}

pub trait WorldDestroyIteratorExt: Iterator {
    fn to_despawn_command(self) -> DespawnManyCommand;
}

impl<I> WorldDestroyIteratorExt for I
where
    I: Iterator<Item = Entity>,
{
    fn to_despawn_command(self) -> DespawnManyCommand {
        DespawnManyCommand::new(self)
    }
}

pub struct WorldJoinIterator<'a, const LOCKING: bool, LeftIter, RightFetch, F, EntityIIter>
where
    LeftIter: Iterator,
    RightFetch: TypedLookupFetch<'a, LOCKING>,
    F: Fn(LeftIter::Item) -> EntityIIter,
    EntityIIter: Iterator<Item = Entity>,
{
    left_iter: LeftIter,
    right_lookup: TypedLookupAccess<'a, LOCKING, RightFetch>,
    entity_producer: F,
    current: Option<(LeftIter::Item, EntityIIter)>,
}

impl<'a, const LOCKING: bool, LeftIter, RightFetch, F, EntityIter>
    WorldJoinIterator<'a, LOCKING, LeftIter, RightFetch, F, EntityIter>
where
    LeftIter: Iterator,
    RightFetch: TypedLookupFetch<'a, LOCKING>,
    F: Fn(LeftIter::Item) -> EntityIter,
    EntityIter: Iterator<Item = Entity>,
{
    pub fn new(
        left_iter: LeftIter,
        right_lookup: TypedLookupAccess<'a, LOCKING, RightFetch>,
        entity_producer: F,
    ) -> Self {
        Self {
            left_iter,
            right_lookup,
            entity_producer,
            current: None,
        }
    }
}

impl<'a, const LOCKING: bool, LeftIter, RightFetch, F, EI> Iterator
    for WorldJoinIterator<'a, LOCKING, LeftIter, RightFetch, F, EI>
where
    LeftIter: Iterator,
    RightFetch: TypedLookupFetch<'a, LOCKING>,
    F: Fn(LeftIter::Item) -> EI,
    EI: Iterator<Item = Entity>,
    LeftIter::Item: Copy,
{
    type Item = (LeftIter::Item, RightFetch::Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((left, entities)) = self.current.as_mut() {
                if let Some(entity) = entities.next() {
                    let right = self.right_lookup.access(entity)?;
                    return Some((*left, right));
                } else {
                    self.current = None;
                }
            }
            let left = self.left_iter.next()?;
            let entities = (self.entity_producer)(left);
            self.current = Some((left, entities));
        }
    }
}

pub trait WorldJoinIteratorExt: Iterator {
    fn join<'a, const LOCKING: bool, RightFetch, F, EntityIter>(
        self,
        right_lookup: TypedLookupAccess<'a, LOCKING, RightFetch>,
        entity_producer: F,
    ) -> WorldJoinIterator<'a, LOCKING, Self, RightFetch, F, EntityIter>
    where
        RightFetch: TypedLookupFetch<'a, LOCKING>,
        F: Fn(Self::Item) -> EntityIter,
        EntityIter: Iterator<Item = Entity>,
        Self: Sized;
}

impl<I> WorldJoinIteratorExt for I
where
    I: Iterator,
{
    fn join<'a, const LOCKING: bool, RightFetch, F, EntityIter>(
        self,
        right_lookup: TypedLookupAccess<'a, LOCKING, RightFetch>,
        entity_producer: F,
    ) -> WorldJoinIterator<'a, LOCKING, Self, RightFetch, F, EntityIter>
    where
        RightFetch: TypedLookupFetch<'a, LOCKING>,
        F: Fn(Self::Item) -> EntityIter,
        EntityIter: Iterator<Item = Entity>,
        Self: Sized,
    {
        WorldJoinIterator::new(self, right_lookup, entity_producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        commands::Command,
        entity::Entity,
        world::{Relation, World},
    };

    #[test]
    fn test_crud() {
        let mut world = World::default();

        // Create:
        [
            ("a", 1usize, 1.0f32),
            ("b", 2usize, 2.0f32),
            ("c", 3usize, 3.0f32),
        ]
        .into_iter()
        .to_spawn_command()
        .execute(&mut world)
        .unwrap();

        // Read:
        let rows = world
            .query::<true, (&&str, &usize, &f32)>()
            .collect::<Vec<_>>();

        assert_eq!(
            rows,
            vec![
                (&"a", &1usize, &1.0f32),
                (&"b", &2usize, &2.0f32),
                (&"c", &3usize, &3.0f32),
            ]
        );

        // Update:
        for value in world.query::<true, &mut usize>() {
            if *value < 2 {
                *value = 0;
            }
        }

        // Delete:
        world
            .query::<true, (Entity, &usize)>()
            .filter(|(_, a)| **a > 0)
            .map(|(entity, _)| entity)
            .to_despawn_command()
            .execute(&mut world)
            .unwrap();

        let rows = world
            .query::<true, (&&str, &usize, &f32)>()
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![(&"a", &0usize, &1.0f32),]);
    }

    #[test]
    fn test_join() {
        let mut world = World::default();

        let a = world.spawn(("a", 1usize)).unwrap();
        let b = world.spawn(("b", 2usize)).unwrap();
        world
            .spawn(("c", 3usize, Relation::<()>::new((), a).with((), b)))
            .unwrap();
        world
            .spawn(("d", 4usize, Relation::<()>::default()))
            .unwrap();

        // Join:
        let rows = world
            .query::<true, (&&str, &Relation<()>)>()
            .join(world.lookup_access::<true, &usize>(), |(_, relation)| {
                relation.entities()
            })
            .map(|((name, _), value)| (name, value))
            .collect::<Vec<_>>();

        assert_eq!(rows, vec![(&"c", &1usize), (&"c", &2usize)]);
    }
}
