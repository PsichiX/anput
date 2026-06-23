use crate::{
    PhysicsAccessView, PhysicsSimulation, Scalar,
    components::{
        AngularVelocity, BodyAccessInfo, BodyMaterial, BodyParentRelation, BodyParticleRelation,
        LinearVelocity, Mass, PhysicsBody, PhysicsParticle, Position, Rotation,
    },
    density_fields::{DensityField, DensityFieldBox},
    queries::shape::{ShapeOverlapCell, ShapeOverlapQuery},
    utils::quat_from_axis_angle,
};
use anput::{
    entity::Entity,
    event::EventDispatcher,
    query::{Include, Lookup},
    systems::{System, SystemContext},
    universe::{Local, Res},
    world::{Relation, World},
};
use anput_spatial::{
    SpatialExtractor, SpatialPartitioning,
    third_party::rstar::{
        AABB, Envelope, Point, PointDistance, RTreeObject, primitives::Rectangle,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Range},
};
use vek::{Aabb, Vec3};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct CollisionMask(pub u128);

impl CollisionMask {
    pub fn flag(index: u128) -> Self {
        Self(1 << index)
    }

    pub fn with(mut self, index: u128) -> Self {
        self.enable(index);
        self
    }

    pub fn without(mut self, index: u128) -> Self {
        self.disable(index);
        self
    }

    pub fn enable(&mut self, index: u128) {
        self.0 |= 1 << index;
    }

    pub fn disable(&mut self, index: u128) {
        self.0 &= !(1 << index);
    }

    pub fn toggle(&mut self, index: u128) {
        self.0 ^= 1 << index;
    }

    pub fn is_enabled(&self, index: u128) -> bool {
        (self.0 & (1 << index)) != 0
    }

    pub fn does_match(&self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    pub fn is_superset_of(&self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub fn is_subset_of(&self, other: Self) -> bool {
        (self.0 & other.0) == self.0
    }
}

impl BitAnd for CollisionMask {
    type Output = Self;

    fn bitand(self, other: Self) -> Self::Output {
        Self(self.0.bitand(other.0))
    }
}

impl BitAndAssign for CollisionMask {
    fn bitand_assign(&mut self, other: Self) {
        self.0.bitand_assign(other.0);
    }
}

impl BitOr for CollisionMask {
    type Output = Self;

    fn bitor(self, other: Self) -> Self::Output {
        Self(self.0.bitor(other.0))
    }
}

impl BitOrAssign for CollisionMask {
    fn bitor_assign(&mut self, other: Self) {
        self.0.bitor_assign(other.0);
    }
}

impl BitXor for CollisionMask {
    type Output = Self;

    fn bitxor(self, other: Self) -> Self::Output {
        Self(self.0.bitxor(other.0))
    }
}

impl BitXorAssign for CollisionMask {
    fn bitxor_assign(&mut self, other: Self) {
        self.0.bitxor_assign(other.0);
    }
}

impl From<u128> for CollisionMask {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollisionProfile {
    pub block: CollisionMask,
    pub overlap: CollisionMask,
    pub trace: CollisionMask,
}

impl CollisionProfile {
    pub fn new(block: CollisionMask, overlap: CollisionMask, trace: CollisionMask) -> Self {
        Self {
            block,
            overlap,
            trace,
        }
    }

    pub fn with_block(mut self, mask: CollisionMask) -> Self {
        self.block |= mask;
        self
    }

    pub fn with_overlap(mut self, mask: CollisionMask) -> Self {
        self.overlap |= mask;
        self
    }

    pub fn with_trace(mut self, mask: CollisionMask) -> Self {
        self.trace |= mask;
        self
    }

    pub fn does_block(&self, other: &Self) -> bool {
        self.block.does_match(other.block)
    }

    pub fn does_overlap(&self, other: &Self) -> bool {
        self.overlap.does_match(other.overlap)
    }

    pub fn does_overlap_permissive(&self, other: &Self) -> bool {
        self.overlap.does_match(other.overlap)
            || self.overlap.does_match(other.block)
            || self.block.does_match(other.overlap)
    }

    pub fn does_trace(&self, other: &Self) -> bool {
        self.trace.does_match(other.trace)
    }

    pub fn does_trace_permissive(&self, other: &Self) -> bool {
        self.trace.does_match(other.trace)
            || self.trace.does_match(other.block)
            || self.block.does_match(other.trace)
    }
}

pub struct CollisionProfilesRegistry<Key: Eq + Hash> {
    registry: HashMap<Key, CollisionProfile>,
}

impl<Key: Eq + Hash> Default for CollisionProfilesRegistry<Key> {
    fn default() -> Self {
        Self {
            registry: Default::default(),
        }
    }
}

impl<Key: Eq + Hash> CollisionProfilesRegistry<Key> {
    pub fn with(mut self, key: Key, profile: CollisionProfile) -> Self {
        self.register(key, profile);
        self
    }

    pub fn register(&mut self, key: Key, profile: CollisionProfile) {
        self.registry.insert(key, profile);
    }

    pub fn unregister(&mut self, key: &Key) -> Option<CollisionProfile> {
        self.registry.remove(key)
    }

    pub fn get(&self, key: &Key) -> Option<&CollisionProfile> {
        self.registry.get(key)
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.registry.contains_key(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContactEventKind {
    Began,
    Continue,
    Ended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContactEvent {
    pub kind: ContactEventKind,
    pub blocking: bool,
    pub self_body: Entity,
    pub other_body: Entity,
    pub self_density_field: Entity,
    pub other_density_field: Entity,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ContactDetection {
    pub enabled: bool,
    pub density_threshold: Option<Scalar>,
    pub voxelization_size_limit: Option<Scalar>,
    pub depth_limit: usize,
}

impl Default for ContactDetection {
    fn default() -> Self {
        Self {
            enabled: true,
            density_threshold: None,
            voxelization_size_limit: None,
            depth_limit: usize::MAX,
        }
    }
}

pub struct DensityFieldSpatialObject {
    pub body_entity: Entity,
    pub aabb: Aabb<Scalar>,
    pub collision_profile: CollisionProfile,
}

impl RTreeObject for DensityFieldSpatialObject {
    type Envelope = AABB<[Scalar; 3]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.aabb.min.into_array(), self.aabb.max.into_array())
    }
}

impl PointDistance for DensityFieldSpatialObject {
    fn distance_2(
        &self,
        point: &<Self::Envelope as Envelope>::Point,
    ) -> <<Self::Envelope as Envelope>::Point as Point>::Scalar {
        Rectangle::from_corners(self.aabb.min.into_array(), self.aabb.max.into_array())
            .distance_2(point)
    }

    fn contains_point(&self, point: &<Self::Envelope as Envelope>::Point) -> bool {
        Rectangle::from_corners(self.aabb.min.into_array(), self.aabb.max.into_array())
            .contains_point(point)
    }

    fn distance_2_if_less_or_equal(
        &self,
        point: &<Self::Envelope as Envelope>::Point,
        max_distance_2: <<Self::Envelope as Envelope>::Point as Point>::Scalar,
    ) -> Option<<<Self::Envelope as Envelope>::Point as Point>::Scalar> {
        Rectangle::from_corners(self.aabb.min.into_array(), self.aabb.max.into_array())
            .distance_2_if_less_or_equal(point, max_distance_2)
    }
}

pub struct DensityFieldSpatialExtractor;

impl SpatialExtractor for DensityFieldSpatialExtractor {
    type SpatialObject = DensityFieldSpatialObject;

    fn extract<const LOCKING: bool>(
        world: &World,
    ) -> impl Iterator<Item = (Entity, Self::SpatialObject)> {
        let view = PhysicsAccessView::new(world);
        world
            .query::<LOCKING, (
                Entity,
                &DensityFieldBox,
                Option<&CollisionProfile>,
                &Relation<BodyParentRelation>,
            )>()
            .flat_map(move |(entity, density_field, collision_profile, parents)| {
                let view = view.clone();
                parents.iter().map(move |(_, parent)| {
                    let info = BodyAccessInfo {
                        entity: parent,
                        view: view.clone(),
                    };
                    let aabb = density_field.aabb(&info);
                    (
                        entity,
                        DensityFieldSpatialObject {
                            body_entity: parent,
                            aabb,
                            collision_profile: collision_profile.cloned().unwrap_or_default(),
                        },
                    )
                })
            })
    }
}

#[derive(Debug)]
struct Contact {
    cells_range: Range<usize>,
    bodies: [Entity; 2],
    density_fields: [Entity; 2],
    overlap_region: Aabb<Scalar>,
    movement_since_last_step: Vec3<Scalar>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DensityFieldContact<'a> {
    pub cells: &'a [ShapeOverlapCell],
    pub bodies: [Entity; 2],
    pub density_fields: [Entity; 2],
    pub overlap_region: Aabb<Scalar>,
    pub movement_since_last_step: Vec3<Scalar>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityPair([Entity; 2]);

impl EntityPair {
    pub fn new(a: Entity, b: Entity) -> Self {
        if a < b { Self([a, b]) } else { Self([b, a]) }
    }

    pub fn from_array([a, b]: [Entity; 2]) -> Self {
        Self::new(a, b)
    }

    pub fn into_array(self) -> [Entity; 2] {
        self.0
    }

    pub fn a(&self) -> Entity {
        self.0[0]
    }

    pub fn b(&self) -> Entity {
        self.0[1]
    }

    pub fn has(&self, entity: Entity) -> bool {
        self.0[0] == entity || self.0[1] == entity
    }
}

impl From<[Entity; 2]> for EntityPair {
    fn from(array: [Entity; 2]) -> Self {
        Self::from_array(array)
    }
}

#[derive(Debug, Default)]
pub struct ContactsCache {
    cells: Vec<ShapeOverlapCell>,
    overlapping_contacts: HashMap<EntityPair, Contact>,
    blocking_contacts: HashMap<EntityPair, Contact>,
    saved_overlapping_contacts: HashMap<EntityPair, Contact>,
    saved_blocking_contacts: HashMap<EntityPair, Contact>,
    saved_contact_center_of_mass: HashMap<EntityPair, Vec3<Scalar>>,
    contacts_began: HashSet<EntityPair>,
    contacts_ended: HashSet<EntityPair>,
}

impl ContactsCache {
    pub fn len(&self) -> usize {
        self.overlapping_contacts.len() + self.blocking_contacts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.overlapping_contacts.is_empty() && self.blocking_contacts.is_empty()
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.overlapping_contacts.clear();
        self.blocking_contacts.clear();
        self.saved_overlapping_contacts.clear();
        self.saved_blocking_contacts.clear();
        self.saved_contact_center_of_mass.clear();
        self.contacts_began.clear();
        self.contacts_ended.clear();
    }

    pub fn begin_contacts_update(&mut self) {
        self.saved_contact_center_of_mass.clear();
        self.saved_contact_center_of_mass.extend(
            self.overlapping_contacts
                .iter()
                .chain(self.blocking_contacts.iter())
                .map(|(pair, contact)| {
                    (
                        *pair,
                        self.cells[contact.cells_range.clone()]
                            .iter()
                            .map(|cell| cell.region.center())
                            .sum::<Vec3<Scalar>>()
                            / contact.cells_range.len() as Scalar,
                    )
                }),
        );

        self.saved_overlapping_contacts.clear();
        self.saved_overlapping_contacts
            .extend(self.overlapping_contacts.drain());

        self.saved_blocking_contacts.clear();
        self.saved_blocking_contacts
            .extend(self.blocking_contacts.drain());

        self.cells.clear();
    }

    pub fn end_contacts_update(&mut self) {
        self.contacts_began.clear();
        self.contacts_began.extend(
            self.overlapping_contacts
                .keys()
                .filter(|pair| !self.saved_overlapping_contacts.contains_key(pair))
                .chain(
                    self.blocking_contacts
                        .keys()
                        .filter(|pair| !self.saved_blocking_contacts.contains_key(pair)),
                ),
        );

        self.contacts_ended.clear();
        self.contacts_ended.extend(
            self.saved_overlapping_contacts
                .keys()
                .filter(|pair| !self.overlapping_contacts.contains_key(pair))
                .chain(
                    self.saved_blocking_contacts
                        .keys()
                        .filter(|pair| !self.blocking_contacts.contains_key(pair)),
                ),
        );
    }

    pub fn contacts_began(&self) -> impl Iterator<Item = EntityPair> + '_ {
        self.contacts_began.iter().copied()
    }

    pub fn contacts_ended(&self) -> impl Iterator<Item = EntityPair> + '_ {
        self.contacts_ended.iter().copied()
    }

    pub fn cancel_contact(&mut self, a: Entity, b: Entity) {
        let pair = EntityPair::new(a, b);
        self.overlapping_contacts.remove(&pair);
        self.blocking_contacts.remove(&pair);
    }

    pub fn convert_to_overlapping(&mut self, a: Entity, b: Entity) {
        let pair = EntityPair::new(a, b);
        if let Some(contact) = self.blocking_contacts.remove(&pair) {
            self.overlapping_contacts.insert(pair, contact);
        }
    }

    pub fn convert_to_blocking(&mut self, a: Entity, b: Entity) {
        let pair = EntityPair::new(a, b);
        if let Some(contact) = self.overlapping_contacts.remove(&pair) {
            self.blocking_contacts.insert(pair, contact);
        }
    }

    pub fn does_overlap(&self, a: Entity, b: Entity) -> bool {
        let pair = EntityPair::new(a, b);
        self.overlapping_contacts.contains_key(&pair)
    }

    pub fn does_block(&self, a: Entity, b: Entity) -> bool {
        let pair = EntityPair::new(a, b);
        self.blocking_contacts.contains_key(&pair)
    }

    pub fn has_contact_between(&self, a: Entity, b: Entity) -> bool {
        let pair = EntityPair::new(a, b);
        self.overlapping_contacts.contains_key(&pair) || self.blocking_contacts.contains_key(&pair)
    }

    pub fn has_blocking_contact_of(&self, entity: Entity) -> bool {
        self.blocking_contacts.keys().any(|pair| pair.has(entity))
    }

    pub fn has_overlapping_contact_of(&self, entity: Entity) -> bool {
        self.overlapping_contacts
            .keys()
            .any(|pair| pair.has(entity))
    }

    pub fn has_any_contact_of(&self, entity: Entity) -> bool {
        self.has_blocking_contact_of(entity) || self.has_overlapping_contact_of(entity)
    }

    pub fn overlapping_contact_between(
        &'_ self,
        a: Entity,
        b: Entity,
    ) -> Option<DensityFieldContact<'_>> {
        let pair = EntityPair::new(a, b);
        self.overlapping_contacts
            .get(&pair)
            .map(|contact| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn blocking_contact_between(
        &'_ self,
        a: Entity,
        b: Entity,
    ) -> Option<DensityFieldContact<'_>> {
        let pair = EntityPair::new(a, b);
        self.blocking_contacts
            .get(&pair)
            .map(|contact| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn any_contact_between(&'_ self, a: Entity, b: Entity) -> Option<DensityFieldContact<'_>> {
        self.overlapping_contact_between(a, b)
            .or_else(|| self.blocking_contact_between(a, b))
    }

    pub fn overlapping_contacts_of(
        &'_ self,
        entity: Entity,
    ) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.overlapping_contacts
            .iter()
            .filter(move |(pair, _)| pair.has(entity))
            .map(move |(_, contact)| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn blocking_contacts_of(
        &'_ self,
        entity: Entity,
    ) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.blocking_contacts
            .iter()
            .filter(move |(pair, _)| pair.has(entity))
            .map(move |(_, contact)| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn any_contacts_of(
        &'_ self,
        entity: Entity,
    ) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.overlapping_contacts_of(entity)
            .chain(self.blocking_contacts_of(entity))
    }

    pub fn overlapping_contacts(&'_ self) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.overlapping_contacts
            .values()
            .map(move |contact| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn blocking_contacts(&'_ self) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.blocking_contacts
            .values()
            .map(move |contact| DensityFieldContact {
                cells: &self.cells[contact.cells_range.clone()],
                bodies: contact.bodies,
                density_fields: contact.density_fields,
                overlap_region: contact.overlap_region,
                movement_since_last_step: contact.movement_since_last_step,
            })
    }

    pub fn any_contacts(&'_ self) -> impl Iterator<Item = DensityFieldContact<'_>> + '_ {
        self.overlapping_contacts().chain(self.blocking_contacts())
    }
}

pub fn collect_contacts<const LOCKING: bool>(context: SystemContext) -> Result<(), Box<dyn Error>> {
    let (world, mut contacts, spatial, density_field_lookup, shape_query_local) = context
        .fetch::<(
            &World,
            Res<LOCKING, &mut ContactsCache>,
            Res<LOCKING, &SpatialPartitioning<DensityFieldSpatialExtractor>>,
            // density field lookup
            Lookup<LOCKING, (&DensityFieldBox, &ContactDetection)>,
            Local<LOCKING, &ShapeOverlapQuery>,
        )>()?;

    contacts.begin_contacts_update();

    let view = PhysicsAccessView::new(world);
    let mut lookup_access = density_field_lookup.lookup_access(world);
    let tree = spatial.tree();

    for a in tree.iter() {
        for b in tree.locate_in_envelope_intersecting(a.envelope()) {
            if a.data == b.data {
                continue;
            }
            let pair = EntityPair::new(a.data, b.data);
            if contacts.blocking_contacts.contains_key(&pair)
                || contacts.overlapping_contacts.contains_key(&pair)
            {
                continue;
            }

            let is_overlapping = a
                .geom()
                .collision_profile
                .does_overlap_permissive(&b.geom().collision_profile);
            let is_blocking = a
                .geom()
                .collision_profile
                .does_block(&b.geom().collision_profile);
            if !is_overlapping && !is_blocking {
                continue;
            }

            let Some((field_a, detection_a)) = lookup_access.access(a.data) else {
                continue;
            };
            let Some((field_b, detection_b)) = lookup_access.access(b.data) else {
                continue;
            };
            if !detection_a.enabled || !detection_b.enabled {
                continue;
            }

            let fields: [&dyn DensityField; 2] = [&**field_a, &**field_b];
            let infos = [
                &BodyAccessInfo {
                    entity: a.geom().body_entity,
                    view: view.clone(),
                },
                &BodyAccessInfo {
                    entity: b.geom().body_entity,
                    view: view.clone(),
                },
            ];
            let mut query = shape_query_local.clone();
            query.region_limit = if let Some(region_limit) = query.region_limit {
                Some(
                    a.geom()
                        .aabb
                        .intersection(b.geom().aabb)
                        .intersection(region_limit),
                )
            } else {
                Some(a.geom().aabb.intersection(b.geom().aabb))
            };
            for detection in [detection_a, detection_b] {
                if let Some(value) = detection.density_threshold {
                    query.density_threshold = query.density_threshold.min(value);
                }
                if let Some(value) = detection.voxelization_size_limit {
                    query.voxelization_size_limit = query.voxelization_size_limit.min(value);
                }
            }
            query.depth_limit = query
                .depth_limit
                .min(detection_a.depth_limit)
                .min(detection_b.depth_limit);
            let start = contacts.cells.len();
            let Some(overlap_region) = query.query_field_pair(fields, infos, &mut contacts.cells)
            else {
                continue;
            };
            let end = contacts.cells.len();
            if end > start {
                let center_of_mass = contacts.cells[start..end]
                    .iter()
                    .map(|cell| cell.region.center())
                    .sum::<Vec3<Scalar>>()
                    / (end - start) as Scalar;
                let prev_center_of_mass = contacts
                    .saved_contact_center_of_mass
                    .get(&pair)
                    .copied()
                    .unwrap_or(center_of_mass);
                let contact = Contact {
                    cells_range: start..end,
                    bodies: [a.geom().body_entity, b.geom().body_entity],
                    density_fields: [a.data, b.data],
                    overlap_region,
                    movement_since_last_step: center_of_mass - prev_center_of_mass,
                };
                if is_blocking {
                    contacts.blocking_contacts.insert(pair, contact);
                } else {
                    contacts.overlapping_contacts.insert(pair, contact);
                }
            }
        }
    }

    contacts.end_contacts_update();

    Ok(())
}

pub fn dispatch_contact_events<const LOCKING: bool>(
    context: SystemContext,
) -> Result<(), Box<dyn Error>> {
    let (world, contacts, events_lookup) = context.fetch::<(
        &World,
        Res<LOCKING, &ContactsCache>,
        // body lookup.
        Lookup<LOCKING, &EventDispatcher<ContactEvent>>,
    )>()?;

    let mut events_lookup = events_lookup.lookup_access(world);

    for (contact, blocking, began) in contacts
        .blocking_contacts
        .values()
        .map(|contact| {
            (
                contact,
                true,
                !contacts
                    .saved_blocking_contacts
                    .contains_key(&EntityPair::from_array(contact.density_fields)),
            )
        })
        .chain(contacts.overlapping_contacts.values().map(|contact| {
            (
                contact,
                false,
                !contacts
                    .saved_overlapping_contacts
                    .contains_key(&EntityPair::from_array(contact.density_fields)),
            )
        }))
    {
        let body_events = contact.bodies.map(|entity| events_lookup.access(entity));

        if began {
            if let Some(event) = body_events[0] {
                event.dispatch(&ContactEvent {
                    kind: ContactEventKind::Began,
                    blocking,
                    self_body: contact.bodies[0],
                    other_body: contact.bodies[1],
                    self_density_field: contact.density_fields[0],
                    other_density_field: contact.density_fields[1],
                });
            }
            if let Some(event) = body_events[1] {
                event.dispatch(&ContactEvent {
                    kind: ContactEventKind::Began,
                    blocking,
                    self_body: contact.bodies[1],
                    other_body: contact.bodies[0],
                    self_density_field: contact.density_fields[1],
                    other_density_field: contact.density_fields[0],
                });
            }
        } else {
            if let Some(event) = body_events[0] {
                event.dispatch(&ContactEvent {
                    kind: ContactEventKind::Continue,
                    blocking,
                    self_body: contact.bodies[0],
                    other_body: contact.bodies[1],
                    self_density_field: contact.density_fields[0],
                    other_density_field: contact.density_fields[1],
                });
            }
            if let Some(event) = body_events[1] {
                event.dispatch(&ContactEvent {
                    kind: ContactEventKind::Continue,
                    blocking,
                    self_body: contact.bodies[1],
                    other_body: contact.bodies[0],
                    self_density_field: contact.density_fields[1],
                    other_density_field: contact.density_fields[0],
                });
            }
        }
    }

    for (contact, blocking) in contacts
        .saved_blocking_contacts
        .iter()
        .filter(|(pair, _)| !contacts.blocking_contacts.contains_key(pair))
        .map(|(_, contact)| (contact, true))
        .chain(
            contacts
                .saved_overlapping_contacts
                .iter()
                .filter(|(pair, _)| !contacts.overlapping_contacts.contains_key(pair))
                .map(|(_, contact)| (contact, false)),
        )
    {
        let body_events = contact.bodies.map(|entity| events_lookup.access(entity));

        if let Some(event) = body_events[0] {
            event.dispatch(&ContactEvent {
                kind: ContactEventKind::Ended,
                blocking,
                self_body: contact.bodies[0],
                other_body: contact.bodies[1],
                self_density_field: contact.density_fields[0],
                other_density_field: contact.density_fields[1],
            });
        }
        if let Some(event) = body_events[1] {
            event.dispatch(&ContactEvent {
                kind: ContactEventKind::Ended,
                blocking,
                self_body: contact.bodies[1],
                other_body: contact.bodies[0],
                self_density_field: contact.density_fields[1],
                other_density_field: contact.density_fields[0],
            });
        }
    }

    Ok(())
}

pub struct RepulsiveCollisionCorrection<'a> {
    pub linear_correction: &'a mut Vec3<Scalar>,
    pub angular_correction: &'a mut Vec3<Scalar>,
    pub contact_normal: Vec3<Scalar>,
    pub position: &'a Position,
    pub rotation: Option<&'a Rotation>,
    pub contact: DensityFieldContact<'a>,
    pub body_index: usize,
    pub weight: [Scalar; 2],
    pub inverse_mass: [Scalar; 2],
    pub callbacks: &'a RepulsiveCollisionCallbacks,
}

pub struct RepulsiveCollisionModifier<'a> {
    pub penetration: &'a mut Scalar,
    pub point: &'a mut Vec3<Scalar>,
    pub contact_normal: Vec3<Scalar>,
    pub position: &'a Position,
    pub rotation: Option<&'a Rotation>,
    pub contact: DensityFieldContact<'a>,
    pub body_index: usize,
    pub inverse_mass: [Scalar; 2],
    pub callbacks: &'a RepulsiveCollisionCallbacks,
}

pub struct RepulsiveCollisionCallbacks {
    #[allow(clippy::type_complexity)]
    corrections: Vec<Box<dyn Fn(RepulsiveCollisionCorrection<'_>) + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    modifiers: Vec<Box<dyn Fn(RepulsiveCollisionModifier<'_>) + Send + Sync>>,
}

impl Default for RepulsiveCollisionCallbacks {
    fn default() -> Self {
        Self::empty().correction(default_repulsive_collision_correction)
    }
}

impl RepulsiveCollisionCallbacks {
    pub fn empty() -> Self {
        Self {
            corrections: Default::default(),
            modifiers: Default::default(),
        }
    }

    pub fn correction(
        mut self,
        callback: impl Fn(RepulsiveCollisionCorrection<'_>) + Send + Sync + 'static,
    ) -> Self {
        self.corrections.push(Box::new(callback));
        self
    }

    pub fn modifier(
        mut self,
        callback: impl Fn(RepulsiveCollisionModifier<'_>) + Send + Sync + 'static,
    ) -> Self {
        self.modifiers.push(Box::new(callback));
        self
    }

    pub fn run_corrections(&self, correction: RepulsiveCollisionCorrection<'_>) {
        if self.corrections.is_empty() {
            return;
        }

        let RepulsiveCollisionCorrection {
            linear_correction,
            angular_correction,
            contact_normal,
            position,
            rotation,
            contact,
            body_index,
            weight,
            inverse_mass,
            callbacks,
        } = correction;

        for callback in &self.corrections {
            callback(RepulsiveCollisionCorrection {
                linear_correction,
                angular_correction,
                contact_normal,
                position,
                rotation,
                contact,
                body_index,
                weight,
                inverse_mass,
                callbacks,
            });
        }
    }

    pub fn run_modifiers(&self, modifier: RepulsiveCollisionModifier<'_>) {
        if self.modifiers.is_empty() {
            return;
        }

        let RepulsiveCollisionModifier {
            penetration,
            point,
            contact_normal,
            position,
            rotation,
            contact,
            body_index,
            inverse_mass,
            callbacks,
        } = modifier;

        for callback in &self.modifiers {
            callback(RepulsiveCollisionModifier {
                penetration,
                point,
                contact_normal,
                position,
                rotation,
                contact,
                body_index,
                inverse_mass,
                callbacks,
            });
        }
    }
}

pub struct RepulsiveCollisionSolver<const LOCKING: bool>;

impl<const LOCKING: bool> System for RepulsiveCollisionSolver<LOCKING> {
    fn run(&self, context: SystemContext) -> Result<(), Box<dyn Error>> {
        let (world, simulation, contacts, body_lookup, particle_lookup, callbacks) = context
            .fetch::<(
                &World,
                Res<LOCKING, &PhysicsSimulation>,
                Res<LOCKING, &ContactsCache>,
                // body lookup
                Lookup<
                    LOCKING,
                    (
                        Option<&Relation<BodyParticleRelation>>,
                        Option<&Mass>,
                        Option<&BodyMaterial>,
                        Include<PhysicsBody>,
                    ),
                >,
                // particle lookup
                Lookup<
                    LOCKING,
                    (
                        &mut Position,
                        Option<&mut Rotation>,
                        &mut LinearVelocity,
                        Option<&mut AngularVelocity>,
                        Include<PhysicsParticle>,
                    ),
                >,
                Local<LOCKING, &RepulsiveCollisionCallbacks>,
            )>()?;

        if contacts.is_empty() {
            return Ok(());
        }

        let inverse_delta_time = simulation.inverse_delta_time();
        let mut body_lookup_access = body_lookup.lookup_access(world);
        let mut particle_lookup_access = particle_lookup.lookup_access(world);

        for contact in contacts.blocking_contacts() {
            let body_access = contact
                .bodies
                .map(|entity| body_lookup_access.access(entity));
            let Some((relations_a, mass_a, material_a, _)) = body_access[0] else {
                continue;
            };
            let Some((relations_b, mass_b, material_b, _)) = body_access[1] else {
                continue;
            };
            if (mass_a.is_none() && mass_b.is_none())
                || (relations_a.is_none() && relations_b.is_none())
            {
                continue;
            }

            let inverse_mass_a = mass_a.map(|mass| mass.inverse()).unwrap_or_default();
            let inverse_mass_b = mass_b.map(|mass| mass.inverse()).unwrap_or_default();
            let inverse_mass = [inverse_mass_a, inverse_mass_b];

            let material_a = material_a.copied().unwrap_or_default();
            let material_b = material_b.copied().unwrap_or_default();
            let material = [material_a, material_b];

            let weight_a = inverse_mass_a / (inverse_mass_a + inverse_mass_b);
            let weight_b = 1.0 - weight_a;
            let weight = [weight_a, weight_b];

            for (entity, body_index) in relations_a
                .into_iter()
                .flat_map(|relation| relation.iter())
                .map(|(_, entity)| (entity, 0))
                .chain(
                    relations_b
                        .into_iter()
                        .flat_map(|relation| relation.iter())
                        .map(|(_, entity)| (entity, 1)),
                )
            {
                let Some((position, rotation, linear_velocity, angular_velocity, _)) =
                    particle_lookup_access.access(entity)
                else {
                    continue;
                };

                let mut linear_correction = Vec3::<Scalar>::zero();
                let mut angular_correction = Vec3::<Scalar>::zero();
                let contact_normal = contact
                    .cells
                    .iter()
                    .map(|cell| cell.normal[body_index])
                    .sum::<Vec3<Scalar>>()
                    .try_normalized()
                    .unwrap_or_default();

                callbacks.run_corrections(RepulsiveCollisionCorrection {
                    linear_correction: &mut linear_correction,
                    angular_correction: &mut angular_correction,
                    contact_normal,
                    position,
                    rotation: rotation.as_deref(),
                    contact,
                    body_index,
                    weight,
                    inverse_mass,
                    callbacks: &callbacks,
                });

                position.current += linear_correction;
                linear_velocity.value += linear_correction * inverse_delta_time;

                if let Some(rotation) = rotation {
                    let angle = angular_correction.magnitude();
                    if angle > Scalar::EPSILON {
                        let axis = angular_correction / angle;
                        let delta = quat_from_axis_angle(axis, angle);
                        rotation.current = (rotation.current * delta).normalized();

                        if let Some(angular_velocity) = angular_velocity {
                            let axis = angular_correction / angle;
                            angular_velocity.value += axis * (angle * inverse_delta_time);
                        }
                    }
                }

                let relative_velocity =
                    linear_velocity.value - contact.movement_since_last_step * inverse_delta_time;
                let normal_velocity = relative_velocity.dot(contact_normal);
                let tangent_velocity = relative_velocity - contact_normal * normal_velocity;

                let restitution = material[body_index].restitution;
                let impulse = -normal_velocity * (1.0 - restitution);
                linear_velocity.value += contact_normal * impulse;
                // TODO: angular velocity.

                let friction = material[body_index].friction;
                let friction_direction = -tangent_velocity.try_normalized().unwrap_or_default();
                let friction_magnitude = friction * normal_velocity.abs();
                linear_velocity.value += friction_direction * friction_magnitude;
                // TODO: angular velocity.
            }
        }
        Ok(())
    }
}

pub fn default_repulsive_collision_correction(correction: RepulsiveCollisionCorrection) {
    let RepulsiveCollisionCorrection {
        linear_correction,
        angular_correction,
        contact_normal,
        position,
        rotation,
        contact,
        body_index,
        weight,
        inverse_mass,
        callbacks,
    } = correction;

    let mut penetration = 0.0;
    let mut total_area = 0.0;
    let mut response_normal = Vec3::<Scalar>::zero();
    let mut center_of_mass = Vec3::<Scalar>::zero();
    for cell in contact.cells {
        let area = cell.area();
        penetration += Vec3::from(cell.region.size()).dot(contact_normal).abs() * area;
        total_area += area;
        response_normal += cell.normal[body_index].reflected(contact_normal);
        center_of_mass += cell.region.center();
    }
    penetration /= total_area;
    response_normal = response_normal.try_normalized().unwrap_or_default();
    center_of_mass /= contact.cells.len() as Scalar;
    let mut point = position.current;

    callbacks.run_modifiers(RepulsiveCollisionModifier {
        penetration: &mut penetration,
        point: &mut point,
        contact_normal,
        position,
        rotation,
        contact,
        body_index,
        inverse_mass,
        callbacks,
    });

    let impulse = response_normal * penetration * inverse_mass[body_index];
    *linear_correction += impulse * weight[body_index];
    if rotation.is_some() {
        *angular_correction += (point - center_of_mass).cross(impulse) * weight[body_index];
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::approx_constant)]

    use super::*;
    use crate::{
        PhysicsPlugin,
        components::{BodyDensityFieldRelation, ExternalForces, LinearVelocity, PhysicsBody},
        density_fields::{aabb::AabbDensityField, sphere::SphereDensityField},
    };
    use anput::{scheduler::GraphScheduler, third_party::moirai::jobs::Jobs, universe::Universe};
    use vek::Vec3;

    #[test]
    fn test_entity_pair() {
        let a = Entity::new(0, 0).unwrap();
        let b = Entity::new(1, 0).unwrap();
        let c = Entity::new(0, 1).unwrap();
        let d = Entity::new(1, 1).unwrap();

        assert_eq!(EntityPair::new(a, b), EntityPair([a, b]));
        assert_eq!(EntityPair::new(b, a), EntityPair([a, b]));
        assert_eq!(EntityPair::new(c, d), EntityPair([c, d]));
        assert_eq!(EntityPair::new(d, c), EntityPair([c, d]));
        assert_eq!(EntityPair::new(a, c), EntityPair([a, c]));
        assert_eq!(EntityPair::new(c, a), EntityPair([a, c]));
        assert_eq!(EntityPair::new(b, c), EntityPair([b, c]));
        assert_eq!(EntityPair::new(c, b), EntityPair([b, c]));
    }

    #[test]
    fn test_collision_profile() {
        let a = CollisionProfile::default();
        let b = CollisionProfile::default().with_block(CollisionMask::flag(0));
        let c = CollisionProfile::default().with_block(CollisionMask::flag(1));
        let d = CollisionProfile::default()
            .with_block(CollisionMask::flag(0))
            .with_block(CollisionMask::flag(1));

        assert!(!a.does_block(&a));
        assert!(!a.does_block(&b));
        assert!(!a.does_block(&c));
        assert!(!a.does_block(&d));

        assert!(!b.does_block(&a));
        assert!(b.does_block(&b));
        assert!(!b.does_block(&c));
        assert!(b.does_block(&d));

        assert!(!c.does_block(&a));
        assert!(!c.does_block(&b));
        assert!(c.does_block(&c));
        assert!(c.does_block(&d));

        assert!(!d.does_block(&a));
        assert!(d.does_block(&b));
        assert!(d.does_block(&c));
        assert!(d.does_block(&d));
    }

    #[test]
    fn test_collision_system() -> Result<(), Box<dyn Error>> {
        let mut universe = Universe::default().with_plugin(
            PhysicsPlugin::<true>::default()
                .simulation(PhysicsSimulation {
                    delta_time: 1.0,
                    ..Default::default()
                })
                .make(),
        );
        let jobs = Jobs::default();
        let scheduler = GraphScheduler::<true>;

        let a = universe.simulation.spawn((
            PhysicsBody,
            DensityFieldBox::new(AabbDensityField {
                aabb: Aabb {
                    min: Vec3::new(-100.0, -100.0, 0.0),
                    max: Vec3::new(100.0, 0.0, 0.0),
                },
                density: 1.0,
            }),
            CollisionProfile::default().with_block(CollisionMask::flag(0)),
            ContactDetection::default(),
        ))?;
        universe
            .simulation
            .relate::<true, _>(BodyDensityFieldRelation, a, a)
            .unwrap();
        universe
            .simulation
            .relate::<true, _>(BodyParentRelation, a, a)
            .unwrap();

        let b = universe.simulation.spawn((
            PhysicsBody,
            PhysicsParticle,
            DensityFieldBox::new(SphereDensityField::<true>::new_hard(1.0, 10.0)),
            CollisionProfile::default().with_block(CollisionMask::flag(0)),
            ContactDetection {
                depth_limit: 0,
                ..Default::default()
            },
            Mass::new(1.0),
            Position::new(Vec3::new(0.0, 10.0, 0.0)),
            LinearVelocity {
                value: Vec3::new(-5.0, -5.0, 0.0),
            },
            ExternalForces::default(),
        ))?;
        universe
            .simulation
            .relate::<true, _>(BodyParticleRelation, b, b)
            .unwrap();
        universe
            .simulation
            .relate::<true, _>(BodyDensityFieldRelation, b, b)
            .unwrap();
        universe
            .simulation
            .relate::<true, _>(BodyParentRelation, b, b)
            .unwrap();

        scheduler.run(&jobs, &mut universe)?;

        // TODO: responses are a bit delayed? might be worth looking into it.
        assert_eq!(
            universe
                .simulation
                .component::<true, Position>(b)
                .unwrap()
                .current,
            Vec3::new(-5.0, 5.0, 0.0)
        );
        assert_eq!(
            universe
                .simulation
                .component::<true, LinearVelocity>(b)
                .unwrap()
                .value,
            Vec3::new(-5.0, -5.0, 0.0)
        );

        Ok(())
    }
}
