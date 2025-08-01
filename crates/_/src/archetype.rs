use crate::{
    bundle::{Bundle, BundleColumns},
    component::Component,
    entity::{Entity, EntityDenseMap},
};
use intuicio_core::types::Type;
use intuicio_data::{Finalize, non_zero_alloc, non_zero_dealloc, type_hash::TypeHash};
use std::{
    alloc::Layout,
    error::Error,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

fn traced_spin_loop() {
    #[cfg(feature = "deadlock-trace")]
    println!(
        "* DEADLOCK BACKTRACE: {}",
        std::backtrace::Backtrace::force_capture()
    );
    std::hint::spin_loop();
}

#[derive(Debug, PartialEq, Eq)]
pub enum ArchetypeError {
    ColumnAlreadyUniquelyAccessed {
        type_hash: TypeHash,
    },
    ColumnNotFound {
        type_hash: TypeHash,
    },
    ColumnTypeIsDuplicated {
        type_hash: TypeHash,
        index: usize,
        duplicate_index: usize,
    },
    ColumnTypeMismatch {
        provided: TypeHash,
        expected: TypeHash,
    },
    IndexNotFound {
        index: usize,
    },
    IndexAlreadyOccupied {
        index: usize,
    },
    EntityNotFound {
        entity: Entity,
    },
    EntityAlreadyOccupied {
        entity: Entity,
    },
    ColumnSdirLocked {
        type_hash: TypeHash,
    },
}

impl Error for ArchetypeError {}

impl std::fmt::Display for ArchetypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnAlreadyUniquelyAccessed { type_hash } => {
                write!(f, "Column is already uniquelly accessed: {type_hash:?}")
            }
            Self::ColumnNotFound { type_hash } => write!(f, "Column not found: {type_hash:?}"),
            Self::ColumnTypeIsDuplicated {
                type_hash,
                index,
                duplicate_index,
            } => write!(
                f,
                "Column type: {type_hash:?} at index: {index} has duplicate at index: {duplicate_index}"
            ),
            Self::ColumnTypeMismatch { provided, expected } => write!(
                f,
                "Provided column: {provided:?} does not match expected: {expected:?}"
            ),
            Self::IndexNotFound { index } => write!(f, "Entity index not found: {index}"),
            Self::IndexAlreadyOccupied { index } => {
                write!(f, "Entity index already occupied: {index}")
            }
            Self::EntityNotFound { entity } => write!(f, "Entity not found: {entity}"),
            Self::EntityAlreadyOccupied { entity } => {
                write!(f, "Entity already occupied: {entity}")
            }
            Self::ColumnSdirLocked { type_hash } => write!(
                f,
                "Column: {type_hash:?} is locked for Spawn/Despawn/Insert/Remove operations"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq)]
pub struct ArchetypeColumnInfo {
    type_hash: TypeHash,
    layout: Layout,
    finalizer: unsafe fn(*mut ()),
}

impl ArchetypeColumnInfo {
    pub fn new<T: Finalize>() -> Self {
        Self {
            type_hash: TypeHash::of::<T>(),
            layout: Layout::new::<T>().pad_to_align(),
            finalizer: T::finalize_raw,
        }
    }

    /// # Safety
    pub unsafe fn new_raw(
        type_hash: TypeHash,
        layout: Layout,
        finalizer: unsafe fn(*mut ()),
    ) -> Self {
        Self {
            type_hash,
            layout: layout.pad_to_align(),
            finalizer,
        }
    }

    pub fn from_type(type_: &Type) -> Self {
        Self {
            type_hash: type_.type_hash(),
            layout: *type_.layout(),
            finalizer: unsafe { type_.finalizer() },
        }
    }

    #[inline]
    pub fn type_hash(&self) -> TypeHash {
        self.type_hash
    }

    #[inline]
    pub fn layout(&self) -> Layout {
        self.layout
    }

    #[inline]
    pub fn finalizer(&self) -> unsafe fn(*mut ()) {
        self.finalizer
    }
}

impl PartialEq for ArchetypeColumnInfo {
    fn eq(&self, other: &Self) -> bool {
        self.type_hash.eq(&other.type_hash) && self.layout.eq(&other.layout)
    }
}

pub struct ArchetypeColumnAccess<'a, const LOCKING: bool, T: Component> {
    column: &'a Column,
    size: usize,
    unique: bool,
    _phantom: PhantomData<fn() -> T>,
}

impl<const LOCKING: bool, T: Component> Drop for ArchetypeColumnAccess<'_, LOCKING, T> {
    fn drop(&mut self) {
        if self.unique {
            if LOCKING {
                while self
                    .column
                    .unique_access
                    .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else {
                let _ = self.column.unique_access.compare_exchange(
                    true,
                    false,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<'a, const LOCKING: bool, T: Component> ArchetypeColumnAccess<'a, LOCKING, T> {
    unsafe fn new(column: &'a Column, unique: bool, size: usize) -> Result<Self, ArchetypeError> {
        if unique {
            if LOCKING {
                while column
                    .unique_access
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else if column
                .unique_access
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                    type_hash: column.info.type_hash,
                });
            }
        } else if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            size,
            unique,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn info(&self) -> &ArchetypeColumnInfo {
        &self.column.info
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// # Safety
    #[inline]
    pub unsafe fn memory(&self) -> *mut u8 {
        self.column.memory
    }

    /// # Safety
    pub unsafe fn data(&self, index: usize) -> Result<*mut u8, ArchetypeError> {
        if index < self.size {
            Ok(unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
            })
        } else {
            Err(ArchetypeError::IndexNotFound { index })
        }
    }

    pub fn read(&self, index: usize) -> Option<&T> {
        if index < self.size {
            unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_ref()
            }
        } else {
            None
        }
    }

    pub fn write(&mut self, index: usize) -> Option<&mut T> {
        if index < self.size && self.unique {
            unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_mut()
            }
        } else {
            None
        }
    }
}

pub struct ArchetypeDynamicColumnAccess<'a, const LOCKING: bool> {
    column: &'a Column,
    size: usize,
    unique: bool,
}

impl<const LOCKING: bool> Drop for ArchetypeDynamicColumnAccess<'_, LOCKING> {
    fn drop(&mut self) {
        if self.unique {
            if LOCKING {
                while self
                    .column
                    .unique_access
                    .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else {
                let _ = self.column.unique_access.compare_exchange(
                    true,
                    false,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<'a, const LOCKING: bool> ArchetypeDynamicColumnAccess<'a, LOCKING> {
    fn new(column: &'a Column, unique: bool, size: usize) -> Result<Self, ArchetypeError> {
        if unique {
            if LOCKING {
                while column
                    .unique_access
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else if column
                .unique_access
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                    type_hash: column.info.type_hash,
                });
            }
        } else if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            size,
            unique,
        })
    }

    #[inline]
    pub fn info(&self) -> &ArchetypeColumnInfo {
        &self.column.info
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// # Safety
    #[inline]
    pub unsafe fn memory(&self) -> *mut u8 {
        self.column.memory
    }

    /// # Safety
    pub unsafe fn data(&self, index: usize) -> Result<*mut u8, ArchetypeError> {
        if index < self.size {
            Ok(unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
            })
        } else {
            Err(ArchetypeError::IndexNotFound { index })
        }
    }

    pub fn read<T: Component>(&self, index: usize) -> Option<&T> {
        if index < self.size && self.column.info.type_hash == TypeHash::of::<T>() {
            unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_ref()
            }
        } else {
            None
        }
    }

    pub fn write<T: Component>(&mut self, index: usize) -> Option<&mut T> {
        if index < self.size && self.unique && self.column.info.type_hash == TypeHash::of::<T>() {
            unsafe {
                self.column
                    .memory
                    .add(index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_mut()
            }
        } else {
            None
        }
    }

    pub fn dynamic_item(&self, index: usize) -> Result<ArchetypeDynamicColumnItem, ArchetypeError> {
        let memory = unsafe { self.data(index)? };
        Ok(ArchetypeDynamicColumnItem {
            memory,
            type_hash: self.column.info.type_hash,
            unique: self.unique,
            _phantom: PhantomData,
        })
    }
}

pub struct ArchetypeEntityColumnAccess<'a, const LOCKING: bool, T: Component> {
    column: &'a Column,
    index: usize,
    unique: bool,
    _phantom: PhantomData<fn() -> T>,
}

impl<const LOCKING: bool, T: Component> Drop for ArchetypeEntityColumnAccess<'_, LOCKING, T> {
    fn drop(&mut self) {
        if self.unique {
            if LOCKING {
                while self
                    .column
                    .unique_access
                    .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else {
                let _ = self.column.unique_access.compare_exchange(
                    true,
                    false,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<'a, const LOCKING: bool, T: Component> ArchetypeEntityColumnAccess<'a, LOCKING, T> {
    unsafe fn new(column: &'a Column, unique: bool, index: usize) -> Result<Self, ArchetypeError> {
        if unique {
            if LOCKING {
                while column
                    .unique_access
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else if column
                .unique_access
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                    type_hash: column.info.type_hash,
                });
            }
        } else if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            index,
            unique,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn info(&self) -> &ArchetypeColumnInfo {
        &self.column.info
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// # Safety
    #[inline]
    pub unsafe fn data(&self) -> *mut u8 {
        unsafe {
            self.column
                .memory
                .add(self.index * self.column.info.layout.size())
        }
    }

    pub fn read(&self) -> Option<&T> {
        unsafe {
            self.column
                .memory
                .add(self.index * self.column.info.layout.size())
                .cast::<T>()
                .as_ref()
        }
    }

    pub fn write(&mut self) -> Option<&mut T> {
        if self.unique {
            unsafe {
                self.column
                    .memory
                    .add(self.index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_mut()
            }
        } else {
            None
        }
    }
}

pub struct ArchetypeDynamicEntityColumnAccess<'a, const LOCKING: bool> {
    column: &'a Column,
    index: usize,
    unique: bool,
}

impl<const LOCKING: bool> Drop for ArchetypeDynamicEntityColumnAccess<'_, LOCKING> {
    fn drop(&mut self) {
        if self.unique {
            if LOCKING {
                while self
                    .column
                    .unique_access
                    .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else {
                let _ = self.column.unique_access.compare_exchange(
                    true,
                    false,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<'a, const LOCKING: bool> ArchetypeDynamicEntityColumnAccess<'a, LOCKING> {
    fn new(column: &'a Column, unique: bool, index: usize) -> Result<Self, ArchetypeError> {
        if unique {
            if LOCKING {
                while column
                    .unique_access
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else if column
                .unique_access
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                    type_hash: column.info.type_hash,
                });
            }
        } else if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            index,
            unique,
        })
    }

    #[inline]
    pub fn info(&self) -> &ArchetypeColumnInfo {
        &self.column.info
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// # Safety
    #[inline]
    pub unsafe fn data(&self) -> *mut u8 {
        unsafe {
            self.column
                .memory
                .add(self.index * self.column.info.layout.size())
        }
    }

    pub fn read<T: Component>(&self) -> Option<&T> {
        if self.column.info.type_hash == TypeHash::of::<T>() {
            unsafe {
                self.column
                    .memory
                    .add(self.index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_ref()
            }
        } else {
            None
        }
    }

    pub fn write<T: Component>(&mut self) -> Option<&mut T> {
        if self.unique && self.column.info.type_hash == TypeHash::of::<T>() {
            unsafe {
                self.column
                    .memory
                    .add(self.index * self.column.info.layout.size())
                    .cast::<T>()
                    .as_mut()
            }
        } else {
            None
        }
    }
}

pub struct ArchetypeEntityRowAccess<'a> {
    columns: Box<[&'a Column]>,
    index: usize,
}

impl Drop for ArchetypeEntityRowAccess<'_> {
    fn drop(&mut self) {
        for column in self.columns.as_ref() {
            while column
                .unique_access
                .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                traced_spin_loop();
            }
        }
    }
}

impl<'a> ArchetypeEntityRowAccess<'a> {
    fn new(columns: Box<[&'a Column]>, index: usize) -> Self {
        for column in columns.as_ref() {
            while column
                .unique_access
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                traced_spin_loop();
            }
        }
        Self { columns, index }
    }

    pub fn len(&self) -> usize {
        self.columns.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.as_ref().is_empty()
    }

    /// # Safety
    pub unsafe fn data(&self, type_hash: TypeHash) -> Result<*mut u8, ArchetypeError> {
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return Ok(unsafe { column.memory.add(self.index * column.info.layout.size()) });
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn types(&self) -> impl Iterator<Item = TypeHash> + '_ {
        self.columns.iter().map(|column| column.info.type_hash)
    }

    pub fn columns(&self) -> impl Iterator<Item = &ArchetypeColumnInfo> {
        self.columns.iter().map(|column| &column.info)
    }

    pub fn read<T: Component>(&self) -> Option<&T> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                unsafe {
                    return column
                        .memory
                        .add(self.index * column.info.layout.size())
                        .cast::<T>()
                        .as_ref();
                }
            }
        }
        None
    }

    pub fn write<T: Component>(&mut self) -> Option<&mut T> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                unsafe {
                    return column
                        .memory
                        .add(self.index * column.info.layout.size())
                        .cast::<T>()
                        .as_mut();
                }
            }
        }
        None
    }

    /// # Safety
    pub unsafe fn initialize<T: Component>(&self, value: T) -> Result<(), ArchetypeError> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                unsafe {
                    column
                        .memory
                        .add(self.index * column.info.layout.size())
                        .cast::<T>()
                        .write(value)
                };
                return Ok(());
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    /// # Safety
    pub unsafe fn initialize_raw(&self, type_: &Type) -> Result<(), ArchetypeError> {
        let type_hash = type_.type_hash();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                unsafe {
                    type_.initialize(column.memory.add(self.index * column.info.layout.size()) as _)
                };
                return Ok(());
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }
}

pub struct ArchetypeColumnReadIter<'a, T: Component> {
    memory: *mut u8,
    stride: usize,
    left: usize,
    _phantom: PhantomData<fn() -> &'a T>,
}

impl<'a, T: Component> ArchetypeColumnReadIter<'a, T> {
    fn new<const LOCKING: bool>(column: &'a Column, size: usize) -> Result<Self, ArchetypeError> {
        if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            memory: column.memory,
            stride: column.info.layout.size(),
            left: size,
            _phantom: PhantomData,
        })
    }
}

impl<'a, T: Component> Iterator for ArchetypeColumnReadIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        unsafe {
            let result = self.memory.cast::<T>().as_ref()?;
            self.memory = self.memory.add(self.stride);
            Some(result)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

pub struct ArchetypeColumnWriteIter<'a, const LOCKING: bool, T: Component> {
    column: &'a Column,
    memory: *mut u8,
    stride: usize,
    left: usize,
    _phantom: PhantomData<fn() -> &'a mut T>,
}

impl<const LOCKING: bool, T: Component> Drop for ArchetypeColumnWriteIter<'_, LOCKING, T> {
    fn drop(&mut self) {
        if LOCKING {
            while self
                .column
                .unique_access
                .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                traced_spin_loop();
            }
        } else {
            let _ = self.column.unique_access.compare_exchange(
                true,
                false,
                Ordering::Acquire,
                Ordering::Relaxed,
            );
        }
    }
}

impl<'a, const LOCKING: bool, T: Component> ArchetypeColumnWriteIter<'a, LOCKING, T> {
    fn new(column: &'a Column, size: usize) -> Result<Self, ArchetypeError> {
        if LOCKING {
            while column
                .unique_access
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                traced_spin_loop();
            }
        } else if column
            .unique_access
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            memory: column.memory,
            stride: column.info.layout.size(),
            left: size,
            _phantom: PhantomData,
        })
    }
}

impl<'a, const LOCKING: bool, T: Component> Iterator for ArchetypeColumnWriteIter<'a, LOCKING, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        unsafe {
            let result = self.memory.cast::<T>().as_mut()?;
            self.memory = self.memory.add(self.stride);
            Some(result)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

pub struct ArchetypeDynamicColumnItem<'a> {
    memory: *mut u8,
    type_hash: TypeHash,
    unique: bool,
    _phantom: PhantomData<&'a ()>,
}

impl ArchetypeDynamicColumnItem<'_> {
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn type_hash(&self) -> TypeHash {
        self.type_hash
    }

    /// # Safety
    pub unsafe fn data(&self) -> *mut u8 {
        self.memory
    }

    pub fn read<T: Component>(&self) -> Option<&T> {
        if self.type_hash == TypeHash::of::<T>() {
            unsafe { self.memory.cast::<T>().as_ref() }
        } else {
            None
        }
    }

    pub fn write<T: Component>(&mut self) -> Option<&mut T> {
        if self.unique && self.type_hash == TypeHash::of::<T>() {
            unsafe { self.memory.cast::<T>().as_mut() }
        } else {
            None
        }
    }
}

pub struct ArchetypeDynamicColumnIter<'a, const LOCKING: bool> {
    column: &'a Column,
    memory: *mut u8,
    type_hash: TypeHash,
    stride: usize,
    left: usize,
    unique: bool,
}

impl<const LOCKING: bool> Drop for ArchetypeDynamicColumnIter<'_, LOCKING> {
    fn drop(&mut self) {
        if self.unique {
            if LOCKING {
                while self
                    .column
                    .unique_access
                    .compare_exchange_weak(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else {
                let _ = self.column.unique_access.compare_exchange(
                    true,
                    false,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<'a, const LOCKING: bool> ArchetypeDynamicColumnIter<'a, LOCKING> {
    fn new(column: &'a Column, unique: bool, size: usize) -> Result<Self, ArchetypeError> {
        if unique {
            if LOCKING {
                while column
                    .unique_access
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    traced_spin_loop();
                }
            } else if column
                .unique_access
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                    type_hash: column.info.type_hash,
                });
            }
        } else if LOCKING {
            while column.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
        } else if column.unique_access.load(Ordering::Acquire) {
            return Err(ArchetypeError::ColumnAlreadyUniquelyAccessed {
                type_hash: column.info.type_hash,
            });
        }
        Ok(Self {
            column,
            memory: column.memory,
            type_hash: column.info.type_hash,
            stride: column.info.layout.size(),
            left: size,
            unique,
        })
    }
}

impl<'a, const LOCKING: bool> Iterator for ArchetypeDynamicColumnIter<'a, LOCKING> {
    type Item = ArchetypeDynamicColumnItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = ArchetypeDynamicColumnItem {
            memory: self.memory,
            type_hash: self.type_hash,
            unique: self.unique,
            _phantom: PhantomData,
        };
        self.memory = unsafe { self.memory.add(self.stride) };
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

struct Column {
    memory: *mut u8,
    layout: Layout,
    info: ArchetypeColumnInfo,
    unique_access: Arc<AtomicBool>,
    instances: Arc<AtomicUsize>,
}

unsafe impl Send for Column {}
unsafe impl Sync for Column {}

impl Drop for Column {
    fn drop(&mut self) {
        if self.instances.fetch_sub(1, Ordering::SeqCst) <= 1 {
            while self.unique_access.load(Ordering::Acquire) {
                traced_spin_loop();
            }
            unsafe {
                non_zero_dealloc(self.memory, self.layout);
            }
        }
    }
}

impl Clone for Column {
    fn clone(&self) -> Self {
        let instances = self.instances.clone();
        instances.fetch_add(1, Ordering::SeqCst);
        Self {
            memory: self.memory,
            layout: self.layout,
            info: self.info,
            unique_access: self.unique_access.clone(),
            instances,
        }
    }
}

impl Column {
    fn new(info: ArchetypeColumnInfo, capacity: usize) -> Self {
        let (memory, layout) = unsafe { Self::allocate_memory(info.layout, capacity) };
        Self {
            memory,
            layout,
            info,
            unique_access: Default::default(),
            instances: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn validate_sdir(&self) -> Result<(), ArchetypeError> {
        if self.instances.load(Ordering::Acquire) <= 1 {
            Ok(())
        } else {
            Err(ArchetypeError::ColumnSdirLocked {
                type_hash: self.info.type_hash,
            })
        }
    }

    unsafe fn reallocate(&mut self, size: usize, capacity: usize) {
        let (memory, layout) = unsafe { Self::allocate_memory(self.info.layout, capacity) };
        unsafe { self.memory.copy_to(memory, self.info.layout.size() * size) };
        unsafe { non_zero_dealloc(self.memory, self.layout) };
        self.memory = memory;
        self.layout = layout;
    }

    unsafe fn allocate_memory(mut item_layout: Layout, capacity: usize) -> (*mut u8, Layout) {
        item_layout = item_layout.pad_to_align();
        let layout = unsafe {
            Layout::from_size_align_unchecked(item_layout.size() * capacity, item_layout.align())
        };
        let memory = unsafe { non_zero_alloc(layout) };
        (memory, layout)
    }

    unsafe fn column_access<const LOCKING: bool, T: Component>(
        &self,
        unique: bool,
        size: usize,
    ) -> Result<ArchetypeColumnAccess<LOCKING, T>, ArchetypeError> {
        unsafe { ArchetypeColumnAccess::new(self, unique, size) }
    }

    fn dynamic_column_access<const LOCKING: bool>(
        &self,
        unique: bool,
        size: usize,
    ) -> Result<ArchetypeDynamicColumnAccess<LOCKING>, ArchetypeError> {
        ArchetypeDynamicColumnAccess::new(self, unique, size)
    }

    unsafe fn entity_access<const LOCKING: bool, T: Component>(
        &self,
        unique: bool,
        index: usize,
    ) -> Result<ArchetypeEntityColumnAccess<LOCKING, T>, ArchetypeError> {
        unsafe { ArchetypeEntityColumnAccess::new(self, unique, index) }
    }

    fn dynamic_entity_access<const LOCKING: bool>(
        &self,
        unique: bool,
        index: usize,
    ) -> Result<ArchetypeDynamicEntityColumnAccess<LOCKING>, ArchetypeError> {
        ArchetypeDynamicEntityColumnAccess::new(self, unique, index)
    }

    fn column_read_iter<const LOCKING: bool, T: Component>(
        &self,
        size: usize,
    ) -> Result<ArchetypeColumnReadIter<T>, ArchetypeError> {
        ArchetypeColumnReadIter::new::<LOCKING>(self, size)
    }

    fn column_write_iter<const LOCKING: bool, T: Component>(
        &self,
        size: usize,
    ) -> Result<ArchetypeColumnWriteIter<LOCKING, T>, ArchetypeError> {
        ArchetypeColumnWriteIter::new(self, size)
    }

    fn dynamic_column_iter<const LOCKING: bool>(
        &self,
        unique: bool,
        size: usize,
    ) -> Result<ArchetypeDynamicColumnIter<LOCKING>, ArchetypeError> {
        ArchetypeDynamicColumnIter::new(self, unique, size)
    }
}

pub struct Archetype {
    columns: Box<[Column]>,
    capacity: usize,
    size: usize,
    entity_dense_map: EntityDenseMap,
}

impl Drop for Archetype {
    fn drop(&mut self) {
        let _ = self.clear::<true>();
    }
}

impl Archetype {
    pub fn new(
        columns: Vec<ArchetypeColumnInfo>,
        mut capacity: usize,
    ) -> Result<Self, ArchetypeError> {
        for (index, column) in columns.iter().enumerate() {
            let position = columns
                .iter()
                .position(|c| c.type_hash == column.type_hash)
                .unwrap();
            if position != index {
                return Err(ArchetypeError::ColumnTypeIsDuplicated {
                    type_hash: column.type_hash,
                    index,
                    duplicate_index: position,
                });
            }
        }
        capacity = capacity.next_power_of_two();
        let columns = columns
            .into_iter()
            .map(|info| Column::new(info, capacity))
            .collect::<Vec<_>>();
        // TODO: reorder to pack for minimal space gaps and compact layout.
        let columns = columns.into_boxed_slice();
        Ok(Self {
            columns,
            capacity,
            size: 0,
            entity_dense_map: EntityDenseMap::with_capacity(capacity),
        })
    }

    pub fn view<B: BundleColumns>(&self) -> Option<ArchetypeView> {
        let columns = B::columns_static()
            .into_iter()
            .filter_map(|info| {
                self.columns
                    .iter()
                    .find(|column| column.info == info)
                    .cloned()
            })
            .collect::<Vec<_>>();
        if columns.is_empty() {
            None
        } else {
            Some(ArchetypeView {
                archetype: Self {
                    columns: columns.into_boxed_slice(),
                    capacity: self.capacity,
                    size: self.size,
                    entity_dense_map: self.entity_dense_map.clone(),
                },
            })
        }
    }

    pub fn view_raw(&self, columns: &[ArchetypeColumnInfo]) -> Option<ArchetypeView> {
        let columns = columns
            .iter()
            .filter_map(|info| {
                self.columns
                    .iter()
                    .find(|column| &column.info == info)
                    .cloned()
            })
            .collect::<Vec<_>>();
        if columns.is_empty() {
            None
        } else {
            Some(ArchetypeView {
                archetype: Self {
                    columns: columns.into_boxed_slice(),
                    capacity: self.capacity,
                    size: self.size,
                    entity_dense_map: self.entity_dense_map.clone(),
                },
            })
        }
    }

    pub fn view_all(&self) -> ArchetypeView {
        ArchetypeView {
            archetype: Self {
                columns: self.columns.to_vec().into_boxed_slice(),
                capacity: self.capacity,
                size: self.size,
                entity_dense_map: self.entity_dense_map.clone(),
            },
        }
    }

    pub fn validate_sdir(&self) -> Result<(), ArchetypeError> {
        for column in self.columns.as_ref() {
            column.validate_sdir()?;
        }
        Ok(())
    }

    pub fn is_column_sdir_locked_raw(&self, type_hash: TypeHash) -> bool {
        self.columns
            .iter()
            .any(|column| column.info.type_hash == type_hash && column.validate_sdir().is_err())
    }

    pub fn is_column_sdir_locked<T: Component>(&self) -> bool {
        self.is_column_sdir_locked_raw(TypeHash::of::<T>())
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn entities(&self) -> &EntityDenseMap {
        &self.entity_dense_map
    }

    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = &ArchetypeColumnInfo> {
        self.columns.as_ref().iter().map(|column| &column.info)
    }

    pub fn has_column(&self, column: &ArchetypeColumnInfo) -> bool {
        self.columns
            .as_ref()
            .iter()
            .any(|c| column.type_hash == c.info.type_hash)
    }

    pub fn has_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        columns.iter().all(|column| {
            self.columns
                .as_ref()
                .iter()
                .any(|c| column.type_hash == c.info.type_hash)
        })
    }

    pub fn has_columns_exact(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        self.columns.as_ref().len() == columns.len() && self.has_columns(columns)
    }

    pub fn has_any_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        columns.iter().any(|column| {
            self.columns
                .as_ref()
                .iter()
                .any(|c| column.type_hash == c.info.type_hash)
        })
    }

    pub fn has_no_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        !columns.iter().any(|column| {
            self.columns
                .as_ref()
                .iter()
                .any(|c| column.type_hash == c.info.type_hash)
        })
    }

    pub fn has_type(&self, type_hash: TypeHash) -> bool {
        self.columns
            .as_ref()
            .iter()
            .any(|c| type_hash == c.info.type_hash)
    }

    pub fn has_types(&self, types: &[TypeHash]) -> bool {
        types.iter().all(|type_hash| {
            self.columns
                .as_ref()
                .iter()
                .any(|c| type_hash == &c.info.type_hash)
        })
    }

    pub fn has_types_exact(&self, types: &[TypeHash]) -> bool {
        self.columns.as_ref().len() == types.len() && self.has_types(types)
    }

    pub fn has_no_types(&self, types: &[TypeHash]) -> bool {
        !types.iter().any(|type_hash| {
            self.columns
                .as_ref()
                .iter()
                .any(|c| type_hash == &c.info.type_hash)
        })
    }

    pub fn clear<const LOCKING: bool>(&mut self) -> Result<(), ArchetypeError> {
        self.validate_sdir()?;
        let access = self
            .columns
            .as_ref()
            .iter()
            .map(|column| column.dynamic_column_access::<LOCKING>(true, self.size))
            .collect::<Result<Vec<_>, _>>()?;
        for access in access {
            for index in 0..access.size() {
                unsafe {
                    (access.info().finalizer())(access.data(index).unwrap().cast());
                }
            }
        }
        self.size = 0;
        self.entity_dense_map.clear();
        Ok(())
    }

    /// # Safety
    pub(crate) unsafe fn clear_uninitialized(&mut self) {
        // TODO: consider SDIR validation if it makes sense in where is it called.
        self.size = 0;
        self.entity_dense_map.clear();
    }

    pub fn insert(&mut self, entity: Entity, bundle: impl Bundle) -> Result<(), ArchetypeError> {
        self.validate_sdir()?;
        for info in bundle.columns() {
            if !self
                .columns
                .as_ref()
                .iter()
                .any(|column| column.info.type_hash == info.type_hash)
            {
                return Err(ArchetypeError::ColumnNotFound {
                    type_hash: info.type_hash,
                });
            }
        }
        if self.size == self.capacity {
            self.capacity *= 2;
            for column in self.columns.as_mut() {
                unsafe { column.reallocate(self.size, self.capacity) };
            }
        }
        let index = match self.entity_dense_map.insert(entity) {
            Ok(index) => index,
            Err(index) => return Err(ArchetypeError::IndexAlreadyOccupied { index }),
        };
        let access = ArchetypeEntityRowAccess::new(
            self.columns
                .as_ref()
                .iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            index,
        );
        bundle.initialize_into(&access);
        self.size += 1;
        Ok(())
    }

    pub fn add(&mut self, entity: Entity) -> Result<ArchetypeEntityRowAccess, ArchetypeError> {
        self.validate_sdir()?;
        if self.size == self.capacity {
            self.capacity *= 2;
            for column in self.columns.as_mut() {
                unsafe { column.reallocate(self.size, self.capacity) };
            }
        }
        let index = match self.entity_dense_map.insert(entity) {
            Ok(index) => index,
            Err(index) => return Err(ArchetypeError::IndexAlreadyOccupied { index }),
        };
        self.size += 1;
        Ok(ArchetypeEntityRowAccess::new(
            self.columns
                .as_ref()
                .iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            index,
        ))
    }

    pub fn remove(&mut self, entity: Entity) -> Result<(), ArchetypeError> {
        if self.size == 0 {
            return Err(ArchetypeError::EntityNotFound { entity });
        }
        self.validate_sdir()?;
        let index = self
            .entity_dense_map
            .remove(entity)
            .ok_or(ArchetypeError::EntityNotFound { entity })?;
        self.size -= 1;
        for column in self.columns.as_ref() {
            unsafe {
                let target = column.memory.add(index * column.info.layout.size());
                (column.info.finalizer)(target.cast());
                if self.size != index {
                    let source = column.memory.add(self.size * column.info.layout.size());
                    source.copy_to(target, column.info.layout.size());
                }
            }
        }
        Ok(())
    }

    /// # Safety
    pub unsafe fn remove_uninitialized(&mut self, entity: Entity) -> Result<(), ArchetypeError> {
        if self.size == 0 {
            return Err(ArchetypeError::EntityNotFound { entity });
        }
        self.validate_sdir()?;
        let index = self
            .entity_dense_map
            .remove(entity)
            .ok_or(ArchetypeError::EntityNotFound { entity })?;
        self.size -= 1;
        for column in self.columns.as_ref() {
            unsafe {
                let target = column.memory.add(index * column.info.layout.size());
                if self.size != index {
                    let source = column.memory.add(self.size * column.info.layout.size());
                    source.copy_to(target, column.info.layout.size());
                }
            }
        }
        Ok(())
    }

    pub fn transfer<'a>(
        &mut self,
        other: &'a mut Self,
        entity: Entity,
    ) -> Result<ArchetypeEntityRowAccess<'a>, ArchetypeError> {
        if self.size == 0 || !self.entity_dense_map.contains(entity) {
            return Err(ArchetypeError::EntityNotFound { entity });
        }
        if other.entity_dense_map.contains(entity) {
            return Err(ArchetypeError::EntityAlreadyOccupied { entity });
        }
        self.validate_sdir()?;
        let index_to = other.entity_dense_map.insert(entity).unwrap();
        let index_from = self.entity_dense_map.remove(entity).unwrap();
        let columns = other
            .columns
            .as_ref()
            .iter()
            .filter(|column| {
                !self
                    .columns
                    .as_ref()
                    .iter()
                    .any(|c| column.info.type_hash == c.info.type_hash)
            })
            .collect::<Vec<_>>();
        let to_initialize = ArchetypeEntityRowAccess::new(columns.into_boxed_slice(), index_to);
        let columns = self
            .columns
            .as_ref()
            .iter()
            .filter(|column| {
                !other
                    .columns
                    .as_ref()
                    .iter()
                    .any(|c| column.info.type_hash == c.info.type_hash)
            })
            .collect::<Vec<_>>();
        let to_finalize = ArchetypeEntityRowAccess::new(columns.into_boxed_slice(), index_from);
        self.size -= 1;
        other.size += 1;
        let (to_move_from, to_move_to): (Vec<_>, Vec<_>) = self
            .columns
            .as_ref()
            .iter()
            .filter_map(|column| {
                let c = other
                    .columns
                    .as_ref()
                    .iter()
                    .find(|c| column.info.type_hash == c.info.type_hash)?;
                Some((column, c))
            })
            .unzip();
        let to_move_from =
            ArchetypeEntityRowAccess::new(to_move_from.into_boxed_slice(), index_from);
        let to_move_to = ArchetypeEntityRowAccess::new(to_move_to.into_boxed_slice(), index_to);
        for (from, to) in to_move_from
            .columns
            .as_ref()
            .iter()
            .zip(to_move_to.columns.as_ref().iter())
        {
            unsafe {
                let source = from.memory.add(index_from * from.info.layout.size());
                let target = to.memory.add(index_to * to.info.layout.size());
                source.copy_to(target, from.info.layout.size());
            }
        }
        for column in to_finalize.columns.as_ref() {
            unsafe {
                let data = column.memory.add(index_from * column.info.layout.size());
                (column.info.finalizer)(data.cast());
            }
        }
        if index_from < self.size {
            for column in self.columns.as_ref().iter() {
                unsafe {
                    let source = column.memory.add(self.size * column.info.layout.size());
                    let target = column.memory.add(index_from * column.info.layout.size());
                    source.copy_to(target, column.info.layout.size());
                }
            }
        }
        Ok(to_initialize)
    }

    pub fn column<const LOCKING: bool, T: Component>(
        &self,
        unique: bool,
    ) -> Result<ArchetypeColumnAccess<LOCKING, T>, ArchetypeError> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return unsafe { column.column_access::<LOCKING, T>(unique, self.size) };
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn dynamic_column<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        unique: bool,
    ) -> Result<ArchetypeDynamicColumnAccess<LOCKING>, ArchetypeError> {
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return column.dynamic_column_access(unique, self.size);
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn entity<const LOCKING: bool, T: Component>(
        &self,
        entity: Entity,
        unique: bool,
    ) -> Result<ArchetypeEntityColumnAccess<LOCKING, T>, ArchetypeError> {
        let type_hash = TypeHash::of::<T>();
        let index = self
            .entity_dense_map
            .index_of(entity)
            .ok_or(ArchetypeError::EntityNotFound { entity })?;
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return unsafe { column.entity_access::<LOCKING, T>(unique, index) };
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn dynamic_entity<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        entity: Entity,
        unique: bool,
    ) -> Result<ArchetypeDynamicEntityColumnAccess<LOCKING>, ArchetypeError> {
        let index = self
            .entity_dense_map
            .index_of(entity)
            .ok_or(ArchetypeError::EntityNotFound { entity })?;
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return column.dynamic_entity_access(unique, index);
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn row<const LOCKING: bool>(
        &self,
        entity: Entity,
    ) -> Result<ArchetypeEntityRowAccess, ArchetypeError> {
        let index = self
            .entity_dense_map
            .index_of(entity)
            .ok_or(ArchetypeError::EntityNotFound { entity })?;
        Ok(ArchetypeEntityRowAccess::new(
            self.columns
                .as_ref()
                .iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            index,
        ))
    }

    pub fn column_read_iter<const LOCKING: bool, T: Component>(
        &self,
    ) -> Result<ArchetypeColumnReadIter<T>, ArchetypeError> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return column.column_read_iter::<LOCKING, T>(self.size);
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn column_write_iter<const LOCKING: bool, T: Component>(
        &self,
    ) -> Result<ArchetypeColumnWriteIter<LOCKING, T>, ArchetypeError> {
        let type_hash = TypeHash::of::<T>();
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return column.column_write_iter::<LOCKING, T>(self.size);
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }

    pub fn dynamic_column_iter<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        unique: bool,
    ) -> Result<ArchetypeDynamicColumnIter<LOCKING>, ArchetypeError> {
        for column in self.columns.as_ref() {
            if column.info.type_hash == type_hash {
                return column.dynamic_column_iter(unique, self.size);
            }
        }
        Err(ArchetypeError::ColumnNotFound { type_hash })
    }
}

pub struct ArchetypeView {
    archetype: Archetype,
}

impl ArchetypeView {
    pub fn view<B: BundleColumns>(&self) -> Option<ArchetypeView> {
        self.archetype.view::<B>()
    }

    pub fn archetype(&self) -> &Archetype {
        &self.archetype
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.archetype.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.archetype.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.archetype.len()
    }

    #[inline]
    pub fn entities(&self) -> &EntityDenseMap {
        self.archetype.entities()
    }

    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = &ArchetypeColumnInfo> {
        self.archetype.columns()
    }

    pub fn has_column(&self, column: &ArchetypeColumnInfo) -> bool {
        self.archetype.has_column(column)
    }

    pub fn has_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        self.archetype.has_columns(columns)
    }

    pub fn has_columns_exact(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        self.archetype.has_columns_exact(columns)
    }

    pub fn has_any_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        self.archetype.has_any_columns(columns)
    }

    pub fn has_no_columns(&self, columns: &[ArchetypeColumnInfo]) -> bool {
        self.archetype.has_no_columns(columns)
    }

    pub fn has_type(&self, type_hash: TypeHash) -> bool {
        self.archetype.has_type(type_hash)
    }

    pub fn has_types(&self, types: &[TypeHash]) -> bool {
        self.archetype.has_types(types)
    }

    pub fn has_types_exact(&self, types: &[TypeHash]) -> bool {
        self.archetype.has_types_exact(types)
    }

    pub fn has_no_types(&self, types: &[TypeHash]) -> bool {
        self.archetype.has_no_types(types)
    }

    pub fn column<const LOCKING: bool, T: Component>(
        &self,
        unique: bool,
    ) -> Result<ArchetypeColumnAccess<LOCKING, T>, ArchetypeError> {
        self.archetype.column::<LOCKING, T>(unique)
    }

    pub fn dynamic_column<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        unique: bool,
    ) -> Result<ArchetypeDynamicColumnAccess<LOCKING>, ArchetypeError> {
        self.archetype.dynamic_column::<LOCKING>(type_hash, unique)
    }

    pub fn entity<const LOCKING: bool, T: Component>(
        &self,
        entity: Entity,
        unique: bool,
    ) -> Result<ArchetypeEntityColumnAccess<LOCKING, T>, ArchetypeError> {
        self.archetype.entity::<LOCKING, T>(entity, unique)
    }

    pub fn dynamic_entity<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        entity: Entity,
        unique: bool,
    ) -> Result<ArchetypeDynamicEntityColumnAccess<LOCKING>, ArchetypeError> {
        self.archetype
            .dynamic_entity::<LOCKING>(type_hash, entity, unique)
    }

    pub fn row<const LOCKING: bool>(
        &self,
        entity: Entity,
    ) -> Result<ArchetypeEntityRowAccess, ArchetypeError> {
        self.archetype.row::<LOCKING>(entity)
    }

    pub fn column_read_iter<const LOCKING: bool, T: Component>(
        &self,
    ) -> Result<ArchetypeColumnReadIter<T>, ArchetypeError> {
        self.archetype.column_read_iter::<LOCKING, T>()
    }

    pub fn column_write_iter<const LOCKING: bool, T: Component>(
        &self,
    ) -> Result<ArchetypeColumnWriteIter<LOCKING, T>, ArchetypeError> {
        self.archetype.column_write_iter::<LOCKING, T>()
    }

    pub fn dynamic_column_iter<const LOCKING: bool>(
        &self,
        type_hash: TypeHash,
        unique: bool,
    ) -> Result<ArchetypeDynamicColumnIter<LOCKING>, ArchetypeError> {
        self.archetype
            .dynamic_column_iter::<LOCKING>(type_hash, unique)
    }
}

impl Clone for ArchetypeView {
    fn clone(&self) -> Self {
        self.archetype.view_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_archetype_changes() {
        let entity = Entity::new(0, 0).unwrap();
        let mut a = Archetype::new(vec![ArchetypeColumnInfo::new::<u8>()], 2).unwrap();
        assert!(a.is_empty());
        assert_eq!(a.capacity(), 2);
        assert!(!a.entities().contains(entity));

        a.insert(entity, (1u8,)).unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a.capacity(), 2);
        assert!(a.entities().contains(entity));

        a.remove(entity).unwrap();
        assert!(a.is_empty());
        assert_eq!(a.capacity(), 2);
        assert!(!a.entities().contains(entity));

        let access = a.add(entity).unwrap();
        unsafe { access.initialize(1u8).unwrap() };
        drop(access);
        assert_eq!(
            *a.entity::<true, u8>(entity, false).unwrap().read().unwrap(),
            1
        );

        let mut b = Archetype::new(
            vec![
                ArchetypeColumnInfo::new::<u8>(),
                ArchetypeColumnInfo::new::<u16>(),
            ],
            2,
        )
        .unwrap();
        let access = a.transfer(&mut b, entity).unwrap();
        unsafe { access.initialize(2u16).unwrap() };
        assert_eq!(access.len(), 1);
        drop(access);
        assert!(a.is_empty());
        assert_eq!(a.capacity(), 2);
        assert!(!a.entities().contains(entity));
        assert_eq!(b.len(), 1);
        assert_eq!(b.capacity(), 2);
        assert!(b.entities().contains(entity));
        assert_eq!(
            *b.entity::<true, u8>(entity, false).unwrap().read().unwrap(),
            1
        );
        assert_eq!(
            *b.entity::<true, u16>(entity, false)
                .unwrap()
                .read()
                .unwrap(),
            2
        );

        let mut c = Archetype::new(vec![ArchetypeColumnInfo::new::<u16>()], 2).unwrap();
        let access = b.transfer(&mut c, entity).unwrap();
        assert_eq!(access.len(), 0);
        drop(access);
        assert!(b.is_empty());
        assert_eq!(b.capacity(), 2);
        assert!(!b.entities().contains(entity));
        assert_eq!(c.len(), 1);
        assert_eq!(c.capacity(), 2);
        assert!(c.entities().contains(entity));
        assert_eq!(
            *c.entity::<true, u16>(entity, false)
                .unwrap()
                .read()
                .unwrap(),
            2
        );

        struct Droppable(Arc<RwLock<bool>>);

        impl Drop for Droppable {
            fn drop(&mut self) {
                *self.0.write().unwrap() = true;
            }
        }

        let mut d = Archetype::new(vec![ArchetypeColumnInfo::new::<Droppable>()], 1).unwrap();
        let dropped = Arc::new(RwLock::new(false));
        d.insert(entity, (Droppable(dropped.clone()),)).unwrap();
        assert!(!*dropped.read().unwrap());
        d.remove(entity).unwrap();
        assert!(*dropped.read().unwrap());
    }

    #[test]
    fn test_archetype_iter() {
        let mut archetype = Archetype::new(
            vec![
                ArchetypeColumnInfo::new::<u8>(),
                ArchetypeColumnInfo::new::<u16>(),
            ],
            5,
        )
        .unwrap();

        for index in 0..5 {
            archetype
                .insert(Entity::new(index, 0).unwrap(), (index as u8, index as u16))
                .unwrap();
        }

        let iter = archetype.column_read_iter::<true, u8>().unwrap();
        assert_eq!(iter.size_hint(), (5, Some(5)));
        for (index, item) in iter.enumerate() {
            assert_eq!(*item, index as u8);
        }

        let iter = archetype.column_write_iter::<true, u16>().unwrap();
        assert_eq!(iter.size_hint(), (5, Some(5)));
        for (index, item) in iter.enumerate() {
            assert_eq!(*item, index as u16);
            *item *= 10;
        }

        let iter = archetype
            .dynamic_column_iter::<true>(TypeHash::of::<u16>(), false)
            .unwrap();
        assert_eq!(iter.size_hint(), (5, Some(5)));
        for (index, item) in iter.enumerate() {
            assert_eq!(*item.read::<u16>().unwrap(), index as u16 * 10);
        }
    }
}
