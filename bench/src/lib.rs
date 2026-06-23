use rand::{RngExt, rng};

#[derive(Clone, Copy)]
pub struct FooDefault {
    x: f32,
    y: f32,
    z: f32,
}

impl Default for FooDefault {
    fn default() -> Self {
        let mut rng = rng();
        Self {
            x: rng.random_range(-100.0..100.0),
            y: rng.random_range(-100.0..100.0),
            z: rng.random_range(-100.0..100.0),
        }
    }
}

impl FooDefault {
    pub fn update(&mut self) {
        self.x = self.x * 1.0001 + self.y * 0.9999 - self.z * 0.5;
        self.y += self.x.sin();
    }
}

#[derive(Clone, Copy)]
pub struct FooSimd {
    pos: [f32; 4],
    vel: [f32; 4],
}

impl Default for FooSimd {
    fn default() -> Self {
        let mut rng = rng();
        Self {
            pos: [
                rng.random_range(-100.0..100.0),
                rng.random_range(-100.0..100.0),
                rng.random_range(-100.0..100.0),
                rng.random_range(-100.0..100.0),
            ],
            vel: [
                rng.random_range(-1.0..1.0),
                rng.random_range(-1.0..1.0),
                rng.random_range(-1.0..1.0),
                rng.random_range(-1.0..1.0),
            ],
        }
    }
}

impl FooSimd {
    pub fn update(&mut self) {
        for i in 0..4 {
            self.pos[i] += self.vel[i] * 0.016;
        }
    }
}

#[derive(Clone, Copy)]
pub struct FooFakeWorkload {
    value: u32,
}

impl Default for FooFakeWorkload {
    fn default() -> Self {
        let mut rng = rng();
        Self {
            value: rng.random_range(0..1000),
        }
    }
}

impl FooFakeWorkload {
    pub fn update(&mut self) {
        if self.value.is_multiple_of(3) {
            self.value = self.value.wrapping_mul(7).wrapping_add(13);
        } else {
            self.value = self.value.wrapping_add(5);
        }
    }
}

#[derive(Clone, Copy)]
pub struct FooCacheTrash {
    big: [u64; 16],
}

impl Default for FooCacheTrash {
    fn default() -> Self {
        let mut rng = rng();
        Self {
            big: std::array::from_fn(|_| rng.random()),
        }
    }
}

impl FooCacheTrash {
    pub fn update(&mut self) {
        for i in 0..16 {
            self.big[i] = self.big[i].wrapping_add(i as u64);
        }
    }
}
