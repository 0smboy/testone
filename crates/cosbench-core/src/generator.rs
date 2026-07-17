//! Name/size generators. Uniform over configured ranges (COSBench c()/u() simplified).

use crate::config::ObjectSpec;
use rand::Rng;

#[derive(Debug, Clone)]
pub struct ObjectGenerator {
    spec: ObjectSpec,
}

impl ObjectGenerator {
    pub fn new(spec: ObjectSpec) -> Self {
        Self { spec }
    }

    pub fn random_container(&self, rng: &mut impl Rng) -> (u64, String) {
        let id = rng.gen_range(self.spec.containers.start..=self.spec.containers.end);
        (id, self.spec.container_name(id))
    }

    pub fn random_object(&self, rng: &mut impl Rng) -> (u64, String) {
        let id = rng.gen_range(self.spec.objects.start..=self.spec.objects.end);
        (id, self.spec.object_name(id))
    }

    pub fn sequential_object(&self, seq: u64) -> (u64, String) {
        let count = self.spec.objects.count();
        let id = self.spec.objects.start + (seq % count);
        (id, self.spec.object_name(id))
    }

    pub fn sequential_container(&self, seq: u64) -> (u64, String) {
        let count = self.spec.containers.count();
        let id = self.spec.containers.start + (seq % count);
        (id, self.spec.container_name(id))
    }

    pub fn random_size(&self, rng: &mut impl Rng) -> u64 {
        let (lo, hi) = self.spec.effective_size_bounds();
        if lo == hi {
            lo
        } else {
            rng.gen_range(lo..=hi)
        }
    }

    pub fn all_containers(&self) -> impl Iterator<Item = String> + '_ {
        (self.spec.containers.start..=self.spec.containers.end).map(|i| self.spec.container_name(i))
    }

    pub fn all_objects(&self) -> impl Iterator<Item = (String, String)> + '_ {
        let c0 = self.spec.containers.start;
        let cname = self.spec.container_name(c0);
        (self.spec.objects.start..=self.spec.objects.end).map(move |oid| {
            (cname.clone(), self.spec.object_name(oid))
        })
    }

    pub fn hash_check(&self) -> bool {
        self.spec.hash_check
    }

    pub fn spec(&self) -> &ObjectSpec {
        &self.spec
    }
}
