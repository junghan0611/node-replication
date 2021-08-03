
### GET CPU ID

extern crate core_affinity;
use std::thread;

        let core_ids = core_affinity::get_core_ids().unwrap()[0];
        println!("{}", core_ids.id);

### Persist 

RUST_TEST_THREADS=1 cargo bench --bench stack --features nr

RUST_TEST_THREADS=1 cargo bench --bench hashmap --features nr