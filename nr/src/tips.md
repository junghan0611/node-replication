
### GET CPU ID

extern crate core_affinity;
use std::thread;

        let core_ids = core_affinity::get_core_ids().unwrap()[0];
        println!("{}", core_ids.id);

### Persist 

