use chrono::Duration;
use uclicious::traits::TryInto;
use uclicious::{FromObject, ObjectError, ObjectRef, Uclicious};

pub mod full;
pub mod incremental;

#[derive(Debug, Clone)]
pub enum Strategy {
    Full(full::Full),
    Incremental(incremental::Incremental),
}

impl Strategy {
    pub fn needs_reset_default(&self) -> bool {
        match self {
            Strategy::Full(_) => false,
            Strategy::Incremental(_) => false,
        }
    }
}

impl Strategy {
    pub fn get_zpool_and_filter(&self) -> (String, String) {
        match self {
            Strategy::Full(stg) => (stg.zpool.clone(), stg.filter.clone()),
            Strategy::Incremental(stg) => (stg.zpool.clone(), stg.filter.clone()),
        }
    }
}

impl FromObject<ObjectRef> for Strategy {
    // There is unwrap in it, but that's okay because nested keys always have key name.
    fn try_from(value: ObjectRef) -> Result<Self, ObjectError> {
        let ret = {
            value
                .iter()
                .map(|obj| match obj.key().unwrap().as_str() {
                    "full" => {
                        let s: full::Full = obj.try_into()?;
                        Ok(Strategy::Full(s))
                    }
                    "incremental" => {
                        let s: incremental::Incremental = obj.try_into()?;
                        Ok(Strategy::Incremental(s))
                    }
                    stg => Err(ObjectError::Other(format!(
                        "Strategy \"{}\" is not supported.",
                        stg
                    ))),
                })
                .next()
        };
        ret.unwrap_or_else(|| {
            Err(ObjectError::Other(
                "Please define strategy to use".to_string(),
            ))
        })
    }
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Cleanup {
    #[ucl(
        default,
        path = "destination.age",
        map = "crate::utils::time_to_chrono"
    )]
    destination_age: Option<Duration>,
    #[ucl(default, path = "destination.count")]
    destination_count: Option<u64>,
    #[ucl(default, path = "local.age", map = "crate::utils::time_to_chrono")]
    local_age: Option<Duration>,
    #[ucl(default, path = "local.count")]
    local_count: Option<u64>,
    #[ucl(default = "false")]
    replace_with_bookmark: bool,
    #[ucl(default = "false")]
    run_every_time: bool,
}
