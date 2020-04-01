use uclicious::traits::TryInto;
use uclicious::{FromObject, ObjectError, ObjectRef};

pub mod full;
pub mod incremental;

#[derive(Debug, Clone)]
pub enum Strategy {
    Full(full::Full),
    Incremental(incremental::Incremental)

}

impl Strategy {
    pub fn needs_reset_default(&self) -> bool {
        match self {
            Strategy::Full(_) => false,
            Strategy::Incremental(_) => false,
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
                    },
                    "incremental" => {
                        let s: incremental::Incremental = obj.try_into()?;
                        Ok(Strategy::Incremental(s))
                    },
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
