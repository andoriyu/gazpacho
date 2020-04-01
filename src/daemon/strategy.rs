use uclicious::traits::TryInto;
use uclicious::{FromObject, ObjectError, ObjectRef};

pub mod full;

#[derive(Debug, Clone)]
pub enum Strategy {
    FullReplication(full::FullReplication),
}

impl FromObject<ObjectRef> for Strategy {
    fn try_from(value: ObjectRef) -> Result<Self, ObjectError> {
        let ret = {
            value
                .iter()
                .map(|obj| match obj.key().unwrap().as_str() {
                    "full_replication" => {
                        let s: full::FullReplication = obj.try_into()?;
                        Ok(Strategy::FullReplication(s))
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
