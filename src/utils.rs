use chrono::Duration;
use uclicious::{ObjectError, ObjectRef, TryInto};

pub fn time_to_chrono(src: ObjectRef) -> Result<Option<Duration>, ObjectError> {
    let std_time: std::time::Duration = src.try_into()?;
    Duration::from_std(std_time)
        .map(Option::from)
        .map_err(|e| ObjectError::other(e))
}
