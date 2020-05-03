use crate::utils::time_to_chrono;
use chrono::{DateTime, Duration, Utc};
use uclicious::traits::TryInto;
use uclicious::{ObjectError, ObjectRef, Uclicious};

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Incremental {
    pub zpool: String,
    pub filter: String,
    #[ucl(default)]
    pub runs_before_reset: Option<i64>,
    #[ucl(default, map = "time_to_chrono")]
    pub duration_before_reset: Option<Duration>,
}

impl Incremental {
    pub fn check_if_needs_reset(
        &self,
        last: Option<(i64, DateTime<Utc>)>,
        now: DateTime<Utc>,
    ) -> bool {
        if last.is_none() {
            return true;
        }
        let (count, last_reset_date) = last.unwrap();
        match self.runs_before_reset {
            Some(t) if count >= t => {
                return true;
            }
            _ => {}
        }

        match self.duration_before_reset {
            Some(t) if (now - last_reset_date) >= t => {
                return true;
            }
            _ => {}
        }
        false
    }

    pub fn new(runs_before_reset: Option<i64>, duration_before_reset: Option<Duration>) -> Self {
        Incremental {
            zpool: "".to_string(),
            filter: "".to_string(),
            runs_before_reset,
            duration_before_reset,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn needs_reset_first_run() {
        let stg = Incremental::new(None, None);
        let first_run = stg.check_if_needs_reset(None, Utc::now());
        assert_eq!(first_run, true);
    }

    #[test]
    fn within_range_count() {
        let now = Utc::now();
        let stg = Incremental::new(Some(10), None);
        let needs_reset = stg.check_if_needs_reset(Some((1, now)), now);
        assert_eq!(needs_reset, false);
    }

    #[test]
    fn within_range_duration() {
        let now = Utc::now();
        let last = now - Duration::days(1);
        let max_duration = Duration::days(2);
        let stg = Incremental::new(None, Some(max_duration));
        let needs_reset = stg.check_if_needs_reset(Some((1, last)), now);
        assert_eq!(needs_reset, false);
    }

    #[test]
    fn within_range_both() {
        let now = Utc::now();
        let last = now - Duration::days(1);
        let max_duration = Duration::days(2);
        let stg = Incremental::new(Some(10), Some(max_duration));
        let needs_reset = stg.check_if_needs_reset(Some((1, last)), now);
        assert_eq!(needs_reset, false);
    }

    #[test]
    fn count_ok_duration_not_ok() {
        let now = Utc::now();
        let last = now - Duration::days(5);
        let max_duration = Duration::days(2);
        let stg = Incremental::new(Some(10), Some(max_duration));
        let needs_reset = stg.check_if_needs_reset(Some((1, last)), now);
        assert_eq!(needs_reset, true);
    }

    #[test]
    fn count_not_ok_duration_ok() {
        let now = Utc::now();
        let last = now - Duration::days(1);
        let max_duration = Duration::days(2);
        let stg = Incremental::new(Some(10), Some(max_duration));
        let needs_reset = stg.check_if_needs_reset(Some((15, last)), now);
        assert_eq!(needs_reset, true);
    }

    #[test]
    fn count_not_ok_duration_not_ok() {
        let now = Utc::now();
        let last = now - Duration::days(5);
        let max_duration = Duration::days(2);
        let stg = Incremental::new(Some(10), Some(max_duration));
        let needs_reset = stg.check_if_needs_reset(Some((15, last)), now);
        assert_eq!(needs_reset, true);
    }
}
