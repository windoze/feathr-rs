use std::time::Duration;

use regex::Regex;

use crate::Error;

pub trait ExtDuration
where
    Self: Sized,
{
    fn from_mins(minutes: u64) -> Self;
    fn from_hours(hours: u64) -> Self;
    fn from_days(days: u64) -> Self;
    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>;
}

impl ExtDuration for Duration {
    fn from_mins(minutes: u64) -> Self {
        Self::from_secs(minutes * 60)
    }

    fn from_hours(hours: u64) -> Self {
        Self::from_secs(hours * 3600)
    }

    fn from_days(days: u64) -> Self {
        Self::from_secs(days * 86400)
    }

    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>,
    {
        str_to_dur(s.as_ref())
    }
}

impl ExtDuration for chrono::Duration {
    fn from_mins(minutes: u64) -> Self {
        Self::minutes(minutes as i64)
    }

    fn from_hours(hours: u64) -> Self {
        Self::hours(hours as i64)
    }

    fn from_days(days: u64) -> Self {
        Self::days(days as i64)
    }

    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>,
    {
        Ok(chrono::Duration::from_std(str_to_dur(s.as_ref())?)
            .map_err(|_| Error::DurationError(s.as_ref().to_owned()))?)
    }
}

pub(crate) fn str_to_dur(s: &str) -> Result<Duration, Error> {
    let re = Regex::new(r"^([0-9]+)([a-z]*)$").unwrap();
    if let Some(caps) = re.captures(s.trim()) {
        let num: u64 = caps
            .get(1)
            .ok_or_else(|| Error::DurationError(s.to_owned()))?
            .as_str()
            .parse()
            .map_err(|_| Error::DurationError(s.to_owned()))?;
        let unit = caps
            .get(2)
            .ok_or_else(|| Error::DurationError(s.to_owned()))?
            .as_str();
        match unit {
            "ns" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => Ok(Duration::from_nanos(num)),
            "us" | "micro" | "micros" | "microsecond" | "microseconds" => {
                Ok(Duration::from_micros(num))
            }
            // Bare numbers are taken to be in milliseconds.
            // @see https://github.com/lightbend/config/blob/main/HOCON.md#duration-format
            "" | "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => {
                Ok(Duration::from_millis(num))
            }
            "s" | "second" | "seconds" => Ok(Duration::from_secs(num)),
            "m" | "minute" | "minutes" => Ok(Duration::from_secs(num * 60)),
            "h" | "hour" | "hours" => Ok(Duration::from_secs(num * 3600)),
            "d" | "day" | "dasys" => Ok(Duration::from_secs(num * 86400)),
            _ => Err(Error::DurationError(s.to_owned())),
        }
    } else {
        Err(Error::DurationError(s.to_owned()))
    }
}

pub(crate) fn dur_to_string(d: Duration) -> String {
    if (d.as_nanos() % 1000) != 0 {
        format!("{}ns", d.as_nanos())
    } else if (d.as_micros() % 1000) != 0 {
        format!("{}us", d.as_micros())
    } else if (d.as_millis() % 1000) != 0 {
        format!("{}ms", d.as_millis())
    } else if (d.as_secs() % 60) != 0 {
        format!("{}s", d.as_secs())
    } else if (d.as_secs() % 3600) != 0 {
        format!("{}m", d.as_secs() / 60)
    } else if (d.as_secs() % 86400) != 0 {
        format!("{}h", d.as_secs() / 3600)
    } else {
        format!("{}d", d.as_secs() / 86400)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::utils::str_to_dur;

    use super::dur_to_string;

    #[test]
    fn test_str_to_dur() {
        assert_eq!(str_to_dur("1d").unwrap(), Duration::from_secs(86400));
        assert_eq!(str_to_dur("8h").unwrap(), Duration::from_secs(8 * 3600));
        assert_eq!(str_to_dur("120m").unwrap(), Duration::from_secs(120 * 60));
        assert_eq!(str_to_dur("54s").unwrap(), Duration::from_secs(54));
        assert_eq!(str_to_dur("999ms").unwrap(), Duration::from_millis(999));
        assert_eq!(str_to_dur("666us").unwrap(), Duration::from_micros(666));
        assert_eq!(str_to_dur("333ns").unwrap(), Duration::from_nanos(333));
        assert_eq!(str_to_dur("777").unwrap(), Duration::from_millis(777));
        assert!(str_to_dur("888xyz").is_err());
        assert!(str_to_dur("xyz999").is_err());
    }

    #[test]
    fn test_dur_to_str() {
        assert_eq!(dur_to_string(Duration::from_nanos(1001)), "1001ns");
        assert_eq!(dur_to_string(Duration::from_nanos(1000)), "1us");
        assert_eq!(dur_to_string(Duration::from_nanos(1_000_000)), "1ms");
        assert_eq!(dur_to_string(Duration::from_nanos(1_000_000_000)), "1s");
        assert_eq!(dur_to_string(Duration::from_nanos(3600_000_000_000)), "1h");
        assert_eq!(dur_to_string(Duration::from_secs(59)), "59s");
        assert_eq!(dur_to_string(Duration::from_secs(60)), "1m");
        assert_eq!(dur_to_string(Duration::from_secs(7200)), "2h");
        assert_eq!(dur_to_string(Duration::from_secs(386400)), "6440m");
        assert_eq!(dur_to_string(Duration::from_secs(986400)), "274h");
        assert_eq!(dur_to_string(Duration::from_secs(86400)), "1d");
    }
}
