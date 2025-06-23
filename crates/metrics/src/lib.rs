mod macros;

use std::sync::atomic::{AtomicU8, Ordering};

pub use metrics;
pub use metrics::{describe_counter, describe_gauge, describe_histogram};

// Use a simple enum to represent the level as a u8
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl From<metrics::Level> for Level {
    fn from(level: metrics::Level) -> Self {
        match level {
            metrics::Level::TRACE => Level::Trace,
            metrics::Level::DEBUG => Level::Debug,
            metrics::Level::INFO => Level::Info,
            metrics::Level::WARN => Level::Warn,
            metrics::Level::ERROR => Level::Error,
        }
    }
}

impl From<Level> for metrics::Level {
    fn from(level_value: Level) -> Self {
        match level_value {
            Level::Trace => metrics::Level::TRACE,
            Level::Debug => metrics::Level::DEBUG,
            Level::Info => metrics::Level::INFO,
            Level::Warn => metrics::Level::WARN,
            Level::Error => metrics::Level::ERROR,
        }
    }
}

static METRICS_LABEL_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

/// Sets the global metrics label level.
///
/// This function allows you to dynamically change the metrics level globally.
/// The level affects how metrics are filtered and processed.
///
/// # Examples
///
/// ```rust
/// use restate_metrics::{set_metrics_label_level, get_metrics_label_level};
/// use metrics::Level;
///
/// // Set the level to DEBUG
/// set_metrics_label_level(Level::DEBUG);
/// assert_eq!(get_metrics_label_level(), Level::DEBUG);
///
/// // Set the level to ERROR
/// set_metrics_label_level(Level::ERROR);
/// assert_eq!(get_metrics_label_level(), Level::ERROR);
/// ```
pub fn set_metrics_label_level(level: impl Into<Level>) {
    let level: Level = level.into();
    METRICS_LABEL_LEVEL.store(level as u8, Ordering::Relaxed);
}

#[doc(hidden)]
#[inline]
pub fn get_metrics_label_level() -> u8 {
    METRICS_LABEL_LEVEL.load(Ordering::Relaxed)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use metrics::{
        Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use crate::{Level, counter, set_metrics_label_level};

    #[derive(Default, Clone)]
    struct TestRecorder {
        registry: Arc<Mutex<HashMap<String, Key>>>,
    }

    impl TestRecorder {
        fn get_key(&self, key: &str) -> Option<Key> {
            self.registry.lock().unwrap().get(key).cloned()
        }
    }

    impl Recorder for TestRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        /// Registers a counter.
        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Counter::noop()
        }

        /// Registers a gauge.
        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Gauge::noop()
        }

        /// Registers a histogram.
        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Histogram::noop()
        }
    }

    #[test]
    fn test_metrics_unfiltered() {
        let recorder = TestRecorder::default();
        metrics::set_global_recorder(recorder.clone()).unwrap();

        let _counter = counter!("my_counter", "label" => "value");
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].key(), "label");
        assert_eq!(labels[0].value(), "value");
    }

    #[test]
    fn test_filtered_info() {
        let recorder = TestRecorder::default();
        metrics::set_global_recorder(recorder.clone()).unwrap();
        set_metrics_label_level(Level::Info);

        let _counter = counter!("my_counter", "label" => "value", "info"; info => "value2", "debug"; debug => "value3");
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key(), "label");
        assert_eq!(labels[0].value(), "value");
    }

    #[test]
    fn test_filtered_warn() {
        let recorder = TestRecorder::default();
        metrics::set_global_recorder(recorder.clone()).unwrap();
        set_metrics_label_level(Level::Warn);

        let _counter = counter!(
            "my_counter",
            "label" => "value",
            "info"; info => "value2",
            "debug"; debug => "value3",
            "warn"; warn => "value4",
            "error"; error => "value5"
        );

        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key(), "warn");
        assert_eq!(labels[0].value(), "value4");
        assert_eq!(labels[1].key(), "error");
        assert_eq!(labels[1].value(), "value5");
    }

    #[test]
    fn test_global_metrics_level() {
        use super::{get_metrics_label_level, set_metrics_label_level};

        // Test initial state
        assert_eq!(get_metrics_label_level(), Level::Info as u8);

        // Test setting different levels
        set_metrics_label_level(metrics::Level::DEBUG);
        assert_eq!(get_metrics_label_level(), Level::Debug as u8);

        set_metrics_label_level(metrics::Level::ERROR);
        assert_eq!(get_metrics_label_level(), Level::Error as u8);

        set_metrics_label_level(metrics::Level::TRACE);
        assert_eq!(get_metrics_label_level(), Level::Trace as u8);

        set_metrics_label_level(metrics::Level::WARN);
        assert_eq!(get_metrics_label_level(), Level::Warn as u8);
    }
}
