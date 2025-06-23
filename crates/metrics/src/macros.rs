#[doc(hidden)]
#[macro_export]
macro_rules! label {
    (@filter $key:expr => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Info as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; trace => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Trace as u8 >= level  {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; debug => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Debug as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; info => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Info as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; warn => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Warn as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; error => $value:tt) => {{
        let level = $crate::get_metrics_label_level();
        if $crate::Level::Error as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};

    (@label $key:literal => $value:literal) => {{
        $crate::metrics::Label::from_static_parts($key, $value)
    }};

    (@label $key:expr => $value:block) => {{
        $crate::metrics::Label::new($key, $value)
    }};

    (@label $key:expr => $value:expr) => {{
        $crate::metrics::Label::new($key, $value)
    }};
}

#[macro_export]
macro_rules! histogram {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::histogram!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_histogram(&metric_key, metadata))
    }};
}

#[macro_export]
macro_rules! counter {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::counter!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_counter(&metric_key, metadata))
    }};
}

#[macro_export]
macro_rules! gauge {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::gauge!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_gauge(&metric_key, metadata))
    }};
}
