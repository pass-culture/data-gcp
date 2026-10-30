**firebase_performance_event_timestamp**: Timestamp of the Firebase Performance trace. Partition column (daily); filter on a date range.

**firebase_performance_event_type**: Type of the Firebase Performance event. Always 'TRACE_METRIC': the model keeps only trace metrics.

**firebase_performance_event_name**: Name of the Firebase Performance trace metric. Always 'home_time_to_interactive_in_ms': the model keeps only this metric.

**firebase_performance_operating_system**: Operating system of the device, set from the source export. Stored values: 'IOS', 'ANDROID'.

**firebase_performance_app_display_version**: Version of the app that sent the trace.

**firebase_performance_home_time_to_interactive_container_in_ms**: Home time to interactive in milliseconds: delay before the Home screen is usable, from the Firebase Performance trace 'home_time_to_interactive_in_ms' (TRACE_METRIC), iOS and Android.
