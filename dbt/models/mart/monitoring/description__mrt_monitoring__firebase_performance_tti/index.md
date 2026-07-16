The `mrt_monitoring__firebase_performance_tti` table

The `mrt_monitoring__firebase_performance_tti` table provides aggregated monitoring data on the Time To Interactive (TTI) metric collected from Firebase Performance Monitoring. This model enables tracking and analysis of application responsiveness as experienced by end users, helping to identify performance bottlenecks and trends over time.

Key features:

- Aggregates TTI metrics from Firebase for various platforms and environments.
- Enables monitoring of performance evolution and detection of regressions.
- Supports reporting and alerting on application interactivity and user experience.

## Table description

| name                                     | data_type | description                                                                                                                                               |
| ---------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| event_timestamp                          |           | The exact timestamp when the event was recorded.                                                                                                          |
| operating_system                         |           | The operating system of the user's device, such as Windows, macOS, iOS, or Android.                                                                       |
| radio_type                               |           | Type of network connection (e.g., WIFI, LTE, 5G).                                                                                                         |
| app_display_version                      |           | The version of the application where the event was recorded.                                                                                              |
| os_version                               |           | The version of the operating system on the user's device.                                                                                                 |
| carrier                                  |           | Mobile carrier or network provider.                                                                                                                       |
| device_name                              |           | Name or model of the device.                                                                                                                              |
| country                                  |           | Country of the user.                                                                                                                                      |
| home_time_to_interactive_container_in_ms |           | Time To Interactive (TTI) in milliseconds for the home container; it is the total time between the app launch and the moment when the homepage is usable. |
