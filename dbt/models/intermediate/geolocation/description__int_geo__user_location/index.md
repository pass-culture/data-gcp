**Internal table, not intended for Metabase.** It holds personal address data (`user_address_raw`, `user_latitude`, `user_longitude`) used to geolocate each user against the IRIS and QPV referentials.

Analyses should use the beneficiary marts (`mrt_global__user_beneficiary`), which expose zone attributes only (`user_iris_internal_id`, municipality / EPCI / department codes, `user_is_in_qpv`, `user_qpv_code`, `user_qpv_name`). Geographic joins go through `user_iris_internal_id` (socio-demographics, population) or the municipality / EPCI / department codes, never through the raw address or coordinates.

QPV membership is computed on the geocoded address and cannot be derived from the IRIS: the same IRIS can mix addresses inside and outside a QPV.
