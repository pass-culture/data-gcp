# Overview

This table maps backoffice log messages to human-readable action names. It is sourced from a Google Sheet manually maintained by the internal teams, and is joined to `int_pcapi__backoffice_log` to name the backoffice actions.

## Key Concepts

### Log Extract

The normalized log message (URL path without ids, or analytics event name). When the sheet holds several rows for the same log extract, a single one is kept, preferring a row with a filled log name.
