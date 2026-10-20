---
title: Backend Feature Logs
description: Description of the `int_pcapi__feature_log` table.
---

{% docs description__int_pcapi__feature_log %}

# Table: Backend Feature Logs

This model extracts backend API logs tagged with a `technical_origin` (origin / feature / action), logged by the backend since 2026-09-17, to monitor the error rate of backend features.

Each row is one log entry. An action is considered successful when the log level is 'INFO' (or when the explicit `success` flag is true for logs carrying it, e.g. `users.login`), and failed when logged as 'WARNING' or 'ERROR'.

The full `extra` payload is kept in `extra_json`, so fields added later by the backend can be used without changing the model.

{% enddocs %}

## Table description

{% docs table__int_pcapi__feature_log %}{% enddocs %}
