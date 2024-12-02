---
title: Educational Deposit
description: Description of the `mrt_global__educational_deposit` table.
---

{% docs description__mrt_global__educational_deposit %}

The `mrt_global__educational_deposit` table lists all credits received by educational institutions as part of the collective component of the Culture Pass.

{% enddocs %}

Educational institutions partners in the program (`educational_institution_id`) receive an annual budget (`amount`) for each school year (`educational_year_id`). This budget is shared among all the classes in the institution to organize school trips. These budgets are provided to the pass Culture by the various ministries involved in the program (identified through a `ministry` field).

## Table description

{% docs table__mrt_global__educational_deposit %}{% enddocs %}
