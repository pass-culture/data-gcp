{% docs column__pricing_week %} Week date {% enddocs %}
{% docs column__pricing_day %}  pricing value date {% enddocs %}
{% docs column__total_pricing %} Sum of all pricings (exit cashflow) on the correponding week. The value is in euros and positive.{% enddocs %}
{% docs column__total_available_credits %} Sum of `deposit_amount` across every individual pass Culture deposit active on the corresponding day/week (see `int_global__daily_deposit`). Tracks the total granted credit mass outstanding over time, including the March 2025 individual-credit reform. Used as an exogenous Prophet regressor. {% enddocs %}
