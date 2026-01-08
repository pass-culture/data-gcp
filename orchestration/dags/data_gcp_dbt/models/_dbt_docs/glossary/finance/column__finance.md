{% docs column__pricing_id %} Unique identifier for a pricing (valorisation). {% enddocs %}
{% docs column__pricing_status %} Status of the pricing: VALIDATED (created), PROCESSED (included in cashflow), or INVOICED (invoice generated). {% enddocs %}
{% docs column__pricing_creation_date %} Date when the pricing was created. {% enddocs %}
{% docs column__pricing_value_date %} Date used for ordering the valorisation. {% enddocs %}
{% docs column__pricing_amount %} Reimbursement amount in euro cents (negative = paid by Pass Culture to the offerer). {% enddocs %}
{% docs column__pricing_standard_rule %} Standard reimbursement rule applied. {% enddocs %}
{% docs column__pricing_custom_rule_id %} Identifier of the custom reimbursement rule if applicable. {% enddocs %}
{% docs column__pricing_revenue %} Cumulative revenue of the pricing point at the time of calculation. {% enddocs %}
{% docs column__pricing_point_id %} Identifier of the pricing point (venue with SIRET). {% enddocs %}

{% docs column__finance_event_id %} Unique identifier for a finance event. {% enddocs %}
{% docs column__finance_event_creation_date %} Date when the finance event was created. {% enddocs %}
{% docs column__finance_event_value_date %} Value date of the finance event. {% enddocs %}
{% docs column__finance_event_price_ordering_date %} Date used for ordering the valorisation of events. Calculated from booking usage date, stock date (for events), and venue-pricing point link date. {% enddocs %}
{% docs column__finance_event_status %} Status of the finance event: PENDING (no pricing point), READY (priceable), or PRICED (valorised). {% enddocs %}
{% docs column__finance_event_motive %} Motive of the finance event. {% enddocs %}

{% docs column__cashflow_id %} Unique identifier for a cashflow (monetary flow). {% enddocs %}
{% docs column__cashflow_creation_date %} Date when the cashflow was created. {% enddocs %}
{% docs column__cashflow_status %} Status of the cashflow: PENDING (created), UNDER_REVIEW (files generated), or ACCEPTED (invoice generated). {% enddocs %}
{% docs column__cashflow_amount %} Total amount of the cashflow in euro cents. {% enddocs %}
{% docs column__cashflow_batch_id %} Identifier of the cashflow batch. {% enddocs %}

{% docs column__cashflow_batch_cutoff %} Cutoff date - only valorisations before this date are included in the batch. {% enddocs %}
{% docs column__cashflow_batch_label %} Label description of the cashflow batch. {% enddocs %}

{% docs column__pricing_line_id %} Unique identifier for a pricing line. {% enddocs %}
{% docs column__pricing_line_amount %} Amount of this pricing line in euro cents. {% enddocs %}
{% docs column__pricing_line_category %} Category of the pricing line. Possible values: `offerer revenue` (main reimbursement to offerer), `offerer contribution` (offerer's participation), `commercial gesture` (rare special arrangements). {% enddocs %}

{% docs column__pricing_log_timestamp %} Timestamp of the log entry. {% enddocs %}
{% docs column__pricing_log_status_before %} Status before the change. {% enddocs %}
{% docs column__pricing_log_status_after %} Status after the change. {% enddocs %}
{% docs column__pricing_log_reason %} Reason for the log entry. {% enddocs %}

{% docs column__invoice_id %} Unique identifier for an invoice (reimbursement receipt). {% enddocs %}
{% docs column__invoice_creation_date %} Date when the invoice was created. {% enddocs %}
{% docs column__invoice_reference %} Unique reference of the invoice. {% enddocs %}
{% docs column__invoice_amount %} Total amount of the invoice in euros. {% enddocs %}

{% docs column__booking_finance_incident_id %} Unique identifier for a booking finance incident. {% enddocs %}
{% docs column__finance_incident_id %} Identifier of the parent finance incident. {% enddocs %}
{% docs column__finance_incident_new_total_amount %} New total amount after incident correction. {% enddocs %}

{% docs column__venue_pricing_point_link_id %} Unique identifier for the venue-pricing point link. {% enddocs %}
{% docs column__pricing_point_link_beginning_date %} Start date of the venue-pricing point link. {% enddocs %}
{% docs column__pricing_point_link_ending_date %} End date of the venue-pricing point link (null if still active). {% enddocs %}

{% docs column__payment_id %} Unique identifier for a legacy payment (pre-2022). {% enddocs %}
{% docs column__payment_author %} Author of the payment. {% enddocs %}
{% docs column__payment_comment %} Comment associated with the payment. {% enddocs %}
{% docs column__payment_recipient_name %} Name of the payment recipient. {% enddocs %}
{% docs column__payment_amount %} Amount of the payment. {% enddocs %}
{% docs column__payment_reimbursement_rule %} Reimbursement rule applied. {% enddocs %}
{% docs column__payment_reimbursement_rate %} Reimbursement rate applied. {% enddocs %}
{% docs column__payment_recipient_siren %} SIREN of the recipient. {% enddocs %}
{% docs column__payment_transaction_end_to_end_id %} End-to-end transaction identifier. {% enddocs %}
{% docs column__payment_transaction_label %} Label of the transaction. {% enddocs %}
{% docs column__payment_message_id %} Identifier of the payment message. {% enddocs %}

{% docs column__payment_status_id %} Unique identifier for the payment status record. {% enddocs %}
{% docs column__payment_status_date %} Date of the payment status change. {% enddocs %}
{% docs column__payment_status %} Status of the payment. {% enddocs %}
{% docs column__payment_status_detail %} Details about the status change. {% enddocs %}

{% docs column__bank_account_id %} Unique identifier for the bank account linked to reimbursements. {% enddocs %}
