**pricing_id**: Unique identifier for a pricing (valorisation).

**pricing_status**: Status of the pricing: VALIDATED (created), PROCESSED (included in cashflow), or INVOICED (invoice generated).

**pricing_creation_date**: Date when the pricing was created.

**pricing_value_date**: Date used for ordering the valorisation.

**pricing_amount**: Reimbursement amount in euro cents (negative = paid by Pass Culture to the offerer).

**pricing_standard_rule**: Standard reimbursement rule applied.

**pricing_custom_rule_id**: Identifier of the custom reimbursement rule if applicable.

**pricing_revenue**: Cumulative revenue of the pricing point at the time of calculation.

**pricing_point_id**: Identifier of the pricing point (venue with SIRET).

**finance_event_id**: Unique identifier for a finance event.

**finance_event_creation_date**: Date when the finance event was created.

**finance_event_value_date**: Value date of the finance event.

**finance_event_price_ordering_date**: Date used for ordering the valorisation of events. Calculated from booking usage date, stock date (for events), and venue-pricing point link date.

**finance_event_status**: Status of the finance event: PENDING (no pricing point), READY (priceable), or PRICED (valorised).

**finance_event_motive**: Motive of the finance event.

**cashflow_id**: Unique identifier for a cashflow (monetary flow).

**cashflow_creation_date**: Date when the cashflow was created.

**cashflow_status**: Status of the cashflow: PENDING (created), UNDER_REVIEW (files generated), or ACCEPTED (invoice generated).

**cashflow_amount**: Total amount of the cashflow in euro cents.

**cashflow_batch_id**: Identifier of the cashflow batch.

**cashflow_batch_cutoff**: Cutoff date - only valorisations before this date are included in the batch.

**cashflow_batch_label**: Label description of the cashflow batch.

**pricing_line_id**: Unique identifier for a pricing line.

**pricing_line_amount**: Amount of this pricing line in euro cents.

**pricing_line_category**: Category of the pricing line. Possible values: `offerer revenue` (main reimbursement to offerer), `offerer contribution` (offerer's participation), `commercial gesture` (rare special arrangements).

**pricing_log_timestamp**: Timestamp of the log entry.

**pricing_log_status_before**: Status before the change.

**pricing_log_status_after**: Status after the change.

**pricing_log_reason**: Reason for the log entry.

**invoice_id**: Unique identifier for an invoice (reimbursement receipt).

**invoice_creation_date**: Date when the invoice was created.

**invoice_reference**: Unique reference of the invoice.

**invoice_amount**: Total amount of the invoice in euros.

**booking_finance_incident_id**: Unique identifier for a booking finance incident.

**finance_incident_id**: Identifier of the parent finance incident.

**finance_incident_new_total_amount**: New total amount after incident correction.

**venue_pricing_point_link_id**: Unique identifier for the venue-pricing point link.

**pricing_point_link_beginning_date**: Start date of the venue-pricing point link.

**pricing_point_link_ending_date**: End date of the venue-pricing point link (null if still active).

**payment_id**: Unique identifier for a legacy payment (pre-2022).

**payment_author**: Author of the payment.

**payment_comment**: Comment associated with the payment.

**payment_recipient_name**: Name of the payment recipient.

**payment_amount**: Amount of the payment.

**payment_reimbursement_rule**: Reimbursement rule applied.

**payment_reimbursement_rate**: Reimbursement rate applied.

**payment_recipient_siren**: SIREN of the recipient.

**payment_transaction_end_to_end_id**: End-to-end transaction identifier.

**payment_transaction_label**: Label of the transaction.

**payment_message_id**: Identifier of the payment message.

**payment_status_id**: Unique identifier for the payment status record.

**payment_status_date**: Date of the payment status change.

**payment_status**: Status of the payment.

**payment_status_detail**: Details about the status change.

**bank_account_id**: Unique identifier for the bank account linked to reimbursements.
