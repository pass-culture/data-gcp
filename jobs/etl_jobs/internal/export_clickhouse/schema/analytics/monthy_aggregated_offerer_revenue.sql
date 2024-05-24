CREATE OR REPLACE VIEW analytics.monthy_aggregated_offerer_revenue ON cluster default
AS
SELECT
    date_trunc('MONTH',toDate(creation_date)) AS creation_month,
    offerer_id,
    sum(case when booking_status = 'USED' or booking_status ='REIMBURSED' then booking_amount else 0 end) AS revenue,
    sum(booking_amount) AS forcasted_revenue
FROM intermediate.booking
GROUP BY
    offerer_id, creation_month
order by offerer_id, creation_month DESC
