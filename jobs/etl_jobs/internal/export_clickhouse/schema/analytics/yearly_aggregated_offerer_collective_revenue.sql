CREATE OR REPLACE VIEW analytics.yearly_aggregated_offerer_collective_revenue ON cluster default
AS
SELECT
    date_trunc('YEAR',toDate(creation_date)) AS creation_year,
    offerer_id,
    sum(case when collective_booking_status = 'USED' or collective_booking_status ='REIMBURSED' then booking_amount else 0 end) AS revenue,
    sum(booking_amount) AS expected_revenue
FROM intermediate.collective_booking
GROUP BY
    offerer_id, creation_year
order by offerer_id, creation_year DESC
