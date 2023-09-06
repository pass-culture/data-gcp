SELECT
    CAST("id" AS varchar(255)) AS id,
    CAST("bookingId" AS varchar(255)) AS booking_id,
    CAST("incidentId" AS varchar(255)) AS incident_id,
    CAST("beneficiaryId" AS varchar(255)) AS beneficiary_id,
    CAST("collectiveBookingId" AS varchar(255)) AS collective_booking_id,
    CAST("newTotalAmount" AS varchar(255)) AS new_total_amount
FROM public.booking_finance_incident
