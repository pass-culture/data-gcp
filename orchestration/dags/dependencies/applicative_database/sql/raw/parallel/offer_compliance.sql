select
	CAST(id AS varchar(255)) AS offer_compliance_id,
	CAST("offerId" AS varchar(255)) AS offer_id,
	compliance_score,
	compliance_reasons,
	validation_status_prediction,
	validation_status_prediction_reason
from offer_compliance
