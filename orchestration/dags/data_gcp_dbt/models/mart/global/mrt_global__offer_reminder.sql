select offer_reminder_id, user_id, offer_id
from {{ ref("int_applicative__offer_reminder") }}
