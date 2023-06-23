WITH logs AS (
SELECT
    firebase_events.user_pseudo_id
    , firebase_events.user_id
    , event_timestamp
    , event_date
    , event_name
    , firebase_screen
    , platform
FROM `{{ bigquery_analytics_dataset }}`.firebase_events
WHERE (event_name IN ('HasAcceptedAllCookies','login','OnboardingStarted','ConsultOffer','BookingConfirmation','first_open','ConsultOffer','ContinueSetEmail','ContinueSetPassword','ContinueSetBirthday','SetEmail','SetPassword','SetBirthday')
OR firebase_screen IN ('SignupForm','ProfilSignUp', 'SignupConfirmationEmailSent', 'OnboardingWelcome','OnboardingGeolocation', 'FirstTutorial','BeneficiaryRequestSent','UnderageAccountCreated','BeneficiaryAccountCreated','FirstTutorial2','FirstTutorial3','FirstTutorial4','HasSkippedTutorial' )) 
),

    -- cette requête permet d'assigner les user_pseudo_id inscrits à leur user_id respectif : c'est nécessaire pour joindre enriched_user_data, car firebase_events ne donnent pas toujours ces informations (expl : une ligne pour l'événement "login" mais NULL dans la colonne user_id)
    all_users AS(
SELECT 
    DISTINCT user_pseudo_id
    ,COALESCE(u.user_id) as user_id
FROM `{{ bigquery_analytics_dataset }}`.firebase_events f
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_user_data u ON f.user_id=u.user_id
),

    -- utilisateurs trackés par une campagne marketing --
    user_accepted_tracking AS (
    SELECT 
        user_pseudo_id 
        ,appsflyer_id
        ,event_timestamp
  FROM `{{ bigquery_analytics_dataset }}`.firebase_events
  WHERE appsflyer_id is not null
  QUALIFY row_number() over (partition by user_pseudo_id order by event_date) = 1
),

    first_open AS(
SELECT
   user_pseudo_id
   ,platform
   ,MIN(event_timestamp) as first_open_date
FROM logs
WHERE event_name = 'first_open'
GROUP BY 1,2
),

    -- utilisateurs ayant accepté les cookies : certains d'entre eux ne font pas l'objet dans événement "HasAcceptedAllCookies", d'où la nécessité d'ajouter d'autres événéments pour s'en assurer --
    accepted_cookies AS(
SELECT
    DISTINCT user_pseudo_id
FROM logs
WHERE (event_name IN ('HasAcceptedAllCookies','login','OnboardingStarted','ConsultOffer','BookingConfirmation','ConsultOffer','ContinueSetEmail','ContinueSetPassword','ContinueSetBirthday','SetEmail','SetPassword','SetBirthday')
OR firebase_screen IN ('SignupForm','ProfilSignUp', 'SignupConfirmationEmailSent', 'OnboardingWelcome','OnboardingGeolocation', 'FirstTutorial','BeneficiaryRequestSent','UnderageAccountCreated','BeneficiaryAccountCreated','FirstTutorial2','FirstTutorial3','FirstTutorial4','HasSkippedTutorial' )
)),

    onboarding_started AS(
SELECT
   user_pseudo_id 
   ,MIN(event_timestamp) as onboarding_started_date
FROM logs
WHERE (firebase_screen IN ('OnboardingWelcome', 'FirstTutorial', 'OnboardingGeolocation','FirstTutorial2','FirstTutorial3','FirstTutorial4') OR event_name IN ('OnboardingStarted','HasSkippedTutorial'))
GROUP BY 1
),

    signup_started AS (
SELECT 
    user_pseudo_id
    ,MIN(event_timestamp) as signup_started_date
FROM logs
WHERE firebase_screen IN ('SignupForm','ProfilSignUp')
OR event_name IN ('ContinueSetEmail','ContinueSetPassword','ContinueSetBirthday','SetEmail','SetPassword','SetBirthday')
GROUP BY 1
),

    signup_completed AS (
SELECT 
    user_pseudo_id
    ,MIN(event_timestamp) as signup_completed_date
FROM logs
WHERE firebase_screen = 'SignupConfirmationEmailSent'
GROUP BY 1
),

    first_login AS (
SELECT 
    user_pseudo_id
    ,MIN(event_timestamp) as first_login_date
FROM logs
WHERE event_name = 'login' 
GROUP BY 1
),

    beneficiary_request_sent AS (
SELECT 
    user_pseudo_id
    ,MIN(event_timestamp) as beneficiary_request_sent_date
FROM logs
WHERE firebase_screen IN ('BeneficiaryRequestSent','UnderageAccountCreated','BeneficiaryAccountCreated')
GROUP BY 1
),

    first_offer_consulted AS (
SELECT 
    user_pseudo_id
    ,MIN(event_timestamp) as first_offer_consulted_date
FROM logs
WHERE event_name = 'ConsultOffer'
GROUP BY 1
)

SELECT 
  first_open.user_pseudo_id
  ,all_users.user_id
  ,uat.appsflyer_id
  ,CASE WHEN first_open.user_pseudo_id IN (SELECT * FROM accepted_cookies) THEN true ELSE false END AS has_accepted_app_cookies
  ,CASE WHEN uat.appsflyer_id IS NULL THEN false ELSE true END AS has_accepted_tracking
  ,user_first_deposit_type
  ,first_open.platform
-- certains utilisateurs s'étant déjà inscrits téléchargent l'app sur un autre device et donc créent un nouveau user_pseudo_id, la query suivante permet d'identifier ceux qui se loguent pour la première fois 
  ,CASE WHEN TIMESTAMP(u.user_deposit_creation_date) < first_open.first_open_date THEN false
        WHEN TIMESTAMP(u.user_activation_date) < first_open.first_open_date THEN false
        ELSE true END 
    AS is_first_device_connected
  ,first_open.first_open_date
  ,onboarding_started.onboarding_started_date
  ,signup_started.signup_started_date
  ,signup_completed.signup_completed_date
  ,first_login.first_login_date
  ,beneficiary_request_sent.beneficiary_request_sent_date
  ,u.user_deposit_creation_date as deposit_created_date
  ,u.first_booking_date as first_booking_date
  ,CASE WHEN uat.appsflyer_id is null THEN 'unknown'
        WHEN au.appsflyer_id  is not null THEN 'paid' 
        ELSE 'organic' END
        AS acquisition_origin
   ,au.media_source as paid_acquisition_media_source
   ,au.campaign as paid_acquisition_campaign
   ,au.ad as paid_acquisition_ad
   ,au.install_time as appsflyer_install_time
FROM first_open
LEFT JOIN all_users ON first_open.user_pseudo_id=all_users.user_pseudo_id
LEFT JOIN onboarding_started ON first_open.user_pseudo_id=onboarding_started.user_pseudo_id
LEFT JOIN signup_started ON first_open.user_pseudo_id=signup_started.user_pseudo_id
LEFT JOIN signup_completed ON first_open.user_pseudo_id=signup_completed.user_pseudo_id
LEFT JOIN first_login ON first_open.user_pseudo_id=first_login.user_pseudo_id
LEFT JOIN beneficiary_request_sent ON first_open.user_pseudo_id=beneficiary_request_sent.user_pseudo_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_user_data u ON all_users.user_id=u.user_id
LEFT JOIN user_accepted_tracking uat ON first_open.user_pseudo_id = uat.user_pseudo_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.appsflyer_users au ON uat.user_pseudo_id = au.firebase_id AND uat.appsflyer_id = au.appsflyer_id