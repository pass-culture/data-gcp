with logs as (
    select
        user_pseudo_id,
        user_id,
        event_timestamp,
        event_date,
        event_name,
        firebase_screen,
        origin,
        platform,
        onboarding_user_selected_age
    from {{ ref('int_firebase__native_event') }} f
    where (
        event_name in ('SelectAge', 'HasAcceptedAllCookies', 'login', 'OnboardingStarted', 'ConsultOffer', 'BookingConfirmation', 'first_open', 'ConsultOffer', 'ContinueSetEmail', 'ContinueSetPassword', 'ContinueSetBirthday', 'ContinueCGU', 'SetEmail', 'SetPassword', 'SetBirthday')
        or firebase_screen in ('SignupForm', 'ProfilSignUp', 'SignupConfirmationEmailSent', 'OnboardingWelcome', 'OnboardingGeolocation', 'FirstTutorial', 'BeneficiaryRequestSent', 'UnderageAccountCreated', 'BeneficiaryAccountCreated', 'FirstTutorial2', 'FirstTutorial3', 'FirstTutorial4', 'HasSkippedTutorial')
    )
),

-- utilisateurs trackés par une campagne marketing --
user_accepted_tracking as (
    select
        user_pseudo_id,
        appsflyer_id,
        event_timestamp
    from {{ ref('int_firebase__native_event') }}
    where appsflyer_id is not null
    qualify row_number() over (partition by user_pseudo_id order by event_date) = 1
),

first_open as (
    select
        user_pseudo_id,
        platform,
        min(event_timestamp) as first_open_date
    from logs
    where event_name = 'first_open'
    group by 1, 2
),

-- utilisateurs ayant accepté les cookies : certains d'entre eux ne font pas l'objet dans événement "HasAcceptedAllCookies", d'où la nécessité d'ajouter d'autres événéments pour s'en assurer --
accepted_cookies as (
    select distinct user_pseudo_id
    from logs
    where (
        event_name in ('HasAcceptedAllCookies', 'login', 'OnboardingStarted', 'ConsultOffer', 'BookingConfirmation', 'ConsultOffer', 'ContinueSetEmail', 'ContinueSetPassword', 'ContinueSetBirthday', 'SetEmail', 'SetPassword', 'SetBirthday')
        or firebase_screen in ('SignupForm', 'ProfilSignUp', 'SignupConfirmationEmailSent', 'OnboardingWelcome', 'OnboardingGeolocation', 'FirstTutorial', 'BeneficiaryRequestSent', 'UnderageAccountCreated', 'BeneficiaryAccountCreated', 'FirstTutorial2', 'FirstTutorial3', 'FirstTutorial4', 'HasSkippedTutorial')
    )
),

onboarding_started as (
    select
        user_pseudo_id,
        min(event_timestamp) as onboarding_started_date
    from logs
    where (firebase_screen in ('OnboardingWelcome', 'FirstTutorial', 'OnboardingGeolocation', 'FirstTutorial2', 'FirstTutorial3', 'FirstTutorial4') or event_name in ('OnboardingStarted', 'HasSkippedTutorial'))
    group by 1
),

-- dernier âge selectionné pendant l'onboarding --
age_selected as (
    select
        user_pseudo_id,
        onboarding_user_selected_age,
        row_number() over (partition by user_pseudo_id order by event_timestamp desc) as rank_time_selected_age
    from logs
    where ((event_name = 'SelectAge' and (origin = 'onboarding' or origin is null)) or event_name = 'SignUpTooYoung')
),

signup_started as (
    select
        user_pseudo_id,
        min(event_timestamp) as signup_started_date
    from logs
    where firebase_screen in ('SignupForm', 'ProfilSignUp')
        or event_name in ('ContinueSetEmail', 'ContinueSetPassword', 'ContinueSetBirthday', 'SetEmail', 'SetPassword', 'SetBirthday')
    group by 1
),

signup_completed as (
    select
        user_pseudo_id,
        min(event_timestamp) as signup_completed_date
    from logs
    where (firebase_screen = 'SignupConfirmationEmailSent' or event_name = 'ContinueCGU')
    group by 1
),

first_login as (
    select
        user_pseudo_id,
        user_id,
        min(event_timestamp) as first_login_date
    from logs
    where event_name = 'login' and user_id is not null
    group by 1, 2
),

beneficiary_request_sent as (
    select
        user_pseudo_id,
        min(event_timestamp) as beneficiary_request_sent_date
    from logs
    where firebase_screen in ('BeneficiaryRequestSent', 'UnderageAccountCreated', 'BeneficiaryAccountCreated')
    group by 1
),

first_offer_consulted as (
    select
        user_pseudo_id,
        min(event_timestamp) as first_offer_consulted_date
    from logs
    where event_name = 'ConsultOffer'
    group by 1
)

select
    first_open.user_pseudo_id,
    first_login.user_id,
    uat.appsflyer_id,
    case when first_open.user_pseudo_id in (select * from accepted_cookies) then true else false end as has_accepted_app_cookies,
    case when uat.appsflyer_id is null then false else true end as has_accepted_tracking,
    first_deposit_type,
    first_open.platform,
    -- certains utilisateurs s'étant déjà inscrits téléchargent l'app sur un autre device et donc créent un nouveau user_pseudo_id, la query suivante permet d'identifier ceux qui se loguent pour la première fois 
    case when timestamp(u.first_deposit_creation_date) < first_open.first_open_date then false
        when timestamp(u.user_activation_date) < first_open.first_open_date then false
        else true end
        as is_first_device_connected,
    age_selected.onboarding_user_selected_age,
    first_open.first_open_date,
    onboarding_started.onboarding_started_date,
    signup_started.signup_started_date,
    signup_completed.signup_completed_date,
    first_login.first_login_date,
    beneficiary_request_sent.beneficiary_request_sent_date,
    u.first_deposit_creation_date as deposit_created_date,
    u.first_booking_date as first_booking_date
from first_open
    left join age_selected on first_open.user_pseudo_id = age_selected.user_pseudo_id and rank_time_selected_age = 1
    left join onboarding_started on first_open.user_pseudo_id = onboarding_started.user_pseudo_id
    left join signup_started on first_open.user_pseudo_id = signup_started.user_pseudo_id
    left join signup_completed on first_open.user_pseudo_id = signup_completed.user_pseudo_id
    left join first_login on first_open.user_pseudo_id = first_login.user_pseudo_id
    left join beneficiary_request_sent on first_open.user_pseudo_id = beneficiary_request_sent.user_pseudo_id
    left join {{ ref('mrt_global__user') }} u on first_login.user_id = u.user_id
    left join user_accepted_tracking uat on first_open.user_pseudo_id = uat.user_pseudo_id
