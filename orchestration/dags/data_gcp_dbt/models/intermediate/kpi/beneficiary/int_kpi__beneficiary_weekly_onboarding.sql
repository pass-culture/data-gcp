with
    source_events as (
        select
            user_pseudo_id,
            session_id,
            event_name,
            event_timestamp,
            event_date,
            onboarding_user_selected_age,
            booking_cancellation_step
        from {{ ref("int_firebase__native_event") }}
        where
            event_date >= date_sub(current_date(), interval 36 month)
            and (
                (event_name = 'SelectAge' and onboarding_user_selected_age != 'other')
                or event_name = 'login'
                or event_name = 'ContinueSetPassword'
                or (
                    event_name = 'StepperDisplayed'
                    and booking_cancellation_step in ('Password', 'Birthday')
                )
            )
    ),

    first_age_selections as (
        select
            user_pseudo_id,
            session_id,
            min(
                case when event_name = 'SelectAge' then event_timestamp end
            ) as first_age_selected_timestamp,
            min(
                case when event_name = 'SelectAge' then onboarding_user_selected_age end
            ) as selected_age,
            countif(event_name = 'login') as total_logins,
            countif(
                event_name = 'ContinueSetPassword'
                or (
                    event_name = 'StepperDisplayed'
                    and booking_cancellation_step in ('Password', 'Birthday')
                )
            ) as total_password_set_steps
        from source_events
        group by user_pseudo_id, session_id
        having
            min(case when event_name = 'SelectAge' then event_timestamp end) is not null
            and (
                countif(event_name = 'login') = 0
                or (
                    countif(event_name = 'login') > 0
                    and countif(
                        event_name = 'ContinueSetPassword'
                        or (
                            event_name = 'StepperDisplayed'
                            and booking_cancellation_step in ('Password', 'Birthday')
                        )
                    )
                    > 0
                )
            )
    ),

    validated_users as (
        select
            first_selection.selected_age as age_at_onboarding,
            first_selection.user_pseudo_id,
            date_trunc(
                date(first_selection.first_age_selected_timestamp), week(monday)
            ) as signup_week,
            max(
                case when event.event_name = 'login' then 1 else 0 end
            ) as is_validated_within_7d
        from first_age_selections as first_selection
        left join
            source_events as event
            on first_selection.user_pseudo_id = event.user_pseudo_id
            and event.event_name = 'login'
            and date_diff(
                date(event.event_date),
                date(first_selection.first_age_selected_timestamp),
                day
            )
            <= 7
        where
            date(first_selection.first_age_selected_timestamp)
            <= date_sub(current_date(), interval 7 day)
        group by
            first_selection.selected_age,
            date_trunc(
                date(first_selection.first_age_selected_timestamp), week(monday)
            ),
            first_selection.user_pseudo_id
    )

select
    signup_week,
    age_at_onboarding,
    count(distinct user_pseudo_id) as total_onboarding_users,
    coalesce(
        count(distinct case when is_validated_within_7d = 1 then user_pseudo_id end), 0
    ) as total_validated_7d_users
from validated_users
where age_at_onboarding is not null
group by signup_week, age_at_onboarding
