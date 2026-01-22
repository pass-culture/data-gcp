with
    flattened_deposits as (
        select
            ei.institution_id,
            ei.institution_external_id,
            ei.institution_name,
            ei.institution_city,
            ei.institution_department_code,
            ei.institution_region_name as region_name,
            ei.institution_academy_name as institution_academie,
            ei.institution_epci,
            ei.ministry,
            ei.institution_type,
            ei.macro_institution_type,
            ei.institution_program_name,
            ei.institution_macro_density_label,
            ed.scholar_year,
            ei.total_students,
            sum(
                case
                    when ed.educational_deposit_period = 'p1'
                    then (ed.educational_deposit_amount)
                end
            ) as p1_deposit,
            sum(
                case
                    when ed.educational_deposit_period = 'p2'
                    then (ed.educational_deposit_amount)
                end
            ) as p2_deposit,
            sum(
                case
                    when ed.educational_deposit_period = 'all_year'
                    then (ed.educational_deposit_amount)
                end
            ) as all_year_deposit

        from {{ ref("mrt_global__educational_institution") }} as ei
        inner join
            {{ ref("mrt_global__educational_deposit") }} as ed using (institution_id)
        group by all
    ),

    bookings as (
        select
            ed.institution_id,
            ed.educational_deposit_id,
            ed.scholar_year,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p1'
                            and cb.collective_booking_status != 'CANCELLED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p1_theoric_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p2'
                            and cb.collective_booking_status != 'CANCELLED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p2_theoric_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'all_year'
                            and cb.collective_booking_status != 'CANCELLED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as all_year_theoric_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p1'
                            and cb.collective_booking_status
                            not in ('CANCELLED', 'PENDING')
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p1_real_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p2'
                            and cb.collective_booking_status
                            not in ('CANCELLED', 'PENDING')
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p2_real_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'all_year'
                            and cb.collective_booking_status
                            not in ('CANCELLED', 'PENDING')
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as all_year_real_amount_spent,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p1'
                            and cb.collective_booking_status = 'REIMBURSED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p1_reimbursed_amount,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'p2'
                            and cb.collective_booking_status = 'REIMBURSED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as p2_reimbursed_amount,
            coalesce(
                sum(
                    case
                        when
                            ed.educational_deposit_period = 'all_year'
                            and cb.collective_booking_status = 'REIMBURSED'
                        then cs.collective_stock_price
                    end
                ),
                0
            ) as all_year_reimbursed_amount,
            count(
                case
                    when
                        ed.educational_deposit_period = 'p1'
                        and cb.collective_booking_status != 'CANCELLED'
                    then cb.collective_booking_id
                end
            ) as p1_total_theoric_bookings,
            count(
                case
                    when
                        ed.educational_deposit_period = 'p2'
                        and cb.collective_booking_status != 'CANCELLED'
                    then cb.collective_booking_id
                end
            ) as p2_total_theoric_bookings,
            count(
                case
                    when
                        ed.educational_deposit_period = 'all_year'
                        and cb.collective_booking_status != 'CANCELLED'
                    then cb.collective_booking_id
                end
            ) as all_year_total_theoric_bookings,
            count(
                case
                    when
                        ed.educational_deposit_period = 'p1'
                        and cb.collective_booking_status not in ('CANCELLED', 'PENDING')
                    then cb.collective_booking_id
                end
            ) as p1_total_confirmed_bookings,
            count(
                case
                    when
                        ed.educational_deposit_period = 'p2'
                        and cb.collective_booking_status not in ('CANCELLED', 'PENDING')
                    then cb.collective_booking_id
                end
            ) as p2_total_confirmed_bookings,
            count(
                case
                    when
                        ed.educational_deposit_period = 'all_year'
                        and cb.collective_booking_status not in ('CANCELLED', 'PENDING')
                    then cb.collective_booking_id
                end
            ) as all_year_total_confirmed_bookings
        from {{ ref("mrt_global__educational_deposit") }} as ed
        left join
            {{ ref("int_applicative__collective_booking") }} as cb
            on ed.educational_deposit_id = cb.educational_deposit_id
        inner join
            {{ ref("int_applicative__collective_stock") }} as cs
            on cb.collective_stock_id = cs.collective_stock_id
        group by 1, 2, 3
    )

select
    flattened_deposits.*,
    bookings.p1_total_theoric_bookings,
    bookings.p2_total_theoric_bookings,
    -- theoric bookings
    bookings.p1_theoric_amount_spent,
    bookings.p2_theoric_amount_spent,
    bookings.p1_total_confirmed_bookings,
    -- theoric amount
    bookings.p2_total_confirmed_bookings,
    bookings.p1_real_amount_spent,
    bookings.p2_real_amount_spent,
    -- pct theoric amount
    bookings.p1_reimbursed_amount,
    bookings.p2_reimbursed_amount,
    coalesce(
        (
            flattened_deposits.p1_deposit is not null
            or flattened_deposits.p2_deposit is not null
        ),
        false
    ) as is_split_deposit,
    -- confirmed bookings
    coalesce(
        flattened_deposits.all_year_deposit,
        flattened_deposits.p1_deposit + flattened_deposits.p2_deposit
    ) as total_scholar_year_deposit,
    case
        when bookings.all_year_total_theoric_bookings = 0
        then bookings.p1_total_theoric_bookings + bookings.p2_total_theoric_bookings
        else bookings.all_year_total_theoric_bookings
    end as all_year_total_theoric_bookings,
    coalesce(
        case
            when bookings.all_year_theoric_amount_spent = 0
            then bookings.p1_theoric_amount_spent + bookings.p2_theoric_amount_spent
            else bookings.all_year_theoric_amount_spent
        end,
        0
    ) as all_year_theoric_amount_spent,
    -- real amount
    safe_divide(
        coalesce(
            case
                when bookings.all_year_theoric_amount_spent = 0
                then bookings.p1_theoric_amount_spent + bookings.p2_theoric_amount_spent
                else bookings.all_year_theoric_amount_spent
            end,
            0
        ),
        coalesce(
            flattened_deposits.all_year_deposit,
            flattened_deposits.p1_deposit + flattened_deposits.p2_deposit
        )
    ) as pct_all_year_theoric_amount_spent,
    coalesce(
        safe_divide(bookings.p1_theoric_amount_spent, flattened_deposits.p1_deposit), 0
    ) as pct_p1_theoric_amount_spent,
    coalesce(
        safe_divide(bookings.p2_theoric_amount_spent, flattened_deposits.p2_deposit), 0
    ) as pct_p2_theoric_amount_spent,
    -- pct real amount
    coalesce(
        case
            when bookings.all_year_total_confirmed_bookings = 0
            then
                bookings.p1_total_confirmed_bookings
                + bookings.p2_total_confirmed_bookings
            else bookings.all_year_total_confirmed_bookings
        end,
        0
    ) as all_year_total_confirmed_bookings,
    coalesce(
        case
            when bookings.all_year_real_amount_spent = 0
            then bookings.p1_real_amount_spent + bookings.p2_real_amount_spent
            else bookings.all_year_real_amount_spent
        end,
        0
    ) as all_year_real_amount_spent,
    coalesce(
        safe_divide(bookings.p1_real_amount_spent, flattened_deposits.p1_deposit), 0
    ) as pct_p1_real_amount_spent,
    -- reimbursed amount
    coalesce(
        safe_divide(bookings.p2_real_amount_spent, flattened_deposits.p2_deposit), 0
    ) as pct_p2_real_amount_spent,
    safe_divide(
        coalesce(
            case
                when bookings.all_year_real_amount_spent = 0
                then bookings.p1_real_amount_spent + bookings.p2_real_amount_spent
                else bookings.all_year_real_amount_spent
            end,
            0
        ),
        coalesce(
            flattened_deposits.all_year_deposit,
            flattened_deposits.p1_deposit + flattened_deposits.p2_deposit
        )
    ) as pct_all_year_real_amount_spent,
    coalesce(
        case
            when bookings.all_year_reimbursed_amount = 0
            then bookings.p1_reimbursed_amount + bookings.p2_reimbursed_amount
            else bookings.all_year_reimbursed_amount
        end,
        0
    ) as all_year_reimbursed_amount,
    -- pct reimbursed amount
    coalesce(
        safe_divide(bookings.p1_reimbursed_amount, flattened_deposits.p1_deposit), 0
    ) as pct_p1_reimbursed_amount_spent,
    coalesce(
        safe_divide(bookings.p2_reimbursed_amount, flattened_deposits.p2_deposit), 0
    ) as pct_p2_reimbursed_amount_spent,
    safe_divide(
        coalesce(
            case
                when bookings.all_year_reimbursed_amount = 0
                then bookings.p1_reimbursed_amount + bookings.p2_reimbursed_amount
                else bookings.all_year_reimbursed_amount
            end,
            0
        ),
        coalesce(
            flattened_deposits.all_year_deposit,
            flattened_deposits.p1_deposit + flattened_deposits.p2_deposit,
            0
        )
    ) as pct_all_year_reimbursed_amount_spent

from flattened_deposits
inner join
    bookings
    on flattened_deposits.institution_id = bookings.institution_id
    and flattened_deposits.scholar_year = bookings.scholar_year
