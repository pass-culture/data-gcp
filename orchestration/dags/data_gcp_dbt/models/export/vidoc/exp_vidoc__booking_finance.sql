with
    source as (
        select
            partition_month,
            venue_region_name,
            venue_department_name,
            venue_department_code,
            offer_category_id,
            sum(total_bookings) as total_bookings,
            sum(total_quantities) as total_quantities,
            sum(total_revenue_amount) as total_revenue_amount,
            sum(total_reimbursed_amount) as total_reimbursed_amount,
            sum(total_contribution_amount) as total_contribution_amount,
            mod(abs(sum(cell_key_bookings)), 256) as cell_key_bookings
        from {{ ref("metrics_booking__finance_individual") }}
        group by
            partition_month,
            venue_region_name,
            venue_department_name,
            venue_department_code,
            offer_category_id
    )

select
    s.partition_month,
    s.venue_region_name,
    s.venue_department_name,
    s.venue_department_code,
    s.offer_category_id,
    {{ apply_perturbation("s.total_bookings", "total_bookings", "pt_bookings") }},
    {{ apply_perturbation("s.total_quantities", "total_quantities", "pt_quantities") }},
    {{
        apply_perturbation(
            "s.total_revenue_amount", "total_revenue_amount", "pt_revenue"
        )
    }},
    {{
        apply_perturbation(
            "s.total_reimbursed_amount", "total_reimbursed_amount", "pt_reimbursed"
        )
    }},
    {{
        apply_perturbation(
            "s.total_contribution_amount",
            "total_contribution_amount",
            "pt_contribution",
        )
    }}
from
    source as s
    {{
        perturbation_join(
            "pt_bookings",
            "s.total_bookings",
            "s.cell_key_bookings",
            "perturbation_table__cultural_partners",
        )
    }}
    {{
        perturbation_join(
            "pt_quantities",
            "s.total_quantities",
            "s.cell_key_bookings",
            "perturbation_table__cultural_partners",
        )
    }}
    {{
        perturbation_join(
            "pt_revenue",
            "s.total_revenue_amount",
            "s.cell_key_bookings",
            "perturbation_table__cultural_partners",
        )
    }}
    {{
        perturbation_join(
            "pt_reimbursed",
            "s.total_reimbursed_amount",
            "s.cell_key_bookings",
            "perturbation_table__cultural_partners",
        )
    }}
    {{
        perturbation_join(
            "pt_contribution",
            "s.total_contribution_amount",
            "s.cell_key_bookings",
            "perturbation_table__cultural_partners",
        )
    }}
