-- Cell Key Perturbation Table for Cultural Partners
-- Parameters: D=3 (max noise), js=3 (threshold: no published count between 1 and 2)
-- Reference: INSEE cell key method, Eurostat ptable package
-- Rule: perturbed count must be 0 or >= 3. Zero stays zero.
{% if target.name in ["local", "dev"] %}
    -- Dev-only override: data volumes are too low for the INSEE rules to yield
    -- non-zero outputs (zeros are preserved, small counts often drop to 0). Add a
    -- flat +3 on every cell so VIDOC can be exercised end-to-end.
    select *
    from
        unnest(
            [
                struct(
                    0 as count_min,
                    999999999 as count_max,
                    0 as cell_key_min,
                    255 as cell_key_max,
                    3 as perturbation
                )
            ]
        )
{% else %}
    select *
    from
        unnest(
            [
                -- count = 0: never perturbed (zero preservation)
                struct(
                    0 as count_min,
                    0 as count_max,
                    0 as cell_key_min,
                    255 as cell_key_max,
                    0 as perturbation
                ),

                -- count = 1: must go to 0 or >= 3 → perturbation in {-1, +2, +3}
                struct(1, 1, 0, 99, -1),
                struct(1, 1, 100, 199, 2),
                struct(1, 1, 200, 255, 3),

                -- count = 2: must go to 0 or >= 3 → perturbation in {-2, +1, +2}
                struct(2, 2, 0, 99, -2),
                struct(2, 2, 100, 199, 1),
                struct(2, 2, 200, 255, 2),

                -- count = 3: already safe (>= 3), must stay 0 or >= 3 → perturbation
                -- in {-3, 0, +1}
                struct(3, 3, 0, 79, -3),
                struct(3, 3, 80, 179, 0),
                struct(3, 3, 180, 255, 1),

                -- count = 4: safe, must stay 0 or >= 3 → perturbation in {-1, 0, +1}
                struct(4, 4, 0, 69, -1),
                struct(4, 4, 70, 189, 0),
                struct(4, 4, 190, 255, 1),

                -- count = 5-10: safe, mild perturbation → {-2, -1, 0, +1}
                struct(5, 10, 0, 39, -2),
                struct(5, 10, 40, 99, -1),
                struct(5, 10, 100, 199, 0),
                struct(5, 10, 200, 255, 1),

                -- count = 11-20: moderate perturbation
                struct(11, 20, 0, 49, -1),
                struct(11, 20, 50, 199, 0),
                struct(11, 20, 200, 255, 1),

                -- count > 20: rare perturbation
                struct(21, 999999999, 0, 31, -1),
                struct(21, 999999999, 32, 223, 0),
                struct(21, 999999999, 224, 255, 1)
            ]
        )
{% endif %}
