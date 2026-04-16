-- Cell Key Perturbation Table
-- Parameters: D=5 (max noise), js=5 (threshold: no published count between 1 and 4)
-- Reference: INSEE cell key method, Eurostat ptable package
-- Rule: perturbed count must be 0 or >= 5. Zero stays zero.
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

            -- count = 1: must go to 0 or >= 5 → perturbation in {-1, +4, +5}
            struct(1, 1, 0, 99, -1),
            struct(1, 1, 100, 199, 4),
            struct(1, 1, 200, 255, 5),

            -- count = 2: must go to 0 or >= 5 → perturbation in {-2, +3, +4}
            struct(2, 2, 0, 99, -2),
            struct(2, 2, 100, 199, 3),
            struct(2, 2, 200, 255, 4),

            -- count = 3: must go to 0 or >= 5 → perturbation in {-3, +2, +3}
            struct(3, 3, 0, 99, -3),
            struct(3, 3, 100, 199, 2),
            struct(3, 3, 200, 255, 3),

            -- count = 4: must go to 0 or >= 5 → perturbation in {-4, +1, +2}
            struct(4, 4, 0, 99, -4),
            struct(4, 4, 100, 199, 1),
            struct(4, 4, 200, 255, 2),

            -- count = 5-10: can stay >= 5 → perturbation in {-5→0, -1, 0, +1, +2}
            struct(5, 5, 0, 49, -5),
            struct(5, 5, 50, 99, 0),
            struct(5, 5, 100, 179, 1),
            struct(5, 5, 180, 255, 2),

            struct(6, 10, 0, 39, -1),
            struct(6, 10, 40, 159, 0),
            struct(6, 10, 160, 219, 1),
            struct(6, 10, 220, 255, 2),

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
