-- Cell Key Perturbation Table
-- Parameters: D=5 (max noise), js=5 (threshold: no published count between 1 and 4)
-- Reference: INSEE cell key method, Eurostat ptable package
-- Rule: perturbed count must be 0 or >= 5. Zero stays zero.
-- Zero mean: for every count, the expected perturbation over the 256 cell keys is
-- ~0, so sums of perturbed cells are unbiased. Counts 1-4 go to 5 with
-- probability count/5 and to 0 otherwise.
{% if target.name in ["local", "dev"] %}
    -- Dev-only override: data volumes are too low for the INSEE rules to yield
    -- non-zero outputs (zeros are preserved, small counts often drop to 0). Add a
    -- flat +5 on every cell so VIDOC can be exercised end-to-end.
    select count_min, count_max, cell_key_min, cell_key_max, perturbation
    from
        unnest(
            [
                struct(
                    0 as count_min,
                    999999999 as count_max,
                    0 as cell_key_min,
                    255 as cell_key_max,
                    5 as perturbation
                )
            ]
        )
{% else %}
    select count_min, count_max, cell_key_min, cell_key_max, perturbation
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

                -- count = 1: 80% -> 0, 20% -> 5
                struct(1, 1, 0, 204, -1),
                struct(1, 1, 205, 255, 4),

                -- count = 2: 60% -> 0, 40% -> 5
                struct(2, 2, 0, 153, -2),
                struct(2, 2, 154, 255, 3),

                -- count = 3: 40% -> 0, 60% -> 5
                struct(3, 3, 0, 101, -3),
                struct(3, 3, 102, 255, 2),

                -- count = 4: 20% -> 0, 80% -> 5
                struct(4, 4, 0, 50, -4),
                struct(4, 4, 51, 255, 1),

                -- count = 5: 4 is forbidden, so the only downward move is -5 (-> 0)
                struct(5, 5, 0, 25, -5),
                struct(5, 5, 26, 125, 0),
                struct(5, 5, 126, 255, 1),

                -- count = 6-10: symmetric +/- 1
                struct(6, 10, 0, 47, -1),
                struct(6, 10, 48, 207, 0),
                struct(6, 10, 208, 255, 1),

                -- count > 10: symmetric +/- 1
                struct(11, 999999999, 0, 31, -1),
                struct(11, 999999999, 32, 223, 0),
                struct(11, 999999999, 224, 255, 1)
            ]
        )
{% endif %}
