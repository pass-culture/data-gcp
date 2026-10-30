# ZRR Fields Documentation

**zrr_level**: Classification of the Zone de Revitalisation Rurale (ZRR) level for the municipality. ZRR is a French government designation for rural areas eligible for economic revitalization support. Derived from the ZRR seed data based on the municipality's INSEE code.

**zrr_level_detail**: Detailed ZRR zone classification label for the municipality.

**is_in_zrr**: Whether the municipality is classified as a Zone de Revitalisation Rurale (ZRR).

**zrr_code**: ANCT code of the last ZRR classification of the municipality (zoning frozen since 2017, ended on 2024-06-30): `C` classified, `P` partially classified, `NC` not classified, plus the transitional codes `A`, `M`, `D`, `CM`, `PA`, `PM`. Published on the 2021 COG; a municipality created since then inherits the status of the municipalities it absorbed (`P` when they differed).

**zrr_label**: Label of `zrr_code`.

**zrr_detail**: Detailed ZRR classification label (ANCT `ZONAGE_ZRR`).

**frr_code**: DGCL code of the France Ruralités Revitalisation (FRR) classification of the municipality, the zoning that replaced the ZRR on 2024-07-01: `4` FRR socle, `5` FRR+, `1` FRR "bénéficiaires" (former ZRR municipality keeping the effects of the zoning until 2027-12-31), `3` La Réunion zone spéciale d'action rurale, `2` new municipality partially classified, null when not classified.
