SELECT
    "gtlType" as gtl_type,
    cast("gtlId" as string) as gtl_id,
    "gtlLabelLevel1" as gtl_label_level_1,
    "gtlLabelLevel2" as gtl_label_level_2,
    "gtlLabelLevel3" as gtl_label_level_3,
    "gtlLabelLevel4" as gtl_label_level_4
 FROM public.titelive_gtl_mapping