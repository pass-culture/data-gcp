SELECT
    CAST("id" AS varchar(255)) AS criterion_category_mapping_id,
    CAST("categoryId" AS varchar(255)) AS criterion_category_id,
    CAST("criterionId" AS varchar(255)) AS criterion_id
FROM criterion_category_mapping
