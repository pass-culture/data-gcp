SELECT
    CAST("id" AS varchar(255)) AS collective_offer_request_id
    ,CAST("collectiveOfferTemplateId" AS varchar(255)) AS collective_offer_template_id
    ,CAST("educationalInstitutionId" AS varchar(255)) AS educational_institution_id
    ,CAST("educationalRedactorId" AS varchar(255)) AS educational_redactor_id
    ,"totalStudents" AS total_students
    ,"totalTeachers" AS total_teachers
    ,"comment"
    ,"requestedDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS requested_date
    ,"dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_offer_request_creation_date
FROM collective_offer_request