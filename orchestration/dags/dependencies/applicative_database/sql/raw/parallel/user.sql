SELECT
	CAST("id" AS varchar(255)) AS user_id
	, "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as user_creation_date
	, "departementCode" as user_department_code
	, roles[1] as user_role
	, "postalCode" as user_postal_code
	, "needsToFillCulturalSurvey" as user_needs_to_fill_cultural_survey
	, CAST("culturalSurveyId" AS varchar(255)) as user_cultural_survey_id
	, "civility" as user_civility
	, "activity" as user_activity
	, "culturalSurveyFilledDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as user_cultural_survey_filled_date
	, "address" as user_address
	, "city" as user_city
	, "lastConnectionDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as user_last_connection_date
	, "isEmailValidated" as user_is_email_validated
	, "isActive" as user_is_active
	, "hasSeenProTutorials" as user_has_seen_pro_tutorials
	, "phoneValidationStatus" AS user_phone_validation_status
	, "isEmailValidated" AS user_has_validated_email
	, CAST("notificationSubscriptions" -> \'marketing_push\' AS BOOLEAN) AS user_has_enabled_marketing_push
	, CAST("notificationSubscriptions" -> \'marketing_email\' AS BOOLEAN) AS user_has_enabled_marketing_email
	, "notificationSubscriptions" -> \'subscribed_themes\' :: text AS user_subscribed_themes
	, "user"."dateOfBirth" AS user_birth_date
	, "user"."validatedBirthDate" AS user_validated_birth_date
	, CASE
            WHEN "user"."schoolType" = \'PUBLIC_SECONDARY_SCHOOL\' THEN \'Collège public\'
            WHEN "user"."schoolType" = \'PUBLIC_HIGH_SCHOOL\' THEN \'Lycée public\'
            WHEN "user"."schoolType" = \'PRIVATE_HIGH_SCHOOL\' THEN \'Lycée privé\'
            WHEN "user"."schoolType" = \'MILITARY_HIGH_SCHOOL\' THEN \'Lycée militaire\'
            WHEN "user"."schoolType" = \'HOME_OR_REMOTE_SCHOOLING\' THEN \'À domicile (CNED, institut de santé, etc.)\'
            WHEN "user"."schoolType" = \'AGRICULTURAL_HIGH_SCHOOL\' THEN \'Lycée agricole\'
            WHEN "user"."schoolType" = \'APPRENTICE_FORMATION_CENTER\' THEN \'Centre de formation apprentis\'
            WHEN "user"."schoolType" = \'PRIVATE_SECONDARY_SCHOOL\' THEN \'Collège privé\'
            WHEN "user"."schoolType" = \'NAVAL_HIGH_SCHOOL\' THEN \'Lycée maritime\'
            ELSE "user"."schoolType" END AS user_school_type
FROM public.user
