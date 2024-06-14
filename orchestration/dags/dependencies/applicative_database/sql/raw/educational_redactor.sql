SELECT
    CAST(id AS varchar(255)) AS educational_redactor_id
    , civility AS educational_redactor_civility
    ,ENCODE(sha256(email::bytea), \'hex\') AS hashed_user_id
FROM educational_redactor