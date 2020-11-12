def anonymize_validation_token_offerer(dataset):
    """
    Table offerer : Génération d'un token de validation aléatoire pour les utilisateurs en ayant un
    """
    update_validation_token = f"""UPDATE {dataset}.offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1, 27)) 
    WHERE validationToken is not null;"""

    return update_validation_token


def anonymize_apikey(dataset):
    """
    Table provider : Génération aléatoire d'une apiKey anonymisée (si la valeur est non nulle)
    """
    update_apikey = f"""UPDATE {dataset}.provider 
    SET apiKey = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,32)) 
    WHERE apiKey is not null;"""

    return update_apikey


def anonymize_firstname(dataset):
    """
    Table user : Remplacement du prénom par firstName || id
    """
    update_firstname = f"""UPDATE {dataset}.user 
    SET  firstName = 'firstName' || id 
    WHERE firstName IS NOT NULL;"""

    return update_firstname


def anonymize_lastname(dataset):
    """
    Table user : Remplacement du nom de famille par lastName || id
    """
    update_lastname = f"""UPDATE {dataset}.user 
    SET  lastName = 'lastName' || id 
    WHERE lastName IS NOT NULL;"""

    return update_lastname


def anonymize_dateofbirth(dataset):
    """
    Table user : Attribution arbitraire de la date de naissance 01/01/2001 pour tous les utilisateurs
    """
    update_dateofbirth = f"""UPDATE {dataset}.user 
    SET  dateOfBirth = '2001-01-01T00:00:00' 
    WHERE dateOfBirth IS NOT NULL;"""

    return update_dateofbirth


def anonymize_phonenumber(dataset):
    """
    Table user : Attribution arbitraire du numéro de téléphone 0606060606 pour tous les utilisateurs
    """
    update_phonenumber = f"""UPDATE {dataset}.user 
    SET  phoneNumber = '0606060606' 
    WHERE phoneNumber IS NOT NULL;"""

    return update_phonenumber


def anonymize_email(dataset):
    """
    Table user : Remplacement de l'email par la concaténation de user@ et de son identifiant
    """
    update_email = f"""UPDATE {dataset}.user 
    SET  email = 'user@' || id 
    WHERE email IS NOT NULL;"""

    return update_email


def anonymize_publicname(dataset):
    """
    Table user : Remplacement du nom d'utilisateur (publicName) par User || id
    """
    update_publicname = f"""UPDATE {dataset}.user 
    SET  publicName = 'User' || id 
    WHERE publicName IS NOT NULL;"""

    return update_publicname


def anonymize_password(dataset):
    """
    Table user : Remplacement du mot de passe par $PASSWORD || id
    """
    update_password = f"""UPDATE {dataset}.user 
    SET  password = CAST('Password' || id AS BYTES) 
    WHERE password IS NOT NULL;"""

    return update_password


def anonymize_validation_token_user(dataset):
    """
    Table user : Génération d'un token de validation aléatoire pour les rattachements en ayant un
    """
    update_validation_token_user = f"""UPDATE {dataset}.user 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user


def anonymize_reset_password_token(dataset):
    """
    Génération d'un token de réinitialisation de mot de passe aléatoire pour les utilisateurs en ayant un
    """
    update_reset_password_token = f"""UPDATE {dataset}.user 
    SET resetPasswordToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE resetPasswordToken is not null;"""

    return update_reset_password_token


def anonymize_iban_bic(dataset):
    """
    Table bank_information : Remplacement du BIC et IBAN par une séquence de même longueur générée aléatoirement
    """
    update_iban_bic = f"""CREATE TEMPORARY FUNCTION generate_random_between(upper_limit FLOAT64, lower_limit FLOAT64) \
    RETURNS STRING 
    LANGUAGE js 
    AS ''' 
    return Math.floor(Math.random() * (upper_limit-lower_limit+1) + lower_limit) 
    '''; 
    UPDATE {dataset}.bank_information SET iban = generate_random_between(999999999,100000000) 
    WHERE iban is not null; 
    UPDATE {dataset}.bank_information SET bic = generate_random_between(999999999, 100000000) 
    WHERE bic is not null;"""

    return update_iban_bic


def anonymize_iban_payment(dataset):
    """
    Table payment : Remplacement iban par une séquence de même longueur générée aléatoirement
    """
    update_iban_payment = f"""UPDATE {dataset}.payment  
    SET iban = 'FR7630001007941234567890185' 
    WHERE iban is not null;"""

    return update_iban_payment


def anonymize_bic_payment(dataset):
    """
    Table payment : Remplacement du BIC par une séquence de même longueur générée aléatoirement
    """
    update_bic_payment = f"""UPDATE {dataset}.payment 
     SET bic = 'BDFEFR2L' WHERE bic is not null;"""

    return update_bic_payment


def anonymize_validation_token_user_offerer(dataset):
    """
    Table user_offerer : Remplacement du BIC par une séquence de même longueur générée aléatoirement
    """
    update_validation_token_user_offerer = f"""UPDATE {dataset}.user_offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user_offerer


def anonymize_token(dataset):
    """
    Table booking : Génération d'une fausse contremarque à partir de l'identifiant du booking
    """
    update_token = f"""UPDATE {dataset}.booking 
    SET token = UPPER(RIGHT(CAST(id AS STRING), 6)) 
    WHERE token is not null;"""

    return update_token


def anonymize_validation_token_venue(dataset):
    """
    Table venue : Génération aléatoire d'un token de validation
    """
    update_validation_token_venue = f"""UPDATE {dataset}.venue 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) 
    WHERE validationToken is not null;"""

    return update_validation_token_venue


def define_anonymization_query(dataset):
    return f"""
        {anonymize_validation_token_offerer(dataset=dataset)}\n
        {anonymize_apikey(dataset=dataset)}\n
        {anonymize_firstname(dataset=dataset)}\n
        {anonymize_lastname(dataset=dataset)}\n
        {anonymize_dateofbirth(dataset=dataset)}\n
        {anonymize_phonenumber(dataset=dataset)}\n
        {anonymize_email(dataset=dataset)}\n
        {anonymize_publicname(dataset=dataset)}\n
        {anonymize_password(dataset=dataset)}\n
        {anonymize_validation_token_user(dataset=dataset)}\n
        {anonymize_reset_password_token(dataset=dataset)}\n
        {anonymize_iban_bic(dataset=dataset)}\n
        {anonymize_iban_payment(dataset=dataset)}\n
        {anonymize_bic_payment(dataset=dataset)}\n
        {anonymize_validation_token_user_offerer(dataset=dataset)}\n
        {anonymize_token(dataset=dataset)}\n
        {anonymize_validation_token_venue(dataset=dataset)}\n
    """
