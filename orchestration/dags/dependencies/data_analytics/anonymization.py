def anonymize_validation_token_offerer(dataset):
    """offerer table: replace validation token with a random token"""
    update_validation_token = f"""UPDATE {dataset}.offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1, 27)) 
    WHERE validationToken is not null;"""

    return update_validation_token


def anonymize_apikey(dataset):
    """provider table: replace API key with a random key"""
    update_apikey = f"""UPDATE {dataset}.provider 
    SET apiKey = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,32)) 
    WHERE apiKey is not null;"""

    return update_apikey


def anonymize_firstname(dataset):
    """user table : replace first name with 'firstName<id>'"""
    update_firstname = f"""UPDATE {dataset}.user 
    SET  firstName = 'firstName' || id 
    WHERE firstName IS NOT NULL;"""

    return update_firstname


def anonymize_lastname(dataset):
    """user table: replace last name with 'lastName<id>'"""
    update_lastname = f"""UPDATE {dataset}.user 
    SET  lastName = 'lastName' || id 
    WHERE lastName IS NOT NULL;"""

    return update_lastname


def anonymize_dateofbirth(dataset):
    """user table: replace birthdate with 01/01/2001"""
    update_dateofbirth = f"""UPDATE {dataset}.user 
    SET  dateOfBirth = '2001-01-01T00:00:00' 
    WHERE dateOfBirth IS NOT NULL;"""

    return update_dateofbirth


def anonymize_phonenumber(dataset):
    """user table: replace birthdate with 0606060606"""
    update_phonenumber = f"""UPDATE {dataset}.user 
    SET  phoneNumber = '0606060606' 
    WHERE phoneNumber IS NOT NULL;"""

    return update_phonenumber


def anonymize_email(dataset):
    """user table: replace email with 'user@<id>'"""
    update_email = f"""UPDATE {dataset}.user 
    SET  email = 'user@' || id 
    WHERE email IS NOT NULL;"""

    return update_email


def anonymize_publicname(dataset):
    """
    user table: replace public name with "User<id>"
    """
    update_publicname = f"""UPDATE {dataset}.user 
    SET  publicName = 'User' || id 
    WHERE publicName IS NOT NULL;"""

    return update_publicname


def anonymize_password(dataset):
    """user table: replace password with byte representation of 'Password<id>'"""
    update_password = f"""UPDATE {dataset}.user 
    SET  password = CAST('Password' || id AS BYTES) 
    WHERE password IS NOT NULL;"""

    return update_password


def anonymize_validation_token_user(dataset):
    """user table: replace validation token with a random token"""
    update_validation_token_user = f"""UPDATE {dataset}.user 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user


def anonymize_reset_password_token(dataset):
    """user table: replace reset password token with a random token"""
    update_reset_password_token = f"""UPDATE {dataset}.user 
    SET resetPasswordToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE resetPasswordToken is not null;"""

    return update_reset_password_token


def anonymize_iban_bic(dataset):
    """bank_information table: replace BIC & IBAN with random sequences"""
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
    """payment table: replace IBAN with a FR7630001007941234567890185"""
    update_iban_payment = f"""UPDATE {dataset}.payment  
    SET iban = 'FR7630001007941234567890185' 
    WHERE iban is not null;"""

    return update_iban_payment


def anonymize_bic_payment(dataset):
    """payment table: replace BIC with BDFEFR2L"""
    update_bic_payment = f"""UPDATE {dataset}.payment 
     SET bic = 'BDFEFR2L' WHERE bic is not null;"""

    return update_bic_payment


def anonymize_validation_token_user_offerer(dataset):
    """user_offerer table: replace validation token with a random sequence"""
    update_validation_token_user_offerer = f"""UPDATE {dataset}.user_offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user_offerer


def anonymize_token(dataset):
    """booking table: replace token ("contremarque") with a sequence deduced from id"""
    update_token = f"""UPDATE {dataset}.booking 
    SET token = UPPER(RIGHT(CAST(id AS STRING), 6)) 
    WHERE token is not null;"""

    return update_token


def anonymize_validation_token_venue(dataset):
    """venue table: replace validation token with random token"""
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
