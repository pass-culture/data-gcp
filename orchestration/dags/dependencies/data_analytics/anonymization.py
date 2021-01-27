def anonymize_validation_token_offerer(dataset, table_prefix):
    """offerer table: replace validation token with a random token"""
    update_validation_token = f"""UPDATE {dataset}.{table_prefix}offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1, 27)) 
    WHERE validationToken is not null;"""

    return update_validation_token


def anonymize_apikey(dataset, table_prefix):
    """provider table: replace API key with a random key"""
    update_apikey = f"""UPDATE {dataset}.{table_prefix}provider 
    SET apiKey = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,32)) 
    WHERE apiKey is not null;"""

    return update_apikey


def anonymize_firstname(dataset, table_prefix):
    """user table : replace first name with 'firstName<id>'"""
    update_firstname = f"""UPDATE {dataset}.{table_prefix}user 
    SET  firstName = 'firstName' || id 
    WHERE firstName IS NOT NULL;"""

    return update_firstname


def anonymize_lastname(dataset, table_prefix):
    """user table: replace last name with 'lastName<id>'"""
    update_lastname = f"""UPDATE {dataset}.{table_prefix}user 
    SET  lastName = 'lastName' || id 
    WHERE lastName IS NOT NULL;"""

    return update_lastname


def anonymize_dateofbirth(dataset, table_prefix):
    """user table: replace birthdate with 01/01/2001"""
    update_dateofbirth = f"""UPDATE {dataset}.{table_prefix}user 
    SET  dateOfBirth = '2001-01-01T00:00:00' 
    WHERE dateOfBirth IS NOT NULL;"""

    return update_dateofbirth


def anonymize_phonenumber(dataset, table_prefix):
    """user table: replace birthdate with 0606060606"""
    update_phonenumber = f"""UPDATE {dataset}.{table_prefix}user 
    SET  phoneNumber = '0606060606' 
    WHERE phoneNumber IS NOT NULL;"""

    return update_phonenumber


def anonymize_email(dataset, table_prefix):
    """user table: replace email with 'user@<id>'"""
    update_email = f"""UPDATE {dataset}.{table_prefix}user 
    SET  email = 'user@' || id 
    WHERE email IS NOT NULL;"""

    return update_email


def anonymize_publicname(dataset, table_prefix):
    """
    user table: replace public name with "User<id>"
    """
    update_publicname = f"""UPDATE {dataset}.{table_prefix}user 
    SET  publicName = 'User' || id 
    WHERE publicName IS NOT NULL;"""

    return update_publicname


def anonymize_password(dataset, table_prefix):
    """user table: replace password with byte representation of 'Password<id>'"""
    update_password = f"""UPDATE {dataset}.{table_prefix}user 
    SET  password = CAST('Password' || id AS BYTES) 
    WHERE password IS NOT NULL;"""

    return update_password


def anonymize_validation_token_user(dataset, table_prefix):
    """user table: replace validation token with a random token"""
    update_validation_token_user = f"""UPDATE {dataset}.{table_prefix}user 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user


def anonymize_reset_password_token(dataset, table_prefix):
    """user table: replace reset password token with a random token"""
    update_reset_password_token = f"""UPDATE {dataset}.{table_prefix}user 
    SET resetPasswordToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE resetPasswordToken is not null;"""

    return update_reset_password_token


def anonymize_iban_bic(dataset, table_prefix):
    """bank_information table: replace BIC & IBAN with random sequences"""
    update_iban_bic = f"""CREATE TEMPORARY FUNCTION generate_random_between(upper_limit FLOAT64, lower_limit FLOAT64) \
    RETURNS STRING 
    LANGUAGE js 
    AS ''' 
    return Math.floor(Math.random() * (upper_limit-lower_limit+1) + lower_limit) 
    '''; 
    UPDATE {dataset}.{table_prefix}bank_information SET iban = generate_random_between(999999999,100000000) 
    WHERE iban is not null; 
    UPDATE {dataset}.{table_prefix}bank_information SET bic = generate_random_between(999999999, 100000000) 
    WHERE bic is not null;"""

    return update_iban_bic


def anonymize_iban_payment(dataset, table_prefix):
    """payment table: replace IBAN with a FR7630001007941234567890185"""
    update_iban_payment = f"""UPDATE {dataset}.{table_prefix}payment  
    SET iban = 'FR7630001007941234567890185' 
    WHERE iban is not null;"""

    return update_iban_payment


def anonymize_bic_payment(dataset, table_prefix):
    """payment table: replace BIC with BDFEFR2L"""
    update_bic_payment = f"""UPDATE {dataset}.{table_prefix}payment 
     SET bic = 'BDFEFR2L' WHERE bic is not null;"""

    return update_bic_payment


def anonymize_validation_token_user_offerer(dataset, table_prefix):
    """user_offerer table: replace validation token with a random sequence"""
    update_validation_token_user_offerer = f"""UPDATE {dataset}.{table_prefix}user_offerer 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) 
    WHERE validationToken is not null;"""

    return update_validation_token_user_offerer


def anonymize_token(dataset, table_prefix):
    """booking table: replace token ("contremarque") with a sequence deduced from id"""
    update_token = f"""UPDATE {dataset}.{table_prefix}booking 
    SET token = UPPER(RIGHT(CAST(id AS STRING), 6)) 
    WHERE token is not null;"""

    return update_token


def anonymize_validation_token_venue(dataset, table_prefix):
    """venue table: replace validation token with random token"""
    update_validation_token_venue = f"""UPDATE {dataset}.{table_prefix}venue 
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) 
    WHERE validationToken is not null;"""

    return update_validation_token_venue


def define_anonymization_query(dataset, table_prefix):
    return f"""
        {anonymize_validation_token_offerer(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_apikey(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_firstname(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_lastname(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_dateofbirth(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_phonenumber(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_email(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_publicname(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_password(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_validation_token_user(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_reset_password_token(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_iban_bic(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_iban_payment(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_bic_payment(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_validation_token_user_offerer(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_token(dataset=dataset, table_prefix=table_prefix)}\n
        {anonymize_validation_token_venue(dataset=dataset, table_prefix=table_prefix)}\n
    """
