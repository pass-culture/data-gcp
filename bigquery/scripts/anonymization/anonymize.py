import logging
import sys

from google.cloud import bigquery
from bigquery.utils import run_query
from set_env import set_env_vars

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main():
    # Table offerer : Génération d'un token de validation aléatoire pour les utilisateurs en ayant un
    anonymize_validation_token_offerer = f"UPDATE poc_scalingo.public_offerer \
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1, 27)) \
    WHERE validationToken is not null;"

    # Table Provider : Génération aléatoire d'une apiKey anonymisée (si la valeur est non nulle)
    anonymize_apikey = f"UPDATE poc_data_federated_query.provider \
    SET apiKey = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,32)) \
    WHERE apiKey is not null;"

    # Table user :
    # Remplacement du prénom par firstName || id
    anonymize_firstname = f"UPDATE poc_data_federated_query.user SET  firstName = 'firstName' || id \
    WHERE firstName IS NOT NULL;"

    # Remplacement du nom de famille par lastName || id
    anonymize_lastname = f"UPDATE poc_data_federated_query.user SET  lastName = 'lastName' || id \
    WHERE lastName IS NOT NULL;"

    # Attribution arbitraire de la date de naissance 01/01/2001 pour tous les utilisateurs
    anonymize_dateofbirth = f"UPDATE poc_data_federated_query.user SET  dateOfBirth = '2001-01-01T00:00:00' \
    WHERE dateOfBirth IS NOT NULL;"

    # Attribution arbitraire du numéro de téléphone 0606060606 pour tous les utilisateurs
    anonymize_phonenumber = f"UPDATE poc_data_federated_query.user SET  phoneNumber = '0606060606' \
    WHERE phoneNumber IS NOT NULL;"

    # Remplacement de l'email par la concaténation de user@ et de son identifiant
    anonymize_email = f"UPDATE poc_data_federated_query.user SET  email = 'user@' || id \
    WHERE email IS NOT NULL;"

    # Remplacement du nom d'utilisateur (publicName) par User || id,
    anonymize_publicname = f"UPDATE poc_data_federated_query.user SET  publicName = 'User' || id \
    WHERE publicName IS NOT NULL;"

    # Remplacement du mot de passe par $PASSWORD || id
    anonymize_password = f"UPDATE poc_data_federated_query.user SET  password = CAST('Password' || id AS BYTES) \
    WHERE password IS NOT NULL;"

    # Génération d'un token de validation aléatoire pour les rattachements en ayant un
    anonymize_validation_token_user = f"UPDATE poc_data_federated_query.user \
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) \
    WHERE validationToken is not null;"

    # Génération d'un token de réinitialisation de mot de passe aléatoire pour les utilisateurs en ayant un
    anonymize_reset_password_token = f"UPDATE poc_data_federated_query.user \
    SET resetPasswordToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) \
    WHERE resetPasswordToken is not null;"

    # Table bank_information : Remplacement du BIC et IBAN par une séquence de même longueur générée aléatoirement
    generate_random_between = f"CREATE TEMPORARY FUNCTION generate_random_between(upper_limit FLOAT64, lower_limit FLOAT64) \
    RETURNS STRING \
    LANGUAGE js \
    AS ''' \
    return Math.floor(Math.random() * (upper_limit-lower_limit+1) + lower_limit) \
    '''; \
    UPDATE poc_scalingo.public_bank_information SET iban = generate_random_between(999999999,100000000) \
    WHERE iban is not null; \
    UPDATE poc_scalingo.public_bank_information SET bic = generate_random_between(999999999, 100000000) \
    WHERE bic is not null;"

    # Table payment : Remplacement iban par une séquence de même longueur générée aléatoirement
    anonymize_iban_payment = f"UPDATE poc_scalingo.public_payment  \
    SET iban = 'FR7630001007941234567890185' WHERE iban is not null;"

    # Table payment : Remplacement du BIC par une séquence de même longueur générée aléatoirement
    anonymize_bic_payment = f"UPDATE poc_scalingo.public_payment \
    SET bic = 'BDFEFR2L' WHERE bic is not null;"

    # Table user_offerer : Remplacement du BIC par une séquence de même longueur générée aléatoirement
    anonymize_validation_token_user_offerer = f"UPDATE poc_data_federated_query.user_offerer \
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,10)) \
    WHERE validationToken is not null;"

    # Table booking : Génération d'une fausse contremarque à partir de l'identifiant du booking
    anonymize_token = f"UPDATE poc_data_federated_query.booking \
    SET token = UPPER(RIGHT(CAST(id AS STRING), 6)) \
    WHERE token is not null;"

    # Table venue : Génération aléatoire d'un token de validation
    anonymize_validation_token_venue = f"UPDATE poc_data_federated_query.venue \
    SET validationToken = (SUBSTR(TO_HEX(MD5(CAST(RAND() AS STRING))), 1,27)) \
    WHERE validationToken is not null;"

    # run query
    client = bigquery.Client()

    run_query(bq_client=client, query=anonymize_validation_token_offerer)
    run_query(bq_client=client, query=anonymize_apikey)
    run_query(bq_client=client, query=anonymize_firstname)
    run_query(bq_client=client, query=anonymize_lastname)
    run_query(bq_client=client, query=anonymize_dateofbirth)
    run_query(bq_client=client, query=anonymize_phonenumber)
    run_query(bq_client=client, query=anonymize_email)
    run_query(bq_client=client, query=anonymize_publicname)
    run_query(bq_client=client, query=anonymize_password)
    run_query(bq_client=client, query=anonymize_validation_token_user)
    run_query(bq_client=client, query=anonymize_reset_password_token)
    run_query(bq_client=client, query=generate_random_between)
    run_query(bq_client=client, query=anonymize_iban_payment)
    run_query(bq_client=client, query=anonymize_bic_payment)
    run_query(bq_client=client, query=anonymize_validation_token_user_offerer)
    run_query(bq_client=client, query=anonymize_token)
    run_query(bq_client=client, query=anonymize_validation_token_venue)


if __name__ == "__main__":
    set_env_vars()
    main()
