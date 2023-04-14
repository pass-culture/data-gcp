import google.auth
import gspread
import pandas as pd
import utils

SPREADSHEET_ID = "1rC4K-WIECJMXYTe0-rCKAiaO3QJ7oK8jXdinvaiXi5M"
EXPECTED_HEADERS = [
    "Vous êtes",
    "Exercez-vous dans un cadre professionnel ?",
    "Quel type de structure territoriale représentez-vous ?",
    "Merci de joindre un document justifiant de cette régie",
    "Êtes-vous une librairie ou un cinéma ?",
    "Votre objet principal est-il la vente au détail de livres neufs ?",
    "Possédez-vous une autorisation d'exercice délivrée par le CNC ?",
    "Possédez-vous un label du ministère de la Culture ?",
    "Quel est votre type de structure ?",
    "Quels sont vos domaines d'intervention ?",
    "Quel est le numéro de SIRET de votre structure ?",
    "Quel est le nom de votre structure ?",
    "Merci d'indiquer votre adresse e-mail.",
    "Merci de donner un exemple d'offre collective que vous proposeriez.",
    "Avez-vous déjà un compte pass Culture ?",
    "Avez-vous déjà un lieu associé à votre compte pass Culture ?",
    "Avez-vous le label Art et Essai ?",
    "Possédez-vous l'un des labels suivants ? ",
    "Possédez-vous le label LiR ou le label LR ?",
    "Votre structure bénéficie-t-elle d'un conventionnement ou d'un soutien avec les Ministères de la Culture ou de l'Education nationale ?",
    "Submitted At",
    "Token",
    "Siret",
]


def export_sheet():

    creds, _ = google.auth.default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)
    df = pd.DataFrame(worksheet.get_all_records(expected_headers=EXPECTED_HEADERS))[
        EXPECTED_HEADERS
    ]
    df.columns = [utils.clean_question(x) for x in df.columns]
    for _c in df.columns:
        df[_c] = df[_c].astype(str)
    return df
