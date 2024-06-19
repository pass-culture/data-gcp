import json

import requests
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


elements = [
    {
        "venue_type_label": "Offre numérique",
        "offerer_name": "LE GROUPE THEATRAL",
        "offer_name": "Fin de partie",
        "offer_description": "Couronné du Prix Laurent Terzieff 2023 en qualité de « Meilleur spectacle dans un théâtre privé », Fin de partie revient pour 30 représentations exceptionnelles.\n\nAprès plusieurs monologues beckettiens en compagnie de Denis Lavant, Jacques Osinski fait un nouveau pari, excitant et effrayant : Fin de partie, la grande pièce de Beckett, sa préférée. Tout à coup, il faut voir les choses en grand. Sommes-nous sur terre ? Sommes-nous sur l’Arche de Noé après la fin du monde ? Peut-être est-ce déjà le purgatoire …",
    },
    {
        "venue_type_label": "Offre numérique",
        "offerer_name": "MAISON POPULAIRE LA CULTURE ET LOISIRS",
        "offer_name": "Dom La Nena",
        "offer_description": "Dom La Nena est une compositrice, violoncelliste et chanteuse née au Brésil à Porto Alegre. Après avoir étudié le violoncelle classique, Dom a accompagné divers artistes en tournée, dont Jane Birkin, Jeanne Moreau, Etienne Daho et Rosemary Standley.\nAprès l’album Tempo en 2021, acclamé par la critique, Dom La Nena revient avec un nouvel album instrumental intitulé Leon. Leon est une œuvre intime, qui évoque une atmosphère obsédante, une déclaration d’amour à son complice de toujours, son violoncelle, et un retour aux sources d’une grande sensibilité, l’histoire en somme d’un bouleversement originel : la découverte de la musique classique. Après une première épiphanie nommée Vivaldi, Dom La Nena s’épanouit aussi en compagnie des œuvres romantiques de Chopin ou du minimalisme de Philip Glass qui semblent lui montrer la voie.",
    },
    {
        "venue_type_label": "Offre numérique",
        "offerer_name": "MAISON DES LOISIRS DE ROUEN",
        "offer_name": "SCH à Rouen",
        "offer_description": "SCH, le rappeur iconique de Marseille Bébé vient visiter le nord pour réchauffer nos coeurs en nous présentant son dernier album JVLIVS Prequel.",
    },
]
st.write(elements)


url = "http://127.0.0.1:8000/latest/model/offer_categorisation/scoring"


with st.form(key="my_form"):
    columns = st.columns(4)
    with columns[0]:
        st_venue = st.text_input("Venue Type Label", value="")

    with columns[1]:
        st_offerer_name = st.text_input("Offerer Name", value="")

    with columns[2]:
        st_offer_name = st.text_input("Offer Name", value="")

    with columns[3]:
        st_offer_description = st.text_input("Offer Description", value="")
    st_form_button = st.form_submit_button("Submit")


st.header("Results")

if st_form_button:
    payload = {
        "venue_type_label": st_venue.replace('"', ""),
        "offerer_name": st_offerer_name.replace('"', ""),
        "offer_name": st_offer_name.replace('"', ""),
        "offer_description": st_offer_description.replace('"', ""),
    }
    headers = {"Content-Type": "application/json"}
    resuts = requests.post(url, data=json.dumps(payload), headers=headers).json()

    st.write(resuts)

    columns = st.columns(3)

    with columns[0]:
        st.write(resuts["most_probable_subcategories"][0]["subcategory"])
        st.progress(resuts["most_probable_subcategories"][0]["probability"])

    with columns[1]:
        st.write(resuts["most_probable_subcategories"][1]["subcategory"])
        st.progress(resuts["most_probable_subcategories"][1]["probability"])

    with columns[2]:
        st.write(resuts["most_probable_subcategories"][2]["subcategory"])
        st.progress(resuts["most_probable_subcategories"][2]["probability"])
