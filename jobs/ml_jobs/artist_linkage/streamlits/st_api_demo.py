import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


element_1 = {
    "venue_type_label": "Offre numérique",
    "offerer_name": "LE GROUPE THEATRAL",
    "offer_name": "Fin de partie",
    "offer_description": "Couronné du Prix Laurent Terzieff 2023 en qualité de « Meilleur spectacle dans un théâtre privé », Fin de partie revient pour 30 représentations exceptionnelles.\n\nAprès plusieurs monologues beckettiens en compagnie de Denis Lavant, Jacques Osinski fait un nouveau pari, excitant et effrayant : Fin de partie, la grande pièce de Beckett, sa préférée. Tout à coup, il faut voir les choses en grand. Sommes-nous sur terre ? Sommes-nous sur l’Arche de Noé après la fin du monde ? Peut-être est-ce déjà le purgatoire …",
}
element_2 = {
    "venue_type_label": "Offre numérique",
    "offerer_name": "MAISON POPULAIRE LA CULTURE ET LOISIRS",
    "offer_name": "Dom La Nena",
    "offer_description": "Dom La Nena est une compositrice, violoncelliste et chanteuse née au Brésil à Porto Alegre. Après avoir étudié le violoncelle classique, Dom a accompagné divers artistes en tournée, dont Jane Birkin, Jeanne Moreau, Etienne Daho et Rosemary Standley.\nAprès l’album Tempo en 2021, acclamé par la critique, Dom La Nena revient avec un nouvel album instrumental intitulé Leon. Leon est une œuvre intime, qui évoque une atmosphère obsédante, une déclaration d’amour à son complice de toujours, son violoncelle, et un retour aux sources d’une grande sensibilité, l’histoire en somme d’un bouleversement originel : la découverte de la musique classique. Après une première épiphanie nommée Vivaldi, Dom La Nena s’épanouit aussi en compagnie des œuvres romantiques de Chopin ou du minimalisme de Philip Glass qui semblent lui montrer la voie.",
}
element_3 = {
    "venue_type_label": "Offre numérique",
    "offerer_name": "MAISON DES LOISIRS DE ROUEN",
    "offer_name": "SCH à Rouen",
    "offer_description": "SCH, le rappeur iconique de Marseille Bébé vient visiter le nord pour réchauffer nos coeurs en nous présentant son dernier album JVLIVS Prequel.",
}


columns = st.columns(4)

with columns[0]:
    st.text_input("Offer Name", value="")


with columns[1]:
    st.text_input("Offerer Name", value="")


with columns[2]:
    st.text_input("Venue Type Label", value="")

with columns[3]:
    st.text_input("Offer Description", value="")


st.markdown("---")


st.header("Results")

resuts = {
    "most_probable_categories": [
        {"category": "CONFERENCE", "probability": 0.22680638115322266},
        {"category": "VISITE", "probability": 0.1634394197681487},
        {"category": "CARTE_MUSEE", "probability": 0.1433547426961901},
    ]
}

st.write(resuts)

columns = st.columns(3)

with columns[0]:
    st.write(resuts["most_probable_categories"][0]["category"])
    st.progress(resuts["most_probable_categories"][0]["probability"])


with columns[1]:
    st.write(resuts["most_probable_categories"][1]["category"])
    st.progress(resuts["most_probable_categories"][1]["probability"])


with columns[2]:
    st.write(resuts["most_probable_categories"][2]["category"])
    st.progress(resuts["most_probable_categories"][2]["probability"])
