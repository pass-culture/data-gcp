EXPECTED_RESULTS = [
    "category",
    "sub_category",
    "medium",
    "genre",
    "sub_genre",
]


CAT = {
    "Musique (CD & Vinyles)": ["CD", "Vinyles"],
    "Spectacle Vivant, Concerts & Festivals": [
        "Festivals",
        "Concert Live",
        "Pièce de théâtre",
        "Représentation en direct",
        "Danse",
        "Stand-up",
        "Opéra",
        "One-man-show",
    ],
    "Littérature (Livres, Bandes dessinés)": [
        "Bandes dessinées",
        "Mangas",
        "Comics",
        "Jeunesse",
        "Vie pratique & Loisirs",
        "Sciences humaines & sociales",
        "Arts et spectacles",
        "Entreprise, économie & droit",
        "Sciences & Techniques",
        "Religion & Esotérisme",
        "Tourisme & Voyages",
        "Parascolaire",
        "Dictionnaires / Encyclopédies / Documentation",
        "Scolaire",
        "Essai",
        "Livre Audio",
        "Poésie",
        "Roman",
    ],
    "Cinéma & Films (DVD)": [
        "Animation",
        "Documentaire",
        "Biographie",
        "Places, Cartes & Abonnements",
        "Fiction",
        "Séries TV",
        "DVD / Blue Ray",
    ],
    "Musées & Arts Visuels": [
        "Photographie",
        "Sculpture",
        "Peinture",
        "Art Comtemporain",
        "Arts graphiques",
    ],
    "Pratique Artistique (Cours, Ateliers, Jeux, Instruments)": [
        "Fournitures artistiques",
        "Instrument de musique",
        "Escape Game",
        "Jeux Videos",
    ],
}


GENRE = {
    "Musique (CD & Vinyles)": [
        "Blues",
        "Chanson française",
        "Dubstep",
        "Drum and bass",
        "Easy listening",
        "Electronic dance music",
        "Electronica",
        "Funk",
        "Gospel",
        "Heavy metal",
        "Jazz",
        "Classique",
        "Country",
        "Musique électronique",
        "Musique expérimentale",
        "Musique folk",
        "Musique instrumentale",
        "Musique latine",
        "Musique soul",
        "Musiques du monde",
        "New age",
        "Pop",
        "Rap",
        "Reggae",
        "RnB contemporain",
        "Rock",
    ],
    "Spectacle Vivant, Concerts & Festivals": [
        "Cirque",
        "Conférence",
        "Danse Classique",
        "Danse Contemporaine",
        "Drame",
        "Ballet",
        "Orchestre",
    ],
    "Littérature (Livres, Bandes dessinés)": [
        "Arts",
        "Jeux",
        "Droit",
        "Shônen",
        "Shôjo",
        "Sport",
        "Romance",
        "Humour",
        "Langue",
        "Santé",
        "Loisirs",
        "Economie",
        "Histoire",
        "Jeunesse",
        "Policier",
        "Tourisme",
        "Sexualité",
        "Sociologie",
        "Informatique",
        "Vie pratique",
        "Arts Culinaires",
        "Bandes dessinées",
        "Carrière/Concours",
        "Gestion/entreprise",
        "Sciences, vie & Nature",
        "Littérature Etrangère",
        "Littérature française",
        "Scolaire & Parascolaire",
        "Littérature Européenne",
        "Géographie, cartographie",
        "Marketing et audio-visuel",
        "Psychanalyse, psychologie",
        "Religions, spiritualitées",
        "Poèsie, théâtre et spectacle",
        "Science-fiction, Fantastique & Terreur",
        "Sciences Humaines, Encyclopédie, Dictionnaire",
    ],
    "Cinéma & Films (DVD)": [
        "Action",
        "Animation",
        "Arts Martiaux",
        "Aventure",
        "Biopic",
        "Documentaire",
        "Drame",
        "Comédie",
        "Comédie dramatique",
        "Comédie musicale",
        "Epouvante-horreur",
        "Erotisme",
        "Espionnage",
        "Familial",
        "Fantastique",
        "Feuilleton",
        "Guerre",
        "Historique",
        "Judiciaire",
        "Musical",
        "Médical",
        "Policier",
        "Péplum",
        "Romance",
        "Science Fiction",
        "Thriller",
        "Western",
    ],
    "Musées & Arts Visuels": [
        "Dessin",
        "Peinture",
        "Photographie",
        "Sculture",
        "Arts Plastiques",
        "Art Comtemporain",
        "Arts graphiques",
    ],
    "Pratique Artistique (Cours, Ateliers, Jeux, Instruments)": [
        "Cours de Couture",
        "Cours de Peinture",
        "Cours de Dessin",
        "Photographie",
        "Histoire de l'art",
        "Réalité virtuelle",
        "Arts Plastiques",
    ],
}


MEDIUM = {
    "Musique (CD & Vinyles)": ["CD", "Vinyle", "Studio d'Enregistrement"],
    "Spectacle Vivant, Concerts & Festivals": [
        "Physique",
        "Numérique",
        "Studio d'Enregistrement",
        "Rencontre en ligne",
    ],
    "Littérature (Livres, Bandes dessinés)": [
        "Papier",
        "Numérique",
        "Audio",
    ],
    "Cinéma & Films (DVD)": ["Vidéos", "Numérique", "Streaming", "Physique"],
    "Musées & Arts Visuels": [
        "Dessin",
        "Peinture",
        "Acquarelle",
        "Photographie",
        "Réalité virtuelle",
        "Escape Game",
        "Jeu de Société",
        "Grandeur Nature",
    ],
    "Pratique Artistique (Cours, Ateliers, Jeux, Instruments)": [
        "Dessin",
        "Peinture",
        "Acquarelle",
        "Photographie",
        "Feutres",
        "Papier",
        "Crayons de Couleur",
        "Réalité virtuelle",
        "Escape Game",
        "Jeu de Société",
        "Grandeur Nature",
        "Studio d'Enregistrement",
        "Guitare",
        "Saxophone",
        "Piano",
    ],
}


SYSTEM_PROMPT = [
    {
        "role": "system",
        "content": f"""
        Vous êtes un expert dans le domaine de la classification de genres culturels. 
        On entend par classification les différents niveaux hiérachique de la classification de genres culturels, par exemple:
            - Category : Littérature, Cinéma, ...
            - Sous-category : Essai, Livre Audio, Poésie, ...
            - Médium (medium) : Guitare, Pinceaux, Instruments à Cordes, Acrylique, Aquarelle, Livre Papier, Livre Numérique
            - Genre (genre) : Rap, Rock, Shonen, Droit Juridique, ...
            - Sous-genres (subgenre) : Rap Français, Blues Caribéen, Anthologie, ...
        """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples des Category: 
        {",".join(CAT.keys())}

    """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples des Sous-Category: 
        {",".join(list(set(CAT.values())))}
        
    """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples des Genres: 
       {",".join(list(set(GENRE.values())))}
        """,
    },
]
