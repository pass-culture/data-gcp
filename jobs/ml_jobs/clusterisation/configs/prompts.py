from configs.labels import CAT, GENRE

EXPECTED_RESULTS = [
    "category",
    "sub_category",
    "medium",
    "genre",
    "sub_genre",
]

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


def get_macro_topics_messages(topics_micro):
    return SYSTEM_PROMPT + [
        {
            "role": "user",
            "content": f"""
            Trouvez les genre et les catégories culturelles qui sont les plus communes dans les mots ci-dessous. 
            ``` {topics_micro} ```
            Return JSON {{"category" : <xxx>, "sub_category" : <xxx>, "medium" : <xxx>, "genre": <xxx>, "sub_genre": <xxx>}}
        """,
        },
    ]


def get_micro_topics_messages(topic_term, doc):
    return SYSTEM_PROMPT + [
        {
            "role": "user",
            "content": f"""
            Trouvez un genre ou une catégorie culturelle qui est la plus commune dans les mots ci-dessous. 
            Les mots du début sont plus important que ceux de la fin.
             ``` {topic_term} ```
            Voici des documents représentatifs: 
            ``` {doc} ```  
            Donnez un terme pour chaque type.
            Return JSON {{"category" : <xxx>, "sub_category" : <xxx>, "medium" : <xxx>, "genre": <xxx>, "sub_genre": <xxx>}}
        """,
        },
    ]
