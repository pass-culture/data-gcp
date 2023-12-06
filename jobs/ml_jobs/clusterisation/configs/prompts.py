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
        Votre tâche consiste à démontrer votre expertise dans la classification de genres culturels en décrivant les différents niveaux hiérarchiques de la classification de genres culturels.
        Votre réponse devrait être détaillée et illustrer une compréhension approfondie des différentes nuances et niveaux de classification de genres culturels.
        Return JSON {{"category" : <xxx>, "sub_category" : <xxx>, "medium" : <xxx>, "genre": <xxx>, "sub_genre": <xxx>}}
        """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples de <category>: 
        {",".join(CAT.keys())}

    """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples de <sous-category>: 
        {",".join(list(set(CAT.values())))}
        
    """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples de <genres>: 
       {",".join(list(set(GENRE.values())))}
        """,
    },
    {
        "role": "system",
        "content": f"""
        Voici des exemples de <medium>: 
       {",".join(list(set(GENRE.values())))}
        """,
    },
]


def get_macro_topics_messages(topics_micro):
    return SYSTEM_PROMPT + [
        {
            "role": "user",
            "content": f"""
            Trouvez le genre,la catégorie, le medium culturel qui est le plus commun dans les mots ci-dessous. 
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
