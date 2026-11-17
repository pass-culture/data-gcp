with
    qpi_mapping as (
        select ux_name, backend_name, gtl_label
        from
            unnest(
                [
                    -- LIVRE
                    struct(
                        'Mangas ou BD' as ux_name,
                        'MANGAS' as backend_name,
                        [
                            'Manga / Manhwa / Man Hua',
                            'BD généralités',
                            'Bandes dessinées',
                            'Comics',
                            'Art / Illustration'
                        ] as gtl_labels
                    ),
                    struct(
                        'Romance',
                        'ROMANCE',
                        [
                            'Romance autre',
                            'Romance contemporaine',
                            'Romance suspense',
                            'Romance historique',
                            'Romance paranormale',
                            'Comédie romantique',
                            'Romance Erotique',
                            'Dark Romance'
                        ]
                    ),
                    struct(
                        'Fantasy ou SF',
                        'FANTASY',
                        ['Fantasy', 'Fantastique / Terreur', 'Science-fiction']
                    ),
                    struct('Thriller ou Polar', 'THRILLER', ['Thriller', 'Policier']),
                    struct(
                        'Romans ou Classiques',
                        'ROMANS',
                        [
                            'Romans & Nouvelles',
                            'Œuvres classiques',
                            'Théâtre',
                            'Poésie',
                            'Contes / Légendes'
                        ]
                    ),
                    struct(
                        'Société ou Actualités',
                        'SOCIETE',
                        [
                            'Littérature argumentative',
                            'Actualités & Reportages',
                            'Sciences sociales  / Société',
                            'Sciences politiques & Politique'
                        ]
                    ),
                    struct(
                        'Philo ou Psycho',
                        'PHILOSOPHIE',
                        ['Philosophie', 'Psychologie / Psychanalyse']
                    ),
                    struct(
                        'Culture, Arts ou Sport',
                        'CULTURE',
                        [
                            'Art / Illustration',
                            'Art éphémère',
                            "Arts de l'image",
                            'Arts de la table / Gastronomie',
                            'Arts du spectacle',
                            'Arts et civilisations',
                            'Sports'
                        ]
                    ),
                    struct(
                        'Histoire ou Voyages',
                        'HISTOIRE',
                        ['Histoire', 'Archéologie', 'Géographie', 'Géographie du monde']
                    ),
                    struct('Aucun', 'LIVRES_AUCUN', cast([] as array<string>)),

                    -- MUSIQUE
                    struct(
                        'Rap ou RnB',
                        'RAP',
                        [
                            'Compilations Rap',
                            'Rap / Hip Hop',
                            'Rap / Hip Hop américain',
                            'Rap / Hip Hop francophone',
                            'Rap / Hip Hop international',
                            'Rap instrumental / DJing',
                            'RNB francophone',
                            'RNB international'
                        ]
                    ),
                    struct(
                        'Pop rock ou Variétés',
                        'POP',
                        [
                            'Pop',
                            'Pop 60s',
                            'Pop francophone',
                            'Pop internationale',
                            'Pop rétro francophone',
                            'Pop rétro internationale',
                            'Variété européenne',
                            'Variété francophone',
                            'Variété internationale',
                            'Variété rétro francophone',
                            'Variété rétro internationale',
                            'Variétés'
                        ]
                    ),
                    struct(
                        'Electro ou Techno',
                        'ELECTRO',
                        ['Electro', 'Electro francophone / French Touch', 'Techno']
                    ),
                    struct(
                        'Hard rock ou Metal',
                        'METAL',
                        [
                            'Hard Rock / Heavy Metal',
                            'Metal / Fusion',
                            'Metal / Fusion francophone',
                            'Punk / Hardcore',
                            'Punk / Hardcore francophone'
                        ]
                    ),
                    struct(
                        'Musique de Films ou Spectacles',
                        'FILMS',
                        ['Comédies musicales / Spectacles', 'Musique de Films']
                    ),
                    struct(
                        'Jazz ou Musique du Monde',
                        'JAZZ',
                        [
                            'Jazz',
                            'Jazz / Blues',
                            'Jazz Rock / Fusion / Funk',
                            'Jazz contemporain / Cool',
                            'Jazz manouche',
                            'Jazz vocal',
                            'Compilations Jazz / Blues'
                        ]
                    ),
                    struct(
                        'Classique',
                        'CLASSIQUE',
                        [
                            'Musique Classique',
                            'Musique ancienne',
                            'Musique baroque',
                            'Messes / Requiems / Passions',
                            'Opéra / Opérette / Chant lyrique'
                        ]
                    ),
                    struct('Aucun', 'MUSIQUES_AUCUN', cast([] as array<string>))
                ]
            )
        left join unnest(gtl_labels) as gtl_label
    )

select distinct qpi_mapping.ux_name, qpi_mapping.backend_name, qpi_mapping.gtl_label
from qpi_mapping
