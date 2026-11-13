-- Static mapping between the QPI ("Quiz de Préférences Initial") UX category
-- names shown to users during onboarding and the raw Titelive GTL labels they
-- correspond to. backend_name is the backend ("Nom BDD") identifier associated
-- to each UX name (ux_name and backend_name are 1:1). A backend_name can map
-- to several gtl_labels (e.g. "CULTURE" covers 4 labels), so this table has
-- one row per (ux_name, backend_name, gtl_label) tuple.
-- "Aucun" rows (LIVRES_AUCUN, MUSIQUES_AUCUN) carry an empty gtl_labels array,
-- so they surface as a single row with gtl_label null after the unnest below.
-- Downstream, recommendation_v2 resolves backend_name -> gtl_label via
-- qpi_gtl_mapping_mv to filter Vertex AI retrieval candidates on gtl_l2.
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
                        ['Bandes dessinées / Comics / Mangas'] as gtl_labels
                    ),
                    struct('Romance', 'ROMANCE', ['Romance']),
                    struct(
                        'Fantasy ou SF',
                        'FANTASY',
                        ['Science-fiction / Fantastique / Terreur']
                    ),
                    struct('Thriller ou Polar', 'THRILLER', ['Thriller / Policier']),
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
                            'Biographie / Témoignage littéraire',
                            'Actualités & Reportages',
                            'Littérature argumentative / politique / sciences sociales'
                        ]
                    ),
                    struct(
                        'Philo ou Psycho',
                        'PHILOSOPHIE',
                        [
                            'Vie quotidienne / Bien-être / Psychologie',
                            'Philosophie / Sciences'
                        ]
                    ),
                    struct(
                        'Culture, Arts ou Sport',
                        'CULTURE',
                        [
                            'Musique / Art / Architecture',
                            'Humour',
                            'Sports',
                            'Arts de la table / Gastronomie / maison'
                        ]
                    ),
                    struct(
                        'Histoire ou Voyages',
                        'HISTOIRE',
                        [
                            'Histoire / Archéologie',
                            'Géographie / Tourisme / Voyages',
                            'Nature & Plein air',
                            'Animaux'
                        ]
                    ),
                    struct('Aucun', 'LIVRES_AUCUN', cast([] as array<string>)),

                    -- MUSIQUE
                    struct('Rap ou RnB', 'RAP', ['Rap / RnB / Hip-hop']),
                    struct('Pop rock ou Variétés', 'POP', ['Pop / Rock / Variétés']),
                    struct(
                        'Electro ou Techno',
                        'ELECTRO',
                        ['Electro / techno / house music']
                    ),
                    struct('Hard rock ou Metal', 'METAL', ['Hard-rock / Metal / Punk']),
                    struct(
                        'Musique de Films ou Spectacles',
                        'FILMS',
                        ['Musique de films', 'Comédies musicales']
                    ),
                    struct(
                        'Jazz ou Musique du Monde',
                        'JAZZ',
                        [
                            'Jazz / blues / Gospel / Lounge',
                            'Musiques du monde',
                            'Country / Folk'
                        ]
                    ),
                    struct(
                        'Classique', 'CLASSIQUE', ['Classique / Baroque / Ancienne']
                    ),
                    struct('Aucun', 'MUSIQUES_AUCUN', cast([] as array<string>))
                ]
            )
        left join unnest(gtl_labels) as gtl_label
    )

select distinct qpi_mapping.ux_name, qpi_mapping.backend_name, qpi_mapping.gtl_label
from qpi_mapping
