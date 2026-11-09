-- Mapping between the QPI ("Quiz de Préférences Initial") UX category names
-- shown to users and the Titelive GTL labels they correspond to, enriched
-- with the true GTL identifiers resolved from int_applicative__titelive_gtl.
-- Each gtl_label used in the static mapping below can come from any GTL
-- level (1 to 4), so it is matched against all four label columns to
-- recover the corresponding gtl_label_matched, matched_level and gtl_id.
-- backend_name is the backend ("Nom BDD") identifier associated to each UX name.
-- "Aucun" rows (LIVRES_AUCUN, MUSIQUES_AUCUN) carry an empty gtl_labels
-- array and only exist to keep the UX category list exhaustive for each
-- domain; they are kept with null gtl_label/gtl_label_matched/matched_level/
-- gtl_id.
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
                        ['Bandes dessinées / Comics / Manga'] as gtl_labels
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
    ),

    gtl as (
        select
            gtl_id,
            gtl_type,
            gtl_label_level_1,
            gtl_label_level_2,
            gtl_label_level_3,
            gtl_label_level_4
        from {{ ref("int_applicative__titelive_gtl") }}
    )

select
    qpi_mapping.ux_name,
    qpi_mapping.backend_name,
    qpi_mapping.gtl_label,
    gtl.gtl_type,
    gtl.gtl_id,
    case
        when qpi_mapping.gtl_label = gtl.gtl_label_level_1
        then gtl.gtl_label_level_1
        when qpi_mapping.gtl_label = gtl.gtl_label_level_2
        then gtl.gtl_label_level_2
        when qpi_mapping.gtl_label = gtl.gtl_label_level_3
        then gtl.gtl_label_level_3
        when qpi_mapping.gtl_label = gtl.gtl_label_level_4
        then gtl.gtl_label_level_4
    end as gtl_label_matched,
    case
        when qpi_mapping.gtl_label = gtl.gtl_label_level_1
        then 1
        when qpi_mapping.gtl_label = gtl.gtl_label_level_2
        then 2
        when qpi_mapping.gtl_label = gtl.gtl_label_level_3
        then 3
        when qpi_mapping.gtl_label = gtl.gtl_label_level_4
        then 4
    end as matched_level
from qpi_mapping
left join
    gtl
    on qpi_mapping.gtl_label in (
        gtl.gtl_label_level_1,
        gtl.gtl_label_level_2,
        gtl.gtl_label_level_3,
        gtl.gtl_label_level_4
    )
