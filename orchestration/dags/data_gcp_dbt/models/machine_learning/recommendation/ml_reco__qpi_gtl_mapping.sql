-- Mapping between the QPI ("Quiz de Préférences Initial") UX category names
-- shown to users and the true GTL identifiers they correspond to, resolved
-- from int_applicative__titelive_gtl. Each gtl_label used in the static
-- mapping below can come from any GTL level (1 to 4). A gtl_label matches
-- a given level only when that level's column equals the label AND the
-- next level down is null, i.e. the row is the exact GTL node for that
-- label and not one of its descendants (descendants inherit the same
-- ancestor label in their own level columns, e.g. gtl_id 01010000 has
-- gtl_label_level_2 = "Romans & Nouvelles" and gtl_label_level_3 null,
-- while its child gtl_id 01010100 has the same gtl_label_level_2 value but
-- a non-null gtl_label_level_3, so it must be excluded).
-- backend_name is the backend ("Nom BDD") identifier associated to each UX
-- name. All qpi_mapping rows are kept even without a match, for debugging:
-- "Aucun" rows (LIVRES_AUCUN, MUSIQUES_AUCUN) carry an empty gtl_labels
-- array and never match, and any gtl_label that fails to resolve to a
-- gtl_id surfaces with null gtl_id/gtl_type/gtl_label_matched/matched_level.
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
