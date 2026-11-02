-- Static mapping between the QPI ("Quiz de Préférences Initial") UX category
-- names shown to users and the Titelive GTL level-1 labels (gtl_label_level_1
-- from int_applicative__titelive_gtl) they correspond to. Used to group book
-- and music offers into the coarser categories exposed in the QPI/onboarding
-- flow. "Aucun" rows carry an empty gtl_labels array and only exist to keep
-- the UX category list exhaustive for each domain.
select category, ux_name, gtl_labels
from
    unnest(
        [
            -- LIVRE
            struct(
                'LIVRE' as category,
                'Mangas ou BD' as ux_name,
                ['Bandes dessinées / Comics / Manga'] as gtl_labels
            ),
            struct('LIVRE', 'Romance', ['Romance']),
            struct(
                'LIVRE', 'Fantasy ou SF', ['Science-fiction / Fantastique / Terreur']
            ),
            struct('LIVRE', 'Thriller ou Polar', ['Thriller / Policier']),
            struct(
                'LIVRE',
                'Romans ou Classiques',
                [
                    'Romans & Nouvelles',
                    'Œuvres classiques',
                    'Théâtre',
                    'Poésie',
                    'Contes / Légendes'
                ]
            ),
            struct(
                'LIVRE',
                'Société ou Actualités',
                [
                    'Biographie / Témoignage littéraire',
                    'Actualités & Reportages',
                    'Littérature argumentative / politique / sciences sociales'
                ]
            ),
            struct(
                'LIVRE',
                'Philo ou Psycho',
                ['Vie quotidienne / Bien-être / Psychologie', 'Philosophie / Sciences']
            ),
            struct(
                'LIVRE',
                'Culture, Arts ou Sport',
                [
                    'Musique / Art / Architecture',
                    'Humour',
                    'Sports',
                    'Arts de la table / Gastronomie / maison'
                ]
            ),
            struct(
                'LIVRE',
                'Histoire ou Voyages',
                [
                    'Histoire / Archéologie',
                    'Géographie / Tourisme / Voyages',
                    'Nature & Plein air',
                    'Animaux'
                ]
            ),

            -- MUSIQUE
            struct('MUSIQUE', 'Rap ou RnB', ['Rap / RnB / Hip-hop']),
            struct('MUSIQUE', 'Pop rock ou Variétés', ['Pop / Rock / Variétés']),
            struct('MUSIQUE', 'Electro ou Techno', ['Electro / techno / house music']),
            struct('MUSIQUE', 'Hard rock ou Metal', ['Hard-rock / Metal / Punk']),
            struct(
                'MUSIQUE',
                'Musique de Films ou Spectacles',
                ['Musique de films', 'Comédies musicales']
            ),
            struct(
                'MUSIQUE',
                'Jazz ou Musique du Monde',
                [
                    'Jazz / blues / Gospel / Lounge',
                    'Musiques du monde',
                    'Country / Folk'
                ]
            ),
            struct('MUSIQUE', 'Classique', ['Classique / Baroque / Ancienne'])
        ]
    )
