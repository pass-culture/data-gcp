# Recours au pass Culture et diversité des pratiques culturelles des jeunes

<!-- Description du jeu de données à publier sur data.gouv.fr (Markdown). -->

Le passe Culture est un opérateur de l'État qui met en œuvre une mission de service public de démocratisation culturelle et de participation active de la jeunesse aux dynamiques culturelles dans l'ensemble des territoires.

Parmi les missions de service public du passe Culture figure l'enjeu de partager une connaissance sur l'offre et les pratiques culturelles des jeunes grâce à l'utilisation des données et des analyses produites par le Département Études et Recherche à partir des données de l'application. L'objectif de l'Observatoire du passe Culture est donc de proposer un environnement pour partager la connaissance produite (données, études, mise en perspective par des thématiques et témoignages) sur l'offre et les pratiques culturelles par le passe Culture et ses partenaires, et de participer à nourrir le débat et la décision publique à partir des recherches contemporaines.

L'Observatoire du passe Culture s'adosse à une politique d'ouverture des données concertée et progressive. Les indicateurs de fonctionnement de la politique publique, co-construits avec le DEPS et diffusés aux services déconcentrés du ministère de la Culture (grâce à des transmissions régulières et à Vidoc notamment), seront progressivement versés dans l'infrastructure data.gouv.fr afin de participer à la transparence de l'action publique et de permettre leur réutilisation. Les prochaines étapes visent à prolonger les projets de recherche ou initiatives du passe Culture sur des explorations par des publications en données ouvertes, dans le respect de la législation.

Pour rendre compte de l'appropriation du passe Culture dans les territoires, des indicateurs concernant l'activité des bénéficiaires et des acteurs culturels sur le passe Culture sont diffusés sur le site de l'Observatoire. Ces indicateurs sont mis à jour chaque semestre et disponibles à l'échelle nationale, régionale et départementale. Ils sont disponibles, dans un premier temps, sous forme de fiches statiques PDF, et seront tous progressivement publiés en données ouvertes.

Ce jeu de données en est la première étape : il partage les indicateurs sur l'activité des jeunes bénéficiaires du **volet individuel** du passe Culture (hors temps scolaire). Le volet collectif (temps scolaire) n'est pas encore couvert.

## Ressources

| Fichier | Contenu | Granularité |
|---|---|---|
| `beneficiary.csv` | Nombre de bénéficiaires et de bénéficiaires actuels (crédit encore disponible), en stock à la fin de chaque mois | Mois × département × âge × QPV × densité |
| `beneficiary_coverage.csv` | Nombre de bénéficiaires ayant atteint un âge donné (16 à 19 ans) sur 12 mois glissants : numérateur du taux de couverture | Mois × département × âge atteint |
| `diversity.csv` | Bénéficiaires dont le crédit a expiré dans le mois, et parmi eux ceux ayant réservé dans au moins 3 catégories d'offres | Mois d'expiration × département × QPV × densité |
| `diversity_by_category.csv` | Pour chaque catégorie d'offre, bénéficiaires dont le crédit a expiré dans le mois et ayant réservé dans cette catégorie | Mois d'expiration × département × QPV × densité × catégorie |

Chaque ressource est décrite par un schéma de données (Table Schema) qui définit chaque colonne : voir l'onglet « Schéma » de la ressource ou le [dépôt du code](https://github.com/pass-culture/data-gcp/tree/master/data_gouv).

Les fichiers sont au format CSV (UTF-8, séparateur virgule). Les dates sont au format AAAA-MM-JJ (premier jour du mois). Les codes région et département sont les codes INSEE du Code officiel géographique. Une cellule vide signifie « non renseigné » (par exemple un bénéficiaire dont l'adresse n'a pas pu être localisée), jamais zéro.

## Indicateurs

**Taux de couverture.** Nombre de jeunes bénéficiaires ayant atteint un âge donné sur 12 mois (`beneficiary_coverage.csv`), divisé par le nombre total de jeunes ayant atteint cet âge sur la même période et le même territoire, estimé par l'INSEE. Par exemple, la couverture « 18+ » correspond aux bénéficiaires ayant eu 19 ans au cours des 12 derniers mois, divisés par l'ensemble des jeunes ayant eu 19 ans en France sur la même période. Les estimations de population de l'INSEE ne sont pas republiées ici : elles sont diffusées par l'INSEE et le [code du calcul](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/metrics/population) est public.

**Bénéficiaires et bénéficiaires actuels.** Un *bénéficiaire* est un jeune ayant créé son compte et débloqué son crédit individuel, qu'il ait ou non réservé et dépensé son crédit. Un *bénéficiaire actuel* dispose encore de crédit et n'a pas atteint l'âge limite d'utilisation (veille de ses 21 ans) : c'est un stock, une photo à la fin du mois, et non un flux.

**Diversité des usages.** Part des bénéficiaires ayant réservé dans au moins 3 catégories d'offres distinctes à l'expiration de leur crédit (`total_3plus_category_booked_beneficiaries / total_expired_deposit_beneficiaries`), et part ayant réservé dans chaque catégorie (`diversity_by_category.csv`, même dénominateur). Seuls les bénéficiaires dont le dernier crédit est un crédit 18 ans (avant mars 2025) ou 17-18 ans (depuis mars 2025) sont pris en compte. Les catégories sont celles renseignées par le partenaire culturel lors de la création de l'offre.

## Protection des données (secret statistique)

Le passe Culture est tenu de respecter le secret statistique afin de préserver la confidentialité commerciale des acteurs économiques. Lorsque le tissu culturel est composé de moins de trois partenaires culturels, ou qu'une structure représente plus de 85 % des recettes, afficher le chiffre permettrait de révéler publiquement son chiffre d'affaires.

**Traitement appliqué aux données des bénéficiaires** : le nombre de bénéficiaires est « brouillé » s'il y a moins de 5 bénéficiaires déclarant résider dans la zone géographique observée. Le nombre de bénéficiaires pourra donc être légèrement modifié à l'aide d'une règle de perturbation statistique.

**Traitement appliqué aux données sur les retombées économiques et sur le nombre de partenaires culturels** : aucune donnée ne pourra être partagée si au moins une des conditions suivantes est remplie, au cours des 6 ou 48 derniers mois :

- la zone géographique observée compte moins de trois partenaires culturels actifs ;
- un partenaire culturel concentre plus de 85 % du chiffre d'affaires local.

Les ressources de ce jeu de données ne portent que sur des bénéficiaires. Leurs effectifs sont protégés par la **méthode des clés de cellule** (Cell Key Method), recommandée par l'INSEE et Eurostat :

- Une valeur publiée vaut toujours 0 ou au moins 5.
- Le bruit est au maximum de ±5 et de moyenne nulle : il se compense quand on additionne des lignes.
- **Additionner les lignes (département, région, trimestre, année) avant de calculer un taux.** Un taux calculé sur une seule petite ligne peut être fortement bruité.
- La somme des lignes d'un département ne correspond pas exactement à un total qui serait perturbé directement.

Détail de la méthode : [secret statistique](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/) (en anglais).

## Biais de lecture fréquents

- **Un taux de couverture dépasse 100 %.** Ce n'est pas une erreur : le numérateur repose sur l'adresse déclarée dans l'application, le dénominateur sur le lieu de résidence au recensement. Dans les territoires universitaires, le taux est souvent sous-estimé à 19 ans (adresse parentale non mise à jour) ; l'inverse s'observe dans les territoires ruraux ou d'origine.
- **Le taux de couverture croît avec l'âge.** Les inscriptions se cumulent : plus un jeune est âgé, plus il a eu d'occasions d'activer son crédit.
- **Les courbes des plus jeunes chutent en 2025-2026.** Depuis la réforme de mars 2025, les 15-16 ans n'ont plus de crédit individuel (offres gratuites uniquement) et le crédit à 17 ans est réduit : leur couverture tend vers 0, non par désintérêt mais parce qu'ils ne sont plus éligibles au crédit.
- **Les indicateurs à expiration du crédit bougent beaucoup d'avril 2025 à février 2026.** La validité du crédit est passée d'une durée de 2 ans à une date limite fixe (veille des 21 ans) : peu de crédits expirent sur cette période, sur une population non représentative (inscriptions tardives). À analyser avec prudence.
- **Une région a beaucoup de bénéficiaires actuels.** C'est un volume, qui suit surtout le poids démographique du territoire. Pour comparer l'adoption du dispositif entre territoires, utiliser le taux de couverture.
- **La somme des parts par catégorie dépasse 100 %.** Un même bénéficiaire peut réserver dans plusieurs catégories : chaque catégorie se lit indépendamment.
- **QPV et ruralité.** Ils sont calculés à partir de l'adresse déclarée à l'inscription, qui peut différer du lieu de résidence effectif (domicile parental pendant les études). Repères nationaux : environ 9,4 % des 15-24 ans résident en QPV, environ 31 % des 15-18 ans en zone rurale.

## Lexique

- **Crédit** : dotation octroyée à un bénéficiaire. 2019-2021 : 500 € à 18 ans (24 mois). Mai 2021 à février 2025 : 300 € à 18 ans (24 mois) ; 20 € à 15 ans, 30 € à 16 et 17 ans. Depuis mars 2025 : 150 € à 18 ans et 50 € à 17 ans (cumulables, valables jusqu'à la veille des 21 ans), plus une bonification de 50 € à 18 ans sous condition de ressources ou en situation de handicap.
- **Expiration** : au bout de 2 ans pour les crédits 18 ans antérieurs à mars 2025 ; à la veille des 21 ans pour les crédits 17-18 ans depuis mars 2025.
- **QPV** : quartier prioritaire de la politique de la ville.
- **Densité** : grille communale de densité de l'INSEE (7 niveaux, regroupés en « urbain » pour les niveaux 1 à 4 et « rural » pour les niveaux 5 à 7).
- **Catégorie d'offre** : catégorie renseignée par le partenaire culturel lors de la création de l'offre sur le portail pass Culture.

## Mise à jour et source

Les données proviennent de l'entrepôt de données de la SAS pass Culture, alimenté par les données applicatives (inscriptions, crédits, réservations). Le jeu de données est mis à jour chaque semestre, avec les données jusqu'à fin juin et fin décembre. Le code de production des données est public : [pass-culture/data-gcp](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/export/data_gouv).

Producteur : pass Culture, Pôle données.
