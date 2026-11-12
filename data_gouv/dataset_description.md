# Recours au pass Culture et diversité des pratiques culturelles des jeunes

<!-- Description du jeu de données à publier sur data.gouv.fr (Markdown). -->

Le pass Culture est un opérateur de l'État et un outil des politiques publiques d'éducation artistique et culturelle et de sensibilisation des jeunes à la diversité des domaines culturels. Ce jeu de données partage des indicateurs quantitatifs sur l'activité des jeunes bénéficiaires du **volet individuel** du pass Culture (hors temps scolaire), à l'échelle nationale, régionale et départementale.

Ce sont les mêmes données que celles du tableau de bord VIDOC du ministère de la Culture (pages « Recours au pass Culture », « Bénéficiaires » et « Diversité des usages »). Le volet collectif (temps scolaire) n'est pas couvert.

## Ressources

| Fichier | Contenu | Granularité |
|---|---|---|
| `beneficiary.csv` | Nombre de bénéficiaires et de bénéficiaires actuels (crédit encore disponible), en stock à la fin de chaque mois | Mois × département × âge × QPV × densité |
| `beneficiary_coverage.csv` | Nombre de bénéficiaires ayant atteint un âge donné (16 à 20 ans) sur 12 mois glissants : numérateur du taux de couverture | Mois × département × âge atteint |
| `diversity.csv` | Bénéficiaires dont le crédit a expiré dans le mois, et parmi eux ceux ayant réservé dans au moins 3 catégories d'offres | Mois d'expiration × département × QPV × densité |
| `diversity_by_category.csv` | Pour chaque catégorie d'offre, bénéficiaires dont le crédit a expiré dans le mois et ayant réservé dans cette catégorie | Mois d'expiration × département × QPV × densité × catégorie |

Chaque ressource est décrite par un schéma de données (Table Schema) qui définit chaque colonne : voir l'onglet « Schéma » de la ressource ou le [dépôt du code](https://github.com/pass-culture/data-gcp/tree/master/data_gouv).

Les fichiers sont au format CSV (UTF-8, séparateur virgule). Les dates sont au format AAAA-MM-JJ (premier jour du mois). Les codes région et département sont les codes INSEE du Code officiel géographique. Une cellule vide signifie « non renseigné » (par exemple un bénéficiaire dont l'adresse n'a pas pu être localisée), jamais zéro.

## Indicateurs

**Taux de couverture.** Nombre de jeunes bénéficiaires ayant atteint un âge donné sur 12 mois (`beneficiary_coverage.csv`), divisé par le nombre total de jeunes ayant atteint cet âge sur la même période et le même territoire, estimé par l'INSEE. Par exemple, la couverture « 18+ » correspond aux bénéficiaires ayant eu 19 ans au cours des 12 derniers mois, divisés par l'ensemble des jeunes ayant eu 19 ans en France sur la même période. Les estimations de population de l'INSEE ne sont pas republiées ici : elles sont diffusées par l'INSEE et le [code du calcul](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/metrics/population) est public.

**Bénéficiaires et bénéficiaires actuels.** Un *bénéficiaire* est un jeune ayant créé son compte et débloqué son crédit individuel, qu'il ait ou non réservé et dépensé son crédit. Un *bénéficiaire actuel* dispose encore de crédit et n'a pas atteint l'âge limite d'utilisation (veille de ses 21 ans) : c'est un stock, une photo à la fin du mois, et non un flux.

**Diversité des usages.** Part des bénéficiaires ayant réservé dans au moins 3 catégories d'offres distinctes à l'expiration de leur crédit (`total_3plus_category_booked_beneficiaries / total_expired_deposit_beneficiaries`), et part ayant réservé dans chaque catégorie (`diversity_by_category.csv`, même dénominateur). Seuls les bénéficiaires dont le dernier crédit est un crédit 18 ans (avant mars 2025) ou 17-18 ans (depuis mars 2025) sont pris en compte. Les catégories sont celles renseignées par le partenaire culturel lors de la création de l'offre.

## Protection des données (secret statistique)

Les effectifs sont protégés par la **méthode des clés de cellule** (Cell Key Method), recommandée par l'INSEE et Eurostat : les petits effectifs sont légèrement modifiés pour qu'aucun individu ne puisse être réidentifié, tandis que les grands effectifs et les totaux restent quasiment exacts.

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

Les données proviennent de l'entrepôt de données de la SAS pass Culture, alimenté par les données applicatives (inscriptions, crédits, réservations). Seuls les mois terminés sont publiés. Le code de production des données est public : [pass-culture/data-gcp](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/export/data_gouv).

Producteur : pass Culture, Pôle données.
