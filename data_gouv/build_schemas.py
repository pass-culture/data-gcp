"""Build the Table Schema (https://specs.frictionlessdata.io/table-schema/) files of
the pass Culture datasets published on data.gouv.fr.

Field definitions are written in French for data.gouv.fr users; column names are
the ones of the dbt models `exp_data_gouv__<resource>`. A published version is
immutable: bump SCHEMA_VERSION and regenerate when a resource changes, then
reference the new URL on data.gouv.fr.

    python -I data_gouv/build_schemas.py
"""

import json
from pathlib import Path

SCHEMA_VERSION = "1.0.0"
SCHEMAS_DIR = Path(__file__).parent / "schemas"
REPO_URL = "https://github.com/pass-culture/data-gcp"
RAW_URL = (
    "https://raw.githubusercontent.com/pass-culture/data-gcp/master/data_gouv/schemas"
)
DOCS_URL = "https://pass-culture.github.io/data-gcp"

PERTURBATION = (
    "Effectif protégé par la méthode de perturbation des clés de cellule "
    "(Cell Key Method, recommandée par l'INSEE et Eurostat) : une valeur publiée "
    "vaut 0 ou au moins 5, et les petits effectifs sont légèrement bruités (±5 au "
    "maximum). Additionner plusieurs lignes (département, région, trimestre, "
    "année) avant de calculer un taux : le bruit se compense à l'agrégation."
)

MICRO_DENSITY = [
    "Grands centres urbains",
    "Centres urbains intermédiaires",
    "Ceintures urbaines",
    "Petites villes",
    "Bourgs ruraux",
    "Rural à habitat dispersé",
    "Rural à habitat très dispersé",
    "non localisé",
]

OFFER_CATEGORIES = {
    "BEAUX_ARTS": "Beaux-arts",
    "CARTE_JEUNES": "Carte jeunes",
    "CINEMA": "Cinéma (séances, abonnements)",
    "CONFERENCE": "Conférences, rencontres",
    "FILM": "Films, vidéos (supports physiques, VOD)",
    "INSTRUMENT": "Instruments de musique",
    "JEU": "Jeux",
    "LIVRE": "Livres",
    "MEDIA": "Médias, presse",
    "MUSEE": "Musées, patrimoine, expositions, visites",
    "MUSIQUE_ENREGISTREE": "Musique enregistrée",
    "MUSIQUE_LIVE": "Musique live (concerts, festivals)",
    "PRATIQUE_ART": "Pratiques artistiques (cours, ateliers)",
    "SPECTACLE": "Spectacle vivant",
    "TECHNIQUE": "Catégorie technique (offres internes)",
}


def month_field(name, title, description):
    return {
        "name": name,
        "title": title,
        "description": description
        + " Date au format AAAA-MM-JJ, toujours le premier jour du mois. Seuls les "
        "mois terminés sont publiés.",
        "type": "date",
        "format": "%Y-%m-%d",
        "example": "2026-01-01",
        "constraints": {"required": True},
    }


GEO_FIELDS = [
    {
        "name": "region_code",
        "title": "Code région",
        "description": "Code INSEE (Code officiel géographique) de la région de "
        "l'adresse déclarée par le bénéficiaire à l'inscription. Vide pour les "
        "collectivités d'outre-mer (975, 978, 986, 987, 988) et pour les "
        "bénéficiaires non localisés.",
        "type": "string",
        "example": "11",
        "constraints": {"required": False, "pattern": "^[0-9]{2}$"},
    },
    {
        "name": "region_name",
        "title": "Région",
        "description": "Nom de la région. « non localisé » lorsque l'adresse n'a pas "
        "pu être rattachée à une commune ; nom de la collectivité ou « Collectivités "
        "d'outre-mer » pour les collectivités d'outre-mer.",
        "type": "string",
        "example": "Île-de-France",
        "constraints": {"required": True},
    },
    {
        "name": "department_code",
        "title": "Code département",
        "description": "Code INSEE (Code officiel géographique) du département de "
        "l'adresse déclarée par le bénéficiaire à l'inscription (2A, 2B pour la "
        "Corse, 971 à 988 pour l'outre-mer). Vide pour les bénéficiaires non "
        "localisés.",
        "type": "string",
        "example": "75",
        "constraints": {
            "required": False,
            "pattern": "^([0-9]{2}|2A|2B|9[78][0-9])$",
        },
    },
    {
        "name": "department_name",
        "title": "Département",
        "description": "Nom du département. « non localisé » lorsque l'adresse n'a "
        "pas pu être rattachée à une commune.",
        "type": "string",
        "example": "Paris",
        "constraints": {"required": True},
    },
]

QPV_DENSITY_FIELDS = [
    {
        "name": "is_in_qpv",
        "title": "Résident en QPV",
        "description": "Vrai si l'adresse déclarée par le bénéficiaire est située dans "
        "un quartier prioritaire de la politique de la ville (QPV). L'adresse peut "
        "différer du lieu de résidence effectif (domicile parental, études). Vide si "
        "l'adresse n'a pas pu être localisée.",
        "type": "boolean",
        "trueValues": ["true"],
        "falseValues": ["false"],
        "constraints": {"required": False},
    },
    {
        "name": "macro_density_label",
        "title": "Densité (2 niveaux)",
        "description": "Densité de la commune de l'adresse déclarée selon la grille "
        "communale de densité de l'INSEE : « urbain » (niveaux 1 à 4), « rural » "
        "(niveaux 5 à 7), « non localisé » si l'adresse n'a pas pu être rattachée à "
        "une commune.",
        "type": "string",
        "example": "rural",
        "constraints": {"required": False, "enum": ["urbain", "rural", "non localisé"]},
    },
    {
        "name": "micro_density_label",
        "title": "Densité (7 niveaux)",
        "description": "Niveau de la grille communale de densité de l'INSEE (7 "
        "niveaux) de la commune de l'adresse déclarée, « non localisé » si l'adresse "
        "n'a pas pu être rattachée à une commune.",
        "type": "string",
        "example": "Bourgs ruraux",
        "constraints": {"required": False, "enum": MICRO_DENSITY},
    },
]


def count_field(name, title, description, example):
    return {
        "name": name,
        "title": title,
        "description": f"{description} {PERTURBATION}",
        "type": "integer",
        "example": str(example),
        "constraints": {"required": True, "minimum": 0},
    }


RESOURCES = {
    "beneficiary": {
        "title": "Bénéficiaires du pass Culture par mois et territoire",
        "description": "Stock mensuel de bénéficiaires du volet individuel du pass "
        "Culture (jeunes ayant reçu un crédit individuel), et parmi eux des "
        "bénéficiaires actuels (crédit encore disponible), par territoire de "
        "résidence, âge, QPV et densité. Une ligne par mois et combinaison de "
        "dimensions.",
        "kpi_doc": "vidoc_kpis/beneficiaries/",
        "fields": [
            month_field(
                "partition_month",
                "Mois",
                "Mois d'observation : le stock est mesuré à la fin de ce mois.",
            ),
            *GEO_FIELDS,
            {
                "name": "age_at_calculation",
                "title": "Âge",
                "description": "Âge révolu du bénéficiaire à la fin du mois "
                "d'observation. Un même jeune change donc de tranche d'âge au fil "
                "des mois. Vide si la date de naissance est inconnue. Quelques "
                "lignes présentent des âges hors 15-20 ans (dates de naissance "
                "déclarées incohérentes, ou bénéficiaires dont le crédit a expiré).",
                "type": "integer",
                "example": "18",
                "constraints": {"required": False, "minimum": 0},
            },
            *QPV_DENSITY_FIELDS,
            count_field(
                "total_beneficiaries",
                "Nombre de bénéficiaires",
                "Nombre de jeunes ayant reçu au moins un crédit individuel du pass "
                "Culture à la fin du mois, que ce crédit soit encore disponible, "
                "entièrement dépensé ou expiré, et qu'ils aient ou non réservé. Les "
                "comptes supprimés sortent du stock. Les jeunes de 15-16 ans "
                "bénéficiant uniquement des offres gratuites (depuis mars 2025) ne "
                "sont pas comptés.",
                723065,
            ),
            count_field(
                "total_actual_beneficiaries",
                "Nombre de bénéficiaires actuels",
                "Nombre de bénéficiaires dont le crédit n'est ni expiré ni "
                "entièrement dépensé à la fin du mois (crédit encore disponible et "
                "âge limite d'utilisation non atteint). Sous-ensemble de "
                "total_beneficiaries : il ne lui est jamais supérieur.",
                684122,
            ),
        ],
    },
    "beneficiary_coverage": {
        "title": "Bénéficiaires du pass Culture ayant atteint un âge sur 12 mois glissants",
        "description": "Nombre de bénéficiaires du pass Culture ayant atteint un âge "
        "donné (16 à 20 ans) au cours des 12 derniers mois, par département de "
        "résidence. C'est le numérateur du taux de couverture : divisé par la "
        "population de cet âge estimée par l'INSEE sur la même période et le même "
        "territoire, il donne la part des jeunes ayant bénéficié du pass Culture. "
        "La population INSEE n'est pas republiée ici ; le code de calcul est public.",
        "kpi_doc": "vidoc_kpis/beneficiaries/beneficiary_coverage_rate/",
        "fields": [
            month_field(
                "partition_month",
                "Mois",
                "Dernier mois de la fenêtre glissante de 12 mois.",
            ),
            *GEO_FIELDS,
            {
                "name": "milestone_age",
                "title": "Âge atteint",
                "description": "Âge (en années révolues) atteint par les jeunes au "
                "cours de la fenêtre de 12 mois. Un âge atteint de N ans correspond "
                "à la couverture « N-1+ » de VIDOC : 19 ans correspond à la "
                "couverture 18+, c'est-à-dire aux jeunes ayant bénéficié du pass "
                "Culture avant leurs 19 ans.",
                "type": "integer",
                "example": "19",
                "constraints": {"required": True, "minimum": 16, "maximum": 20},
            },
            count_field(
                "total_beneficiaries_last_12_months",
                "Nombre de bénéficiaires ayant atteint l'âge sur 12 mois",
                "Nombre de bénéficiaires (jeunes ayant reçu au moins un crédit "
                "individuel) ayant atteint l'âge milestone_age au cours des 12 mois "
                "se terminant par partition_month. Chaque jeune n'atteint un âge "
                "qu'une fois et n'est donc compté qu'une fois.",
                737567,
            ),
        ],
    },
    "diversity": {
        "title": "Diversité des pratiques culturelles à l'expiration du crédit",
        "description": "Bénéficiaires dont le crédit pass Culture a expiré dans le "
        "mois, et parmi eux ceux ayant réservé dans au moins 3 catégories d'offres "
        "distinctes, par territoire de résidence, QPV et densité. Le rapport des "
        "deux colonnes donne la part de bénéficiaires ayant diversifié leurs "
        "pratiques. Seuls les bénéficiaires dont le dernier crédit est un crédit "
        "17-18 ans sont pris en compte.",
        "kpi_doc": "vidoc_kpis/diversity/diversity_3plus_categories/",
        "fields": [
            month_field(
                "deposit_expiration_month",
                "Mois d'expiration du crédit",
                "Mois au cours duquel le crédit des bénéficiaires a expiré. "
                "Attention : depuis la réforme de mars 2025, la validité du crédit "
                "passe d'une durée de 2 ans à une date limite fixe (veille des 21 "
                "ans), ce qui crée un creux statistique d'avril 2025 à février 2026 "
                "(peu d'expirations, population non représentative).",
            ),
            *GEO_FIELDS,
            *QPV_DENSITY_FIELDS,
            count_field(
                "total_expired_deposit_beneficiaries",
                "Nombre de bénéficiaires dont le crédit a expiré",
                "Nombre de bénéficiaires dont le crédit a expiré dans le mois. "
                "Dénominateur des parts de ce jeu de données.",
                61317,
            ),
            count_field(
                "total_3plus_category_booked_beneficiaries",
                "Nombre de bénéficiaires ayant réservé dans 3 catégories ou plus",
                "Parmi les bénéficiaires dont le crédit a expiré dans le mois, "
                "nombre de ceux ayant effectué des réservations validées dans au "
                "moins 3 catégories d'offres distinctes avant l'expiration (toutes "
                "les réservations faites avec leurs crédits de 15 à 20 ans). Jamais "
                "supérieur à total_expired_deposit_beneficiaries.",
                33806,
            ),
        ],
    },
    "diversity_by_category": {
        "title": "Catégories d'offres réservées à l'expiration du crédit",
        "description": "Pour chaque catégorie d'offre, nombre de bénéficiaires dont "
        "le crédit pass Culture a expiré dans le mois et ayant réservé au moins une "
        "offre de cette catégorie, par territoire de résidence, QPV et densité. Le "
        "dénominateur est total_expired_deposit_beneficiaries du jeu « diversity » "
        "pour le même mois et la même ligne (mêmes territoire, QPV et densité). Un "
        "même bénéficiaire pouvant réserver dans plusieurs catégories, la somme des "
        "parts dépasse 100 %.",
        "kpi_doc": "vidoc_kpis/diversity/booking_share_by_category/",
        "fields": [
            month_field(
                "deposit_expiration_month",
                "Mois d'expiration du crédit",
                "Mois au cours duquel le crédit des bénéficiaires a expiré (voir le "
                "jeu « diversity » pour le creux statistique d'avril 2025 à "
                "février 2026).",
            ),
            *GEO_FIELDS,
            *QPV_DENSITY_FIELDS,
            {
                "name": "offer_category_id",
                "title": "Catégorie d'offre",
                "description": "Catégorie de l'offre, renseignée par le partenaire "
                "culturel lors de la création de l'offre sur le portail pass "
                "Culture : "
                + " ; ".join(f"{k} = {v}" for k, v in OFFER_CATEGORIES.items())
                + ".",
                "type": "string",
                "example": "LIVRE",
                "constraints": {"required": True, "enum": list(OFFER_CATEGORIES)},
            },
            count_field(
                "total_category_booked_beneficiaries",
                "Nombre de bénéficiaires ayant réservé dans la catégorie",
                "Parmi les bénéficiaires dont le crédit a expiré dans le mois, "
                "nombre de ceux ayant effectué au moins une réservation validée dans "
                "la catégorie avant l'expiration. Jamais supérieur à "
                "total_expired_deposit_beneficiaries de la même ligne du jeu "
                "« diversity ».",
                41000,
            ),
        ],
    },
}


def build_schema(resource, spec):
    model = f"exp_data_gouv__{resource}"
    return {
        "$schema": "https://frictionlessdata.io/schemas/table-schema.json",
        "name": f"pass-culture-{resource.replace('_', '-')}",
        "title": spec["title"],
        "description": spec["description"],
        "version": SCHEMA_VERSION,
        "path": f"{RAW_URL}/{resource}/{SCHEMA_VERSION}/schema.json",
        "homepage": f"{REPO_URL}/tree/master/data_gouv",
        "countryCode": "FR",
        "licenses": [
            {
                "name": "etalab-2.0",
                "title": "Licence Ouverte / Open Licence version 2.0",
                "path": "https://www.etalab.gouv.fr/licence-ouverte-open-licence/",
            }
        ],
        "contributors": [{"title": "pass Culture - Pôle données", "role": "author"}],
        "sources": [
            {
                "title": f"Modèle dbt {model}",
                "path": f"{REPO_URL}/blob/master/orchestration/dags/data_gcp_dbt/models/export/data_gouv/{model}.sql",
            },
            {
                "title": "Définition de l'indicateur (documentation, en anglais)",
                "path": f"{DOCS_URL}/dbt/{spec['kpi_doc']}",
            },
            {
                "title": "Méthode de protection statistique (en anglais)",
                "path": f"{DOCS_URL}/dbt/references/statistical_confidentiality/",
            },
        ],
        "missingValues": [""],
        "fields": spec["fields"],
    }


def main():
    for resource, spec in RESOURCES.items():
        path = SCHEMAS_DIR / resource / SCHEMA_VERSION / "schema.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(build_schema(resource, spec), ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
        print(path)


if __name__ == "__main__":
    main()
