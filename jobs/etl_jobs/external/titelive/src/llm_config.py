from enum import Enum

from pydantic import BaseModel, Field


# Writing styles
class WritingStyle(str, Enum):
    """Enum for writing styles to ensure consistent outputs"""

    DIDACTIQUE = "Didactique"
    INFORMATIF = "Informatif"
    MYSTÉRIEUX = "Mystérieux"
    NARRATIF = "Narratif"
    RÉFLEXIF = "Réflexif"
    HUMORISTIQUE = "Humoristique"
    POÉTIQUE = "Poétique"
    DRAMATIQUE = "Dramatique"
    DESCRIPTIF = "Descriptif"
    ARGUMENTATIF = "Argumentatif"


class Topics(str, Enum):
    """Enum for topics to ensure consistent outputs"""

    AMOUR = "Amour"
    AMITIÉ = "Amitié"
    HAINE = "Haine"
    ENFANCE = "Enfance"
    ADOLESCENCE = "Adolescence"
    MAGIE = "Magie"
    IDENTITÉ = "Identité"
    BEAUTÉ = "Beauté"
    ARTS = "Arts"
    CRIMINALITÉ = "Criminalité"
    SURVIE = "Survie"
    ESPOIR = "Espoir"
    DÉSESPOIR = "Désespoir"
    MANIPULATION = "Manipulation"
    VIOLENCE = "Violence"
    SECRETS = "Secrets"
    FAMILLE = "Famille"
    SCIENCE = "Science"
    HISTOIRE = "Histoire"
    APPRENTISSAGE = "Apprentissage"
    POLITIQUE = "Politique"
    SCIENCE_FICTION = "Science-fiction"


class HistoricalPeriod(str, Enum):
    """Enum for historical periods to ensure consistent outputs"""

    ANTIQUITÉ = "Antiquité"
    HAUT_MOYEN_AGE = "Haut moyen-âge"
    MOYEN_AGE_CENTRAL = "Moyen-âge central"
    BAS_MOYEN_AGE = "Bas moyen-âge"
    RENAISSANCE = "Renaissance"
    CLASSIQUE = "Classique"
    LUMIÈRES = "Lumières"
    ROMANTIQUE = "Romantique"
    MODERNE = "Moderne"
    CONTEMPORAINE = "Contemporaine"


MAX_TOPICS = 3


class BookAnalysis(BaseModel):
    # """Structured output for book analysis"""

    writing_style: WritingStyle = Field(
        description="Le style d'écriture identifié à partir de la liste prédéfinie"
    )
    topics: list[Topics] = Field(
        description="Liste des sujets pertinents à partir de la liste prédéfinie "
        f"(max {MAX_TOPICS} éléments)",
        max_length=MAX_TOPICS,
    )
    historical_period: HistoricalPeriod | None = Field(
        default=None, description="La période historique de l'intrigue, si applicable"
    )


WRITING_STYLES = [style.value for style in WritingStyle]
