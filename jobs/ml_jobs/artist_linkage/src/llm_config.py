from llm_config import WikipediaSummary, WikipediaSummaryPrompt
from pydantic import BaseModel, Field
from pydantic_ai.models.gemini import ThinkingConfig
from pydantic_ai.models.google import GoogleModel, GoogleModelSettings
from pydantic_ai.providers.google import GoogleProvider

# Model Config
GEMINI_MODEL_NAME = "gemini-flash-latest"
LLM_MODEL = GoogleModel(GEMINI_MODEL_NAME, provider=GoogleProvider(vertexai=True))
LLM_SETTINGS = GoogleModelSettings(
    google_thinking_config=ThinkingConfig(include_thoughts=False, thinking_budget=0),
    timeout=15,
)


# Prompt Config


class WikipediaSummary(BaseModel):
    biography: str = Field(
        description="Résumé de la page Wikipedia de l'artiste.", max_length=500
    )


WikipediaSummaryPrompt = """
Tu es un rédacteur pour une application d'État.
Rédige un résumé biographique en FRANÇAIS à partir du texte brut d'un article Wikipédia.
    Détails importants :
    1. Ce texte est issu d'une extraction brut de Wikipedia et peut contenir des informations non structurées.
    2. Ce texte brut peut être écrit en anglais ou en français.
    3. Concentre-toi uniquement sur la carrière artistique de la personne (genre, succès, œuvres).

    RÈGLES IMPÉRATIVES :
    1. Maximum 500 caractères. Sois extrêmement concis.
    2. Ton neutre et factuel.
    3. SUJET : Uniquement la carrière artistique (genre, succès, œuvres).
    4. INTERDIT : Pas de politique, pas de polémiques, pas de vie privée, pas de scandales.
    5. Si peu d'infos dans la source, fais une phrase simple. N'invente rien.
"""
