import asyncio

import logfire
import pandas as pd
from loguru import logger
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.gemini import ThinkingConfig
from pydantic_ai.models.google import GoogleModel, GoogleModelSettings
from pydantic_ai.providers.google import GoogleProvider
from tqdm.asyncio import tqdm

from src.llm_config import (
    MAX_TOPICS,
    BookAnalysis,
    HistoricalPeriod,
    Topics,
    WritingStyle,
)

# This is to debug and monitor usage of pydantic-ai with logfire
# See https://ai.pydantic.dev/logfire/#using-logfire
logfire.configure(scrubbing=False)
logfire.instrument_pydantic_ai()


def initialize_pydantic_ai_agent(
    gemini_model_name: str, output_type: type[BaseModel] = BookAnalysis
) -> Agent | None:
    """Initialize pydantic AI agent with Google Vertex AI (cached for performance)"""
    try:
        # Use Vertex AI with application default credentials
        provider = GoogleProvider(vertexai=True)
        model = GoogleModel(gemini_model_name, provider=provider)
        thinkingDisabledConfig = ThinkingConfig(
            include_thoughts=False, thinking_budget=0
        )
        model_settings = GoogleModelSettings(
            google_thinking_config=thinkingDisabledConfig, timeout=15
        )
        # Create agent with structured output
        agent = Agent(
            model=model,
            model_settings=model_settings,
            output_type=output_type,
            system_prompt=f"""Vous êtes un analyste littéraire expert spécialisé dans l'identification des styles d'écriture et des thèmes dans les livres.

            Votre tâche est d'analyser les titres de livres, les résumés, et les informations sur les auteurs pour déterminer :
            1. Le style d'écriture à partir de la liste prédéfinie
            2. Les sujets/thèmes pertinents (0 à {MAX_TOPICS} sujets maximum)
            3. La période historique de l'intrigue (si applicable)

            <Styles d'écriture disponibles>
            - {WritingStyle.DIDACTIQUE.value} : Contenu éducatif, instructionnel
            - {WritingStyle.INFORMATIF.value} : Contenu informatif, factuel
            - {WritingStyle.MYSTÉRIEUX.value} : Mystère, suspense, énigmatique
            - {WritingStyle.NARRATIF.value} : Narration, axé sur l'histoire
            - {WritingStyle.RÉFLEXIF.value} : Contemplatif, introspectif
            - {WritingStyle.HUMORISTIQUE.value} : Humoristique, comique
            - {WritingStyle.POÉTIQUE.value} : Langage poétique, lyrique
            - {WritingStyle.DRAMATIQUE.value} : Dramatique, émotions intenses
            - {WritingStyle.DESCRIPTIF.value} : Descriptif, imagerie détaillée
            - {WritingStyle.ARGUMENTATIF.value} : Persuasif, argumentatif
            </Styles d'écriture disponibles>

            <Sujets disponibles>
            - {Topics.AMOUR.value} : Amour, romance, relations
            - {Topics.AMITIÉ.value} : Amitié, compagnonnage
            - {Topics.HAINE.value} : Haine, antagonisme, conflit
            - {Topics.ENFANCE.value} : Enfance, jeunesse
            - {Topics.ADOLESCENCE.value} : Années d'adolescence, passage à l'âge adulte
            - {Topics.MAGIE.value} : Magie, éléments surnaturels
            - {Topics.IDENTITÉ.value} : Identité, découverte de soi
            - {Topics.BEAUTÉ.value} : Beauté, esthétique
            - {Topics.ARTS.value} : Art, créativité, expression artistique
            - {Topics.CRIMINALITÉ.value} : Crime, activités criminelles
            - {Topics.SURVIE.value} : Survie, persévérance
            - {Topics.ESPOIR.value} : Espoir, optimisme
            - {Topics.DÉSESPOIR.value} : Désespoir, absence d'espoir
            - {Topics.MANIPULATION.value} : Manipulation, contrôle
            - {Topics.VIOLENCE.value} : Violence, agression
            - {Topics.SECRETS.value} : Secrets, vérités cachées
            - {Topics.FAMILLE.value} : Famille, proches
            - {Topics.SCIENCE.value} : Science, thèmes scientifiques
            - {Topics.HISTOIRE.value} : Histoire, événements historiques
            - {Topics.APPRENTISSAGE.value} : Apprentissage, éducation
            - {Topics.POLITIQUE.value} : Politique, thèmes politiques
            - {Topics.SCIENCE_FICTION.value} : Science-fiction, thèmes futuristes
            </Sujets disponibles>

            <Périodes historiques>
            - {HistoricalPeriod.ANTIQUITÉ.value} : avant 476
            - {HistoricalPeriod.HAUT_MOYEN_AGE.value} : 476 à 999
            - {HistoricalPeriod.MOYEN_AGE_CENTRAL.value} : 1000 à 1299
            - {HistoricalPeriod.BAS_MOYEN_AGE.value} : 1300 à 1499
            - {HistoricalPeriod.RENAISSANCE.value} : 1500 à 1599
            - {HistoricalPeriod.CLASSIQUE.value} : 1599 à 1699
            - {HistoricalPeriod.LUMIÈRES.value} : 1700 à 1799
            - {HistoricalPeriod.ROMANTIQUE.value} : 1800 à 1899
            - {HistoricalPeriod.MODERNE.value} : 1900 à 1945
            - {HistoricalPeriod.CONTEMPORAINE.value} : 1946 à aujourd'hui
            </Périodes historiques>

            <Directives>
            - Basez votre analyse du style d'écriture sur la façon dont le contenu est présenté, pas seulement sur le sujet
            - Sélectionnez 0 à {MAX_TOPICS} sujets les plus pertinents qui sont centraux au contenu du livre
                - Soyez très sélectif avec les sujets - ne choisissez que ceux qui sont vraiment centraux au livre.
                - Nous préfèrons un nombre plus petit (1-2) de sujets très pertinents qu'une longue liste de sujets moins pertinents
            - Identifiez la période historique uniquement si l'intrigue se déroule clairement dans une époque spécifique
                - Pour les livres contemporains ou sans contexte historique précis, laissez la période historique vide
            </Directives>
            """,  # noqa: E501
        )

        return agent
    except Exception as e:
        logger.error(f"Error initializing Pydantic AI agent: {e}")
        logger.error(f"Full error details: {e!r}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def prepare_text_data(row_data: pd.Series) -> tuple[str, str, str]:
    """Prepare text data from row for prediction"""
    title = str(row_data.get("titre", "")).strip()
    resume = str(row_data.get("article_resume", "")).strip()
    authors = str(row_data.get("auteurs_multi", "")).strip()

    return title, resume, authors


async def predict_writing_style_with_pydantic_ai(
    title: str, resume: str, authors: str, agent: Agent
) -> dict[str, str | float | list[str] | None]:
    """
    Predict writing style and topics using Pydantic AI for structured output (async)
    """
    try:
        # Prepare the input text
        input_text = f"""
        <Titre>{title}</Titre>
        <Résumé>{resume}</Résumé>
        <Auteurs>{authors}</Auteurs>

        Veuillez analyser ce livre et déterminer son style d'écriture et ses sujets pertinents.
        """  # noqa: E501

        # Run the agent to get structured output with timeout (async version)
        result = await agent.run(input_text)

        # Extract topics as list of string values
        topics_list = [topic.value for topic in result.output.topics]

        # Extract historical period if available
        historical_period_value = (
            result.output.historical_period.value
            if result.output.historical_period
            else None
        )

        # Convert the result to a dictionary with token usage info
        return {
            "prediction": result.output.writing_style.value,
            "topics": topics_list,
            "historical_period": historical_period_value,
            "input_tokens": result.usage().input_tokens if result.usage() else None,
            "output_tokens": result.usage().output_tokens if result.usage() else None,
            "raw_response": result.output.dict() if result.output else None,
        }

    except Exception as e:
        logger.error(f"Error predicting writing style for '{title}': {e}")
        logger.error(f"Full error details: {e!r}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "prediction": None,
            "topics": None,
            "historical_period": None,
            "input_tokens": None,
            "output_tokens": None,
            "raw_response": None,
        }


async def process_single_row(
    row_data: pd.Series, agent: Agent, semaphore: asyncio.Semaphore
) -> pd.Series:
    """Process a single row with semaphore for concurrency control"""
    async with semaphore:
        # Prepare input data
        title, resume, authors = prepare_text_data(row_data)

        # Skip if essential data is missing
        if not title and not resume:
            prediction_result = {
                "prediction": None,
                "topics": None,
                "historical_period": None,
                "input_tokens": None,
                "output_tokens": None,
                "raw_response": None,
            }
        else:
            # Make prediction
            prediction_result = await predict_writing_style_with_pydantic_ai(
                title, resume, authors, agent
            )

        # Store result
        result_row = row_data.copy()
        result_row["predicted_writing_style"] = prediction_result["prediction"]
        result_row["predicted_topics"] = prediction_result["topics"]
        result_row["predicted_historical_period"] = prediction_result[
            "historical_period"
        ]
        result_row["input_tokens"] = prediction_result["input_tokens"]
        result_row["output_tokens"] = prediction_result["output_tokens"]
        result_row["raw_llm_response"] = prediction_result["raw_response"]
        return result_row


def run_writing_style_prediction(
    data: pd.DataFrame, gemini_model_name: str, max_concurrent: int = 5
) -> pd.DataFrame:
    """
    Run writing style prediction on the dataset using Pydantic AI with async concurrency
    """
    return asyncio.run(
        _async_run_writing_style_prediction(data, gemini_model_name, max_concurrent)
    )


async def _async_run_writing_style_prediction(
    data: pd.DataFrame, gemini_model_name: str, max_concurrent: int = 5
) -> pd.DataFrame:
    """Async implementation of writing style prediction with controlled concurrency"""
    # Initialize agent once and reuse it (cached)
    agent = initialize_pydantic_ai_agent(gemini_model_name=gemini_model_name)
    if not agent:
        logger.error(
            "Failed to initialize Pydantic AI agent. Cannot proceed with predictions."
        )
        return pd.DataFrame()

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)

    # Create tasks for all rows
    tasks = [
        process_single_row(row_data, agent, semaphore)
        for _, row_data in data.iterrows()
    ]

    # Run all tasks concurrently with progress bar
    # Use asyncio.gather with tqdm to show progress
    completed_results = await tqdm.gather(
        *tasks, desc="Predicting writing styles (async)", total=len(tasks)
    )

    return pd.DataFrame(completed_results)
