import time

import pandas as pd
from langchain_core.prompts import PromptTemplate
from loguru import logger
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider

from app.constants import GEMINI_MODEL_NAME

SYSTEM_PROMPT = (
    "Extrayez toutes les offres sélectionnées du contexte. "
    "Pour chaque offre, indiquez : id (product-ID ou offer-ID), pertinence (brève explication sur le lien entre l'offre et la thématique sans décrire l'offre ni prendre en compte les autres offres selectionnées). "
    "Si aucune offre ne correspond ou n'est pertinente, retournez une liste vide. "
    "Retournez le résultat sous forme d'un objet JSON avec une liste 'offers'."
)


class OfferSelection(BaseModel):
    id: str
    pertinence: str


class LLMOutput(BaseModel):
    offers: list[OfferSelection] = Field(default_factory=list)


def build_prompt(question, retrieved_results, custom_prompt=None):
    """
    Build a prompt for the LLM based on the user's question and retrieved results.
    """
    rag_prompt_template = PromptTemplate(
        input_variables=["context", "question", "action"],
        template=(
            "{action} "
            "\n\nContext:\n{context}\n\n"
            "Analysez les offres culturelles fournies et identifiez celles qui correspondent à la requête suivante : '{question}'\n\n"
        ),
    )
    if custom_prompt:
        action = custom_prompt
    else:
        action = (
            "Vous êtes un(e) curateur(trice) culturel(le) et organisateur(trice) d'événements. "
            "Votre tâche est d'analyser une liste d'offres culturelles et de sélectionner celles qui correspondent à une requête spécifique. "
            "Il n'y a pas de limite au nombre d'offres que vous pouvez sélectionner, mais vous devez privilégier la pertinence et la qualité. "
            "Présentez les offres sélectionnées dans une liste claire et organisée. Pour chaque offre sélectionnée, veuillez inclure : "
            "id, pertinence : Expliquez brièvement en français pourquoi cette offre a été retenue pour la thématique sans décrire l'offre ni prendre en compte les autres offres selectionnées."
        )
    context = "\n".join(
        [
            f"- ID: {item['id']},"
            f" Name: {item.get('offer_name', 'N/A')},"
            f" Description: {str(item.get('offer_description', 'N/A'))[:200]},"
            f" Type de bien: {item.get('offer_subcategory_id', 'N/A')}"
            for item in retrieved_results
        ]
    )
    return rag_prompt_template.format(context=context, question=question, action=action)


def build_vertex_gemini_agent(
    model_name: str = "gemini-1.5-flash",
):
    """
    Build a Pydantic AI Agent backed by Google Gemini via Vertex AI.

    Authentication:
        - Uses Application Default Credentials if available (gcloud, GCE/GKE, etc.).
        - For service account JSON, configure GOOGLE_APPLICATION_CREDENTIALS env var externally.

    Parameters mirror the Pydantic AI Google docs.
    """
    provider = GoogleProvider(
        vertexai=True, location="europe-west1", project="passculture-data-prod"
    )
    model = GoogleModel(model_name, provider=provider)
    return model


# Convenient default Vertex AI Gemini agent using ADC and default region
gemini_model = build_vertex_gemini_agent(model_name=GEMINI_MODEL_NAME)


def get_llm_agent():
    return Agent(
        gemini_model,
        output_type=[LLMOutput, str],
        system_prompt=SYSTEM_PROMPT,
    )


def llm_thematic_filtering(search_query: str, vector_search_results: list) -> LLMOutput:
    prompt = build_prompt(search_query, vector_search_results, custom_prompt=None)
    logger.info(f"Prompt length for thematic filtering: {len(prompt)}")
    start_time = time.time()
    llm_result = get_llm_agent().run_sync(prompt)
    logger.info(f"LLM thematic filtering perform in {time.time() - start_time} s")
    llm_output = llm_result.output
    logger.info(f"LLM call perform in {time.time() - start_time} s")
    # logger.info(f"LLM output: {llm_output}")
    offers = getattr(llm_output, "offers", None) or []
    # logger.info(f"Offers extracted: {offers}")
    if offers:
        llm_df = pd.DataFrame(
            [offer.dict() if hasattr(offer, "dict") else offer for offer in offers]
        )
        logger.info(f"LLM offers DataFrame len: {len(llm_df)}")
        llm_df["rank"] = range(1, len(llm_df) + 1)
    else:
        llm_df = pd.DataFrame()
        logger.info("No offers found in LLM output")
    return llm_df
