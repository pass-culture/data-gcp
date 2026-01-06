import time

import pandas as pd
from langchain_core.prompts import PromptTemplate
from loguru import logger
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider

from app.constants import GEMINI_MODEL_NAME,GCP_PROJECT
from app.models.validators import EditorialResult

with open("app/llm/system_prompt.txt", encoding="utf-8") as f:
    SYSTEM_PROMPT = f.read().strip()

with open("app/llm/action_prompt.txt", encoding="utf-8") as f:
    ACTION_PROMPT = f.read().strip()


def build_prompt(question, retrieved_results, custom_prompt=None):
    """
    Build a prompt for the LLM based on the user's question and retrieved results.
    """
    rag_prompt_template = PromptTemplate(
        input_variables=["context", "question", "action"],
        template=(
            "{action} "
            "\n\nContext:\n{context}\n\n"
            "Analysez les offres culturelles fournies "
            "et identifiez celles qui correspondent à la requête suivante :'{question}'"
        ),
    )
    action = custom_prompt if custom_prompt else ACTION_PROMPT
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
    """
    provider = GoogleProvider(
        vertexai=True, location="europe-west1", project=GCP_PROJECT
    )
    model = GoogleModel(model_name, provider=provider)
    return model


gemini_model = build_vertex_gemini_agent(model_name=GEMINI_MODEL_NAME)


def get_llm_agent():
    return Agent(
        gemini_model,
        output_type=[EditorialResult, str],
        system_prompt=SYSTEM_PROMPT,
    )


def llm_thematic_filtering(
    search_query: str, vector_search_results: list
) -> EditorialResult:
    prompt = build_prompt(search_query, vector_search_results, custom_prompt=None)
    start_time = time.time()
    llm_result = get_llm_agent().run_sync(prompt)
    llm_output = llm_result.output
    items = getattr(llm_output, "items", None) or []
    if items:
        llm_df = pd.DataFrame(
            [item.dict() if hasattr(item, "dict") else item for item in items]
        )
        llm_df["rank"] = range(1, len(llm_df) + 1)
    else:
        llm_df = pd.DataFrame()
    return llm_df
