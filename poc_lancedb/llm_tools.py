from pydantic import BaseModel, Field
from pydantic_ai import Agent
from langchain.prompts import PromptTemplate
from typing import List
def build_prompt(question, retrieved_results,custom_prompt=None):
    """
    Builds a prompt for the LLM based on the user's question and retrieved results.
    """
    # LangChain prompt template for RAG
    rag_prompt_template = PromptTemplate(
        input_variables=["context", "question", "action"],
        template=(
            "{action} "
            "\n\nContext:\n{context}\n\n"
            "Analyze the provided cultural offerings and identify all events, exhibitions, or performances that fit the following thematic: '{question}'\n\n"
        )
    )
    if custom_prompt:
        action = custom_prompt
    else:
        action = (
            "You are a cultural curator and event planner. "
            "Your task is to analyze a list of cultural offerings and select those that align with a specific thematic. "
            "There is no limit on the number of offers you can select, but you should focus on relevance and quality."
            "Present the selected offerings in a clear and organized list. For each selected item, please include: id, relevance: Briefly explain why this offering was selected for the thematic."
        )
    context = "\n".join([f"- ID: {item['id']}, Description: {item.get('offer_description', 'N/A')}" for item in retrieved_results])
    return rag_prompt_template.format(context=context, question=question, action=action)

# --- RAG Query Function ---
class OfferSelection(BaseModel):
    id: str
    relevance: str

class LLMOutput(BaseModel):
    offers: List[OfferSelection] = Field(default_factory=list)

# Create the agent for LLM calls and parsing
llm_agent = Agent(
    'openai:gpt-3.5-turbo',  # or 'openai:gpt-4o-mini' if available
    output_type=[LLMOutput, str],
    system_prompt=(
        "Extract all selected offers from the context. "
        "For each offer, provide: id (product-ID or offer-ID), relevance (brief explanation). "
        "If no offers match, return an empty list. "
        "Return the result as a JSON object with an 'offers' list."
    ),
)


