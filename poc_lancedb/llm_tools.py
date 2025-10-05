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
            "Analysez les offres culturelles fournies et identifiez celles qui correspondent à la requête suivante : '{question}'\n\n"
        )
    )
    if custom_prompt:
        action = custom_prompt
    else:
        action = (
        "Vous êtes un(e) curateur(trice) culturel(le) et organisateur(trice) d'événements. "
        "Votre tâche est d'analyser une liste d'offres culturelles et de sélectionner celles qui correspondent à une requête spécifique. "
        "Il n'y a pas de limite au nombre d'offres que vous pouvez sélectionner, mais vous devez privilégier la pertinence et la qualité."
         "Présentez les offres sélectionnées dans une liste claire et organisée. Pour chaque offre sélectionnée, veuillez inclure : "
         "id, pertinence : Expliquez brièvement en français pourquoi cette offre a été retenue pour la thématique sans décrire l'offre ni prendre en compte les autres offres selectionnées."
        )
    context = "\n".join([
        f"- ID: {item['id']}, Name: {item.get('offer_name', 'N/A')}, Description: {str(item.get('offer_description', 'N/A'))[:500]}, Type de bien: {item.get('offer_subcategory_id', 'N/A')}"
        for item in retrieved_results
    ])
    return rag_prompt_template.format(context=context, question=question, action=action)

# --- RAG Query Function ---
class OfferSelection(BaseModel):
    id: str
    pertinence: str

class LLMOutput(BaseModel):
    offers: List[OfferSelection] = Field(default_factory=list)

# Create the agent for LLM calls and parsing
llm_agent = Agent(
    'openai:gpt-5-nano',  # or 'openai:gpt-4o-mini' if available
    output_type=[LLMOutput, str],
    system_prompt=(
        "Extrayez toutes les offres sélectionnées du contexte."
        "Pour chaque offre, indiquez : id (product-ID ou offer-ID), pertinence (brève explication sur le lien entre l'offre et la thématique sans décrire l'offre ni prendre en compte les autres offres selectionnées). "
        "Si aucune offre ne correspond ou n'est pertinente, retournez une liste vide. "
        "Retournez le résultat sous forme d'un objet JSON avec une liste 'offers'."
    ),
)


