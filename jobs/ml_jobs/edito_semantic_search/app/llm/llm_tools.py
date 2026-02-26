import time
import pandas as pd
from loguru import logger
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.settings import ModelSettings

from app.constants import GCP_PROJECT, GEMINI_MODEL_NAME
from app.models.validators import EditorialResult

# --- Initialization ---

with open("app/llm/system_prompt.txt", encoding="utf-8") as f:
    SYSTEM_PROMPT = f.read().strip()

with open("app/llm/action_prompt.txt", encoding="utf-8") as f:
    ACTION_PROMPT = f.read().strip()

# Initialize provider once to reuse connections (Excellent for latency)
_provider = GoogleProvider(vertexai=True, location="europe-west1", project=GCP_PROJECT)

def build_vertex_gemini_agent(model_name: str = "gemini-1.5-flash"):
    """
    Build a Pydantic AI Agent optimized for speed.
    """
    logger.info(f"Building Google Gemini agent with model: {model_name}")
    # Pydantic AI's GoogleModel handles the Vertex API call natively
    model = GoogleModel(
        model_name,
        provider=_provider,
    )
    return model

gemini_model = build_vertex_gemini_agent(model_name=GEMINI_MODEL_NAME)

# Pydantic AI automatically handles native structured output via output_type
_cached_agent = Agent(
    gemini_model,
    output_type=EditorialResult,
    system_prompt=SYSTEM_PROMPT,
    retries=1,
    model_settings=ModelSettings(
        temperature=0.0,
    ),
)
logger.info(
    f"Google Gemini agent initialized successfully, with model: {GEMINI_MODEL_NAME}"
)

# --- Logic ---

def build_context_from_df(retrieved_results: list):
    """
    Converts list of dicts to a token-efficient TSV table with Short IDs.
    Returns: (tsv_table_string, reverse_id_map)
    """
    if not retrieved_results:
        return "", {}

    df = pd.DataFrame(retrieved_results)

    # 1. Short ID Mapping 
    id_map = {str(orig): str(i + 1) for i, orig in enumerate(df["id"])}
    reverse_id_map = {str(i + 1): orig for i, orig in enumerate(df["id"])}

    df["short_id"] = df["id"].astype(str).map(id_map)

    # 2. Select columns and clean text
    cols_to_keep = [
        "short_id",
        "offer_name",
        "offer_description",
        "offer_subcategory_id",
    ]
    if "offer_description" in df.columns:
        df["offer_description"] = (
            df["offer_description"]
            .fillna("N/A")
            # Strip newlines AND tabs to protect the TSV structure
            .str.replace(r"[\n\r\t]+", " ", regex=True) 
            .str.slice(0, 100) 
        )

    # 3. Convert to TSV 
    tsv_table = (
        df[cols_to_keep]
        .rename(columns={"short_id": "item_id"})
        .to_csv(index=False, sep='\t')
    )

    return tsv_table, reverse_id_map

def build_prompt(question: str, retrieved_results: list):
    """
    Build a prompt using TSV Table context with native f-strings for speed.
    """
    context_table, reverse_map = build_context_from_df(retrieved_results)

    # Dropped LangChain PromptTemplate in favor of native f-strings for lower overhead
    prompt = (
        f"### DONNÉES DES OFFRES (Format TSV)\n"
        f"{context_table}\n\n"
        f"--- \n"
        f"### REQUÊTE UTILISATEUR\n"
        f"{question}\n\n"
        f"### TÂCHE\n"
        f"Analyse les offres du tableau ci-dessus.\n"
        f"Sélectionne celles qui correspondent à la requête.\n"
    )

    logger.info(f"RAG Prompt built: {len(prompt)} chars")
    return prompt, reverse_map

def llm_thematic_filtering(
    search_query: str, vector_search_results: list
) -> pd.DataFrame:
    
    # 1. Build prompt and get the ID mapping
    prompt, reverse_map = build_prompt(search_query, vector_search_results)

    start_time = time.time()
    
    # 2. Run LLM
    llm_result = _cached_agent.run_sync(prompt)
    elapsed_time = time.time() - start_time

    # Log token usage and timing
    usage = llm_result.usage()
    if usage:
        tokens_per_sec = usage.total_tokens / elapsed_time if elapsed_time > 0 else 0
        logger.info(
            f"LLM completed in {elapsed_time:.2f}s | "
            f"Tokens: {usage.total_tokens} ({tokens_per_sec:.0f} tokens/s) | "
            f"Input: {usage.request_tokens}, Output: {usage.response_tokens}"
        )
    else:
        logger.info(f"LLM completed in {elapsed_time:.2f}s")

    # Log if validation took multiple attempts (sign of schema issues)
    # Pydantic AI retries under the hood if the LLM output fails validation
    if hasattr(llm_result, "_attempt_count") or elapsed_time > 10:
        logger.warning(
            f"⚠️ Slow LLM response ({elapsed_time:.2f}s) - possible validation retries"
        )
        logger.info(
            f'llm_result _attempt_count: {getattr(llm_result, "_attempt_count", "N/A")} '
        )

    llm_output = llm_result.output # Note: Pydantic AI uses .data for the validated response

    # 3. Handle result and restore original IDs
    try:
        items = getattr(llm_output, "items", [])
    except Exception as e:
        logger.error(f"Failed to get 'items' attribute: {e}")
        logger.error(f"LLM output repr: {llm_output!r}")
        return pd.DataFrame()

    if not items:
        logger.warning("LLM returned no items")
        return pd.DataFrame()

    processed_items = []
    for idx, item in enumerate(items):
        try:
            # Optimized for Pydantic v2 (Pydantic AI standard)
            item_dict = item.model_dump() if hasattr(item, "model_dump") else item

            # Accept both "item_id" and "id"
            raw_id = item_dict.get("item_id") or item_dict.get("id")
            if not raw_id:
                logger.error(
                    f"Item {idx} missing 'item_id'. Keys: {list(item_dict.keys())}."
                )
                continue

            # Map the Short ID back to original
            short_id = str(raw_id)
            if short_id in reverse_map:
                item_dict["item_id"] = reverse_map[short_id]
            else:
                logger.warning(
                    f"Short ID '{short_id}' not in reverse_map. "
                    f"Available IDs: {list(reverse_map.keys())[:10]}"
                )
                continue

            processed_items.append(item_dict)

        except Exception as e:
            logger.error(f"Error processing item {idx}: {e}")
            continue

    if processed_items:
        llm_df = pd.DataFrame(processed_items)
        llm_df["rank"] = range(1, len(llm_df) + 1)
        logger.info(f"Successfully processed {len(llm_df)} items")
    else:
        logger.warning("No valid items after processing")
        llm_df = pd.DataFrame()

    return llm_df