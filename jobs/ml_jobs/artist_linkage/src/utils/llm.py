import asyncio

import logfire
import pandas as pd
from loguru import logger
from pydantic_ai import Agent
from tqdm.asyncio import tqdm

from src.constants import ARTIST_NAME_KEY, BIOGRAPHY_KEY, WIKIPEDIA_CONTENT_KEY
from src.llm_config import (
    LLM_MODEL,
    LLM_SETTINGS,
    MAX_RETRIES,
    WikipediaSummary,
    WikipediaSummaryPrompt,
)

EMPTY_PREDICTION = {
    BIOGRAPHY_KEY: None,
    "input_tokens": None,
    "output_tokens": None,
    "raw_response": None,
}


def summerize_biographies_with_llm(
    data: pd.DataFrame,
    max_concurrent: int,
    *,
    debug: bool,
) -> pd.DataFrame:
    """
    Run writing style prediction on the dataset using Pydantic AI with async concurrency
    """
    if debug:
        # This is to debug and monitor usage of pydantic-ai with logfire
        # See https://ai.pydantic.dev/logfire/#using-logfire
        logfire.configure(scrubbing=False)
        logfire.instrument_pydantic_ai()

    return asyncio.run(_async_summerize_biographies_with_llm(data, max_concurrent))


async def _async_summerize_biographies_with_llm(
    data: pd.DataFrame, max_concurrent: int
) -> pd.DataFrame:
    """Async implementation of writing style prediction with controlled concurrency"""
    # Initialize agent once and reuse it (cached)
    agent = Agent(
        model=LLM_MODEL,
        model_settings=LLM_SETTINGS,
        output_type=WikipediaSummary,
        system_prompt=WikipediaSummaryPrompt,
        retries=MAX_RETRIES,
    )
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


async def process_single_row(
    row_data: pd.Series, agent: Agent, semaphore: asyncio.Semaphore
) -> pd.Series:
    """Process a single row with semaphore for concurrency control"""
    async with semaphore:
        # Prepare input data
        artist_name, wikipedia_content = (
            str(row_data[ARTIST_NAME_KEY]).strip(),
            str(row_data[WIKIPEDIA_CONTENT_KEY]).strip(),
        )

        # Make prediction
        prediction_result_dict = await predict_biography_with_pydantic_ai(
            artist_name, wikipedia_content, agent
        )

        # Store result
        return pd.Series(
            {
                **row_data.to_dict(),
                **prediction_result_dict,
            }
        )


async def predict_biography_with_pydantic_ai(
    artist_name: str, wikipedia_content, agent: Agent
) -> dict[str, str | float | None]:
    try:
        # Prepare the input text
        input_text = f"""
        <Artiste>{artist_name}</Artiste>
        <ContenuWikipedia>{wikipedia_content}</ContenuWikipedia>
        """

        # Run the agent to get structured output with timeout (async version)
        result = await agent.run(input_text)

        # Convert the result to a dictionary with token usage info
        return {
            BIOGRAPHY_KEY: result.output.biography,
            "input_tokens": result.usage().input_tokens if result.usage() else None,
            "output_tokens": result.usage().output_tokens if result.usage() else None,
            "raw_response": result.output.dict() if result.output else None,
        }

    except Exception as e:
        logger.error(f"Error predicting biography '{artist_name}': {e}")
        logger.error(f"Full error details: {e!r}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return EMPTY_PREDICTION
