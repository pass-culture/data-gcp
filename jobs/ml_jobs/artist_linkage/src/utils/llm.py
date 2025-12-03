import asyncio

import logfire
import pandas as pd
from loguru import logger
from pydantic_ai import Agent
from tqdm.asyncio import tqdm

from src.constants import (
    ARTIST_BIOGRAPHY_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIPEDIA_CONTENT_KEY,
)
from src.llm_config import (
    LLM_MODEL,
    LLM_SETTINGS,
    MAX_RETRIES,
    WikipediaSummary,
    WikipediaSummaryPrompt,
)

EMPTY_PREDICTION = {
    ARTIST_BIOGRAPHY_KEY: None,
}


def summarize_biographies_with_llm(
    data: pd.DataFrame,
    max_concurrent: int,
    *,
    debug: bool,
) -> pd.DataFrame:
    """Summarize artist biographies from Wikipedia content using an LLM.

    Args:
        data: DataFrame containing artist information with columns for artist ID,
            artist name, and Wikipedia content.
        max_concurrent: Maximum number of concurrent LLM API requests to make.
        debug: If True, enables logfire monitoring and instrumentation for pydantic-ai.

    Returns:
        DataFrame with artist IDs and their corresponding generated biographies.
        Returns empty DataFrame with column headers if no results are produced.
    """
    if debug:
        # This is to debug and monitor usage of pydantic-ai with logfire
        # See https://ai.pydantic.dev/logfire/#using-logfire
        logfire.configure(scrubbing=False)
        logfire.instrument_pydantic_ai()

    results_df = asyncio.run(
        _async_summarize_biographies_with_llm(data, max_concurrent)
    )

    if results_df.empty:
        return pd.DataFrame(columns=[ARTIST_ID_KEY, ARTIST_BIOGRAPHY_KEY])
    return results_df[[ARTIST_ID_KEY, ARTIST_BIOGRAPHY_KEY]]


async def _async_summarize_biographies_with_llm(
    data: pd.DataFrame, max_concurrent: int
) -> pd.DataFrame:
    """Async implementation of biography summarization with controlled concurrency.

    Args:
        data: DataFrame containing artist information to process.
        max_concurrent: Maximum number of concurrent API requests allowed.

    Returns:
        DataFrame containing all input data plus prediction results including
        biographies, token usage, and raw responses.
    """
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
    """Process a single artist row to generate a biography.

    Args:
        row_data: Series containing artist data including name and Wikipedia content.
        agent: Pydantic AI agent configured for biography generation.
        semaphore: Asyncio semaphore to control concurrent API calls.

    Returns:
        Series containing original row data merged with prediction results.
    """
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
    artist_name: str, wikipedia_content: str, agent: Agent
) -> dict[str, str | float | None]:
    """Generate a biography for an artist using Pydantic AI.

    Args:
        artist_name: Name of the artist to generate a biography for.
        wikipedia_content: Wikipedia content about the artist.
        agent: Configured Pydantic AI agent for making predictions.

    Returns:
        Dictionary containing:
            - biography: Generated biography text or None if error
            - input_tokens: Number of input tokens used or None
            - output_tokens: Number of output tokens generated or None
            - raw_response: Raw response dictionary from the model or None

    Note:
        Returns EMPTY_PREDICTION dict if any error occurs during processing.
    """
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
            ARTIST_BIOGRAPHY_KEY: result.output.biography,
        }

    except Exception as e:
        logger.error(f"Error predicting biography '{artist_name}': {e}")
        logger.error(f"Full error details: {e!r}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return EMPTY_PREDICTION
