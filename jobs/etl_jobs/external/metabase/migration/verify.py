"""Post-migration verification.

A migration is verified when no card still references a legacy table and every
card on the new table runs. Cards are executed read-only through the Metabase API.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from api.models import TableDependency, TablesToMigrate
from discovery.metabase import get_impacted_cards_from_cache

logger = logging.getLogger(__name__)


@dataclass
class VerificationReport:
    """Outcome of a verification run."""

    cards_on_legacy_tables: dict[str, list[int]] = field(default_factory=dict)
    cards_checked: list[int] = field(default_factory=list)
    failed_cards: dict[int, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.cards_on_legacy_tables and not self.failed_cards


def verify_migration(
    tables_to_migrate: TablesToMigrate,
    cache: dict[str, TableDependency],
    run_card: Callable[[int], str | None],
    card_ids: set[int] | None = None,
) -> VerificationReport:
    """Check that a migration left no card behind and broke none.

    Args:
        tables_to_migrate: The (environment-resolved) migration entries.
        cache: Card dependency cache, keyed by ``schema.table``.
        run_card: Executes a card; returns None on success, else the error message.
        card_ids: Optional restriction of the cards executed.

    Returns:
        A VerificationReport.
    """
    report = VerificationReport()

    for legacy_key, entry in tables_to_migrate.root.items():
        target_key = entry.target_table or legacy_key

        if entry.target_table is not None and entry.target_table != legacy_key:
            leftovers = get_impacted_cards_from_cache(legacy_key, cache)
            if leftovers:
                logger.error("%d cards still reference '%s': %s", len(leftovers), legacy_key, leftovers)
                report.cards_on_legacy_tables[legacy_key] = leftovers

        for card_id in get_impacted_cards_from_cache(target_key, cache):
            if card_ids is not None and card_id not in card_ids:
                continue
            if card_id in report.cards_checked:
                continue
            report.cards_checked.append(card_id)
            error = run_card(card_id)
            if error is not None:
                logger.error("✗ Card %d on '%s' fails: %s", card_id, target_key, error)
                report.failed_cards[card_id] = error
            else:
                logger.info("✓ Card %d on '%s' runs", card_id, target_key)

    return report
