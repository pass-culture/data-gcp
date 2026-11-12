from unittest.mock import MagicMock, patch

import pytest

from jobs.ml import item_embedding as ie

VECTOR = ie.AVAILABLE_VECTORS[0]


def _hook(existing_tables: set[str], merge_pending: bool = False) -> MagicMock:
    hook = MagicMock()
    hook.table_exists.side_effect = (
        lambda dataset_id, table_id, project_id: f"{dataset_id}.{table_id}"
        in existing_tables
    )
    hook.get_records.return_value = [[merge_pending]]
    return hook


class TestMergePending:
    def test_missing_output_has_nothing_to_protect(self):
        hook = _hook(existing_tables={VECTOR.merged_table})
        assert ie._merge_pending(hook, VECTOR) is False
        hook.get_records.assert_not_called()

    def test_missing_merged_table_is_pending(self):
        hook = _hook(existing_tables={VECTOR.output_table})
        assert ie._merge_pending(hook, VECTOR) is True
        hook.get_records.assert_not_called()

    @pytest.mark.parametrize("pending", [True, False])
    def test_both_tables_exist_queries_run_id(self, pending):
        hook = _hook(
            existing_tables={VECTOR.output_table, VECTOR.merged_table},
            merge_pending=pending,
        )
        assert ie._merge_pending(hook, VECTOR) is pending
        query = hook.get_records.call_args.args[0]
        assert VECTOR.output_table in query
        assert VECTOR.merged_table in query


class TestPlanVectorsToEmbed:
    @pytest.fixture
    def hook(self):
        with patch.object(ie, "BigQueryHook") as hook_cls:
            yield hook_cls.return_value

    def _plan(self, vectors, embed_all, pending=()):
        with patch.object(
            ie, "_merge_pending", side_effect=lambda _, v: v.name in pending
        ) as merge_pending:
            plan = ie._plan_vectors_to_embed(
                params={"vectors": vectors, "embed_all": embed_all}
            )
        return plan, merge_pending

    def test_embed_all_ignores_merge_check(self, hook):
        plan, merge_pending = self._plan(
            ie.VECTOR_NAMES, embed_all=True, pending=ie.VECTOR_NAMES
        )
        assert plan == ie.VECTOR_NAMES
        merge_pending.assert_not_called()
        hook.get_records.assert_not_called()

    def test_skips_pending_vectors_before_probing_inputs(self, hook):
        names = ["all_items_metadata", "books_metadata"]
        hook.get_records.return_value = [["books_metadata"]]
        plan, _ = self._plan(names, embed_all=False, pending={"all_items_metadata"})
        assert plan == ["books_metadata"]
        query = hook.get_records.call_args.args[0]
        assert "books_metadata_to_embed" in query
        assert "all_items_metadata_to_embed" not in query

    def test_all_pending_returns_empty_without_query(self, hook):
        plan, _ = self._plan(ie.VECTOR_NAMES, embed_all=False, pending=ie.VECTOR_NAMES)
        assert plan == []
        hook.get_records.assert_not_called()


class TestVectorChain:
    @staticmethod
    def _task(vector_name: str, step: str):
        return ie.dag.get_task(f"{vector_name}.{step}_{vector_name}")

    def test_vector_gate_only_skips_its_own_subchain(self):
        for name in ie.VECTOR_NAMES:
            gate = self._task(name, "check_vector_selected")
            assert gate.ignore_downstream_trigger_rules is False
            assert gate.downstream_task_ids == {f"{name}.export_input_{name}"}

    def test_embeds_are_serialized_through_an_all_done_barrier(self):
        for previous, current in zip(ie.VECTOR_NAMES, ie.VECTOR_NAMES[1:]):
            barrier = self._task(current, "wait_for_gpu_slot")
            assert barrier.trigger_rule == "all_done"
            assert barrier.upstream_task_ids == {f"{previous}.embed_{previous}"}
            assert barrier.downstream_task_ids == {f"{current}.prepare_{current}"}

    def test_prepare_requires_its_own_export(self):
        for name in ie.VECTOR_NAMES:
            prepare = self._task(name, "prepare")
            assert prepare.trigger_rule == "all_success"
            assert f"{name}.export_input_{name}" in prepare.upstream_task_ids
