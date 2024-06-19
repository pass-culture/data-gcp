class QueryCard:
    def __init__(self, card_id, metabase_api):
        self.card_id = card_id
        self.metabase_api = metabase_api
        self.card_info = self.metabase_api.get_cards(self.card_id)

    def update_table_id(self, new_table_id):
        self.card_info["table_id"] = new_table_id

    def update_dataset_query(self, mapped_fields_dict, legacy_table_id, new_table_id):
        if "filter" in self.card_info["dataset_query"]["query"]:
            for filt in self.card_info["dataset_query"]["query"]["filter"]:
                if isinstance(filt, list):
                    if filt[0] == "field":
                        filt[1] = mapped_fields_dict.get(filt[1], filt[1])
                    if isinstance(filt[1], list):
                        if (
                            len(filt[1]) > 2
                            and filt[1][2]
                            and "source-field" in filt[1][2]
                        ):
                            filt[1][2]["source-field"] = mapped_fields_dict.get(
                                filt[1][2]["source-field"], filt[1][2]["source-field"]
                            )
                    for f in filt:
                        if isinstance(f, list):
                            if isinstance(f[1], list):
                                f[1][1] = mapped_fields_dict.get(f[1][1], f[1][1])
                            else:
                                f[1] = mapped_fields_dict.get(f[1], f[1])

        if "fields" in self.card_info["dataset_query"]["query"]:
            for field in self.card_info["dataset_query"]["query"]["fields"]:
                field[1] = mapped_fields_dict.get(field[1], field[1])
        if "aggregation" in self.card_info["dataset_query"]["query"]:
            for i, agg in enumerate(
                self.card_info["dataset_query"]["query"]["aggregation"]
            ):
                if agg[0] == "aggregation-options":
                    if agg[1][1][0] == "field":
                        agg[1][1][1] = mapped_fields_dict.get(
                            agg[1][1][1], agg[1][1][1]
                        )
                    elif agg[1][1][1][0] == "field":
                        agg[1][1][1][1] = mapped_fields_dict.get(
                            agg[1][1][1][1], agg[1][1][1][1]
                        )
                    elif agg[1][1][1][1][0] == "field":
                        agg[1][1][1][1][1] = mapped_fields_dict.get(
                            agg[1][1][1][1][1], agg[1][1][1][1][1]
                        )
                elif isinstance(agg, list) and len(agg) > 1:
                    agg[1][1] = mapped_fields_dict.get(agg[1][1], agg[1][1])

        if "breakout" in self.card_info["dataset_query"]["query"]:
            for bk in self.card_info["dataset_query"]["query"]["breakout"]:
                bk[1] = mapped_fields_dict.get(bk[1], bk[1])
                if len(bk) > 2 and bk[2] and "source-field" in bk[2]:
                    bk[2]["source-field"] = mapped_fields_dict.get(
                        bk[2]["source-field"], bk[2]["source-field"]
                    )
        if "source-table" in self.card_info["dataset_query"]["query"]:
            if (
                self.card_info["dataset_query"]["query"]["source-table"]
                == legacy_table_id
            ):
                self.card_info["dataset_query"]["query"]["source-table"] = new_table_id
        if "joins" in self.card_info["dataset_query"]["query"]:
            for cond in self.card_info["dataset_query"]["query"]["joins"]:
                if cond["source-table"] == legacy_table_id:
                    cond["source-table"] = new_table_id
                for field in cond["condition"]:
                    if isinstance(field, list):
                        if field[0] == "field":
                            field[1] = mapped_fields_dict.get(field[1], field[1])
                        else:
                            for el in field:
                                if el[0] == "field":
                                    el[1] = mapped_fields_dict.get(el[1], el[1])
        if "order-by" in self.card_info["dataset_query"]["query"]:
            for order in self.card_info["dataset_query"]["query"]["order-by"]:
                order[1][1] = mapped_fields_dict.get(order[1][1], order[1][1])

        if "expressions" in self.card_info["dataset_query"]["query"]:
            for key, value in self.card_info["dataset_query"]["query"][
                "expressions"
            ].items():
                stack = [value]
                while stack:
                    current = stack.pop()
                    for item in current:
                        if isinstance(item, list):
                            if item and item[0] == "field" and len(item) > 1:
                                item[1] = mapped_fields_dict.get(item[1], item[1])
                            else:
                                stack.append(item)

    def update_viz_settings(self, mapped_fields_dict):
        if "table.columns" in self.card_info["visualization_settings"]:
            for column in self.card_info["visualization_settings"]["table.columns"]:
                if "field_ref" in column and column.get("field_ref")[0] == "field":
                    column["field_ref"][1] = mapped_fields_dict.get(
                        column["field_ref"][1], column["field_ref"][1]
                    )
                elif "fieldRef" in column and column.get("fieldRef")[0] == "field":
                    column["fieldRef"][1] = mapped_fields_dict.get(
                        column["fieldRef"][1], column["fieldRef"][1]
                    )

    def update_result_metadata(self, mapped_fields_dict):

        if self.card_info["result_metadata"]:
            for result in self.card_info["result_metadata"]:
                if "field_ref" in result and result["field_ref"][0] == "field":
                    if isinstance(result["field_ref"][1], int):
                        result["field_ref"][1] = mapped_fields_dict.get(
                            result["field_ref"][1], result["field_ref"][1]
                        )
                        result["id"] = mapped_fields_dict.get(
                            result["id"], result["id"]
                        )

    def update_card(self):
        self.metabase_api.put_card(_id=self.card_id, _dict=self.card_info)
