def get_subcat_proportion(df_user_interaction): df_user_interaction_subcat = df_user_interaction [['offer_subcategoryId']]
    df_user_interaction_subcat["subcat_count"] = 1 df_user_interaction_subcat = df_user_interaction_subcat.groupby(['offer_subcategoryId'], as_index = False) ['subcat_count'].sum() df_user_interaction_subcat ['percent'] = (
    df_user_interaction_subcat ['subcat_count'] / df_user_interaction_subcat ['subcat_count'].sum()
) * 100 return df_user_interaction_subcat.sort_values(['percent'], ascending = False)

with subcat_proportion

#need user_id, proportion(lire,cinema,musique,spectacle) , join enriched_user_data

