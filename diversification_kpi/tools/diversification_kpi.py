import numpy as np
import pandas as pd


def get_diversification_feature(list_used, feature_value):
    div = 0
    if feature_value not in list_used and feature_value not in [np.nan, ""]:
        div = 1
    return div


def add_feature_if_feature_not_booked(list_used, feature_value):
    if feature_value not in list_used and feature_value not in [np.nan, ""]:
        list_used.append(feature_value)
    return list_used


def calculate_diversification_per_feature(df_clean, features):
    divers_per_feature = {}
    list_per_feature = {}
    list_per_feature_free = {}
    for feature in features:
        divers_per_feature[feature] = []
    divers_per_feature["qpi_diversification"] = []
    divers_per_feature["delta_diversification"] = []
    for i in range(0, len(df_clean)):
        booking = df_clean.iloc[i]
        if len(booking["qpi_subcategories"]) > 0:
            user_qpi_subcategories = [
                qpi["subcategories"] for qpi in list(booking["qpi_subcategories"])
            ]
        else:
            user_qpi_subcategories = []
        for feature in features:
            feature_value = booking[feature]
            multiplicator = 1

            # Use a unique venue for digital bookings
            if feature == "venue" and booking["format"] == "digital":
                feature_value = "digital"

            # Calculate diversification for feature
            if df_clean.iloc[i - 1].user_id != booking.user_id or i == 0:
                list_per_feature[feature] = []
                list_per_feature_free[feature] = []
                div = 0
                qpi_point_given = False

            elif (
                feature == "type"
                and booking["subcategory"] == "SEANCE_CINE"
                and feature_value not in [np.nan]
            ):
                div_temp = 0
                for theme in feature_value:
                    div_temp = div_temp + get_diversification_feature(
                        list_per_feature[feature], theme
                    )
                div = 1 if div_temp > 0 else 0

            else:
                div = get_diversification_feature(
                    list_per_feature[feature], feature_value
                )

                # Multiplicator of the diversification
                if feature == "venue" and booking["category"] != "LIVRE":
                    multiplicator = multiplicator * 2
                if (
                    booking["booking_amount"] == 0
                    and feature_value in list_per_feature_free[feature]
                ):
                    multiplicator = multiplicator * 0
                else:
                    if feature_value in list_per_feature_free[feature]:
                        multiplicator = multiplicator * 0.5
                    if booking["booking_amount"] == 0:
                        multiplicator = multiplicator * 0.5
                        list_per_feature_free[
                            feature
                        ] = add_feature_if_feature_not_booked(
                            list_per_feature_free[feature], feature_value
                        )

            # Add feature value to the list of values booked
            if booking["booking_amount"] != 0:
                if (
                    feature == "type"
                    and booking["subcategory"] == "SEANCE_CINE"
                    and feature_value not in [np.nan]
                ):
                    for theme in feature_value:
                        list_per_feature[feature] = add_feature_if_feature_not_booked(
                            list_per_feature[feature], theme
                        )
                else:
                    list_per_feature[feature] = add_feature_if_feature_not_booked(
                        list_per_feature[feature], feature_value
                    )

            divers_per_feature[feature].append(div * multiplicator)
        # QPI
        # One point given if user did not declared the category in QPI.
        qpi_diversification = 0
        if not qpi_point_given:
            subcat = booking["subcategory"]
            if subcat not in user_qpi_subcategories:
                qpi_point_given = True
                qpi_diversification = 1
        divers_per_feature["qpi_diversification"].append(qpi_diversification)

        # Calculate delta div
        if df_clean.iloc[i - 1].user_id != booking.user_id or i == 0:
            delta_div = 1 + qpi_diversification

        else:
            delta_div = (
                sum([float(divers_per_feature[feature][-1]) for feature in features])
                + qpi_diversification
            )
        divers_per_feature["delta_diversification"].append(delta_div)
    return divers_per_feature
