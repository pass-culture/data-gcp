# pylint: disable=invalid-name
import datetime
import json
import pytz
from pcreco.core.model_selection.model_configuration import ModelConfiguration
from pcreco.core.user import User
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.model_selection import select_reco_model_params
from sqlalchemy import text
from pcreco.utils.db.db_connection import get_session
from pcreco.core.model_engine import ModelEngine


class Recommendation(ModelEngine):
    def get_model_configuration(
        self, user: User, params_in: PlaylistParamsIn
    ) -> ModelConfiguration:
        model_params, reco_origin = select_reco_model_params(
            params_in.model_endpoint, user
        )
        self.reco_origin = reco_origin
        return model_params

    def save_recommendation(self, recommendations) -> None:
        if len(recommendations) > 0:
            date = datetime.datetime.now(pytz.utc)
            rows = []

            for offer_id in recommendations:
                rows.append(
                    {
                        "user_id": self.user.id,
                        "offer_id": offer_id,
                        "date": date,
                        "group_id": self.model_params.name,
                        "reco_origin": self.reco_origin,
                        "model_name": self.scorer.retrieval_endpoints[
                            0
                        ].model_display_name,
                        "model_version": self.scorer.retrieval_endpoints[
                            0
                        ].model_version,
                        "reco_filters": json.dumps(self.params_in.json_input),
                        "call_id": self.user.call_id,
                        "user_iris_id": self.user.iris_id,
                    }
                )

            connection = get_session()
            connection.execute(
                text(
                    """
                    INSERT INTO public.past_recommended_offers (userid, offerid, date, group_id, reco_origin, model_name, model_version, reco_filters, call_id, user_iris_id)
                    VALUES (:user_id, :offer_id, :date, :group_id, :reco_origin, :model_name, :model_version, :reco_filters, :call_id, :user_iris_id)
                    """
                ),
                rows,
            )
