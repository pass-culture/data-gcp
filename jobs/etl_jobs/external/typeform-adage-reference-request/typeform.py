import requests
import pandas as pd
from tqdm import tqdm
import utils


class ApiQueryError(Exception):
    pass


class TypeformDownloader:
    def __init__(self, api_key, form_id):
        self.api_key = api_key
        self.form_id = form_id

    def define_extra_answer_columns(self, questions_df):
        columns = (
            questions_df[["question_id", "title", "type"]]
            .to_dict(orient="index")
            .values()
        )

        out_cols = {}
        for _d in columns:
            out_cols[_d["question_id"]] = utils.clean_question(_d["title"])
        return out_cols

    def run(self):
        print("Fetching the answers...")
        responses = self.query_all()
        questions = self.get_form_questions()
        columns_question_def = self.define_extra_answer_columns(questions)
        clean_responses = self.clean_api_response(responses, columns_question_def)
        return clean_responses

    def query_page(self, nb_items_per_page=1000, after=None):
        """
        Typeform responses are stored in pages of {nb_items_per_page}
        items each (except possibly the last one).
        Query a page of responses, possibly submitted until a specified time.

        Parameters
        ----------
        nb_items_per_page : int, default=1000
            Number of items in each page

        after : string, default=None
            Limit request to responses submitted after the specified token.

        Returns
        -------
        result : requests.models.Response
            A requests response.
        """
        request_url = f"https://api.typeform.com/forms/{self.form_id}/responses?page_size={nb_items_per_page}"
        if after is not None:
            request_url += f"&before={after}"

        result = requests.get(
            request_url, headers={"Authorization": f"Bearer {self.api_key}"}
        )
        if result.status_code != 200:
            raise ApiQueryError(
                f"Error {result.status_code} when querying typeform API on {self.form_id} form."
            )
        return result

    def query_all(self, nb_items_per_page=200, after=None):
        """
        Query all typeform responses.

        Parameters
        ----------
        nb_items_per_page : int, default=1000
            The number of items per page chosen to query typeform responses.

        after : string, default=None
            Limit request to responses submitted after the specified token.
        Returns
        -------
        items : List[dict]
            The list of responses.
        """
        data = self.query_page(nb_items_per_page=nb_items_per_page, after=after).json()
        items = data["items"]
        page_count = data["page_count"]

        progress_bar = tqdm(total=page_count)

        while page_count > 1:
            data = self.query_page(
                nb_items_per_page=nb_items_per_page, after=items[-1].get("token")
            ).json()
            new_items = data["items"]
            items.extend(new_items)

            page_count = data["page_count"]
            progress_bar.update(1)

        return items

    def clean_api_response(self, results, columns_question_def):

        results_clean = []
        for result in results:
            answers = self.extract_answers(result["answers"])
            user_data = {
                "form_id": result["landing_id"],
                "landed_at": result["landed_at"],
                "submitted_at": result["submitted_at"],
                "platform": result["metadata"]["platform"],
                "answers": answers,
            }

            for answer in answers:
                _id = answer["question_id"]
                naming = columns_question_def.get(_id)
                if naming and "value" in answer:
                    user_data[naming] = str(answer["value"])
            if user_data:
                results_clean.append(user_data)

        return results_clean

    def extract_answers(self, answers):

        extracted_answers = []
        if answers is None:
            return {}
        for i, answer in enumerate(answers):
            output = {
                "question_id": answer["field"]["id"],
                "question_number": i + 1,
                "choice_type": answer["type"],
            }
            choice_type = answer["type"]
            _dict = answer[choice_type]
            if choice_type in ["choice", "choices"]:
                if "label" in _dict:
                    output["value"] = _dict["label"]
                if "labels" in _dict:
                    output["value"] = "/".join(_dict["labels"])
                if "other" in answer[choice_type]:
                    output["value"] = _dict["other"]

            else:
                output["value"] = _dict

            extracted_answers.append(output)
        return extracted_answers

    def get_form_questions(self):
        """
        The questions are not stored with the responses but with the form itself.
        This method gets the form details and returns the questions ids, text and choices.

        Returns
        -------

        questions : pandas.DataFrame
            A dataframe with questions ids, text and choices.
        """

        headers = {"Authorization": f"Bearer {self.api_key}"}
        request_url = f"https://api.typeform.com/forms/{self.form_id}"

        form = requests.get(request_url, headers=headers).json()

        questions = pd.DataFrame(form.get("fields")).rename(
            columns={"id": "question_id"}
        )
        questions.title = questions.title.apply(
            lambda x: x.replace("\n", " ").replace("  ", " ")
        )
        questions["choices"] = questions.apply(
            lambda row: [c["label"] for c in row["properties"].get("choices", [])],
            axis=1,
        )

        questions.drop(["ref", "properties", "validations"], axis=1, inplace=True)

        return questions
