import json
from urllib.parse import urlparse, parse_qs
import requests
import pandas as pd
from tqdm import tqdm
import gcsfs


class ApiQueryError(Exception):
    pass


class QPIDownloader:
    """
    Download the initial cultural habits of customers.

    Parameters
    ----------

    api_key : str
        The typeform token to use to query the API.

    form_id : str
        The form id to fetch.

    answers_file_name : str
        The name of the file where to save the answers. (Must be .jsonl format)

    questions_file_name: str
        The name of the file where to save the questions.(Must be .csv format)


    Attributes
    ----------
    api_key : str
        The typeform token to use to query the API.

    form_id : str
        The form id to fetch.

    answers_file_name : str
        The name of the file where to save the answers.

    questions_file_name: str
        The name of the file where to save the questions.

    """

    def __init__(self, api_key, form_id, answers_file_name, questions_file_name, after):
        self.api_key = api_key
        self.form_id = form_id
        self.answers_file_name = answers_file_name
        self.questions_file_name = questions_file_name
        self.after = after

    def run(self):
        """ Main method : used to launch the download. """

        print("Fetching the answers...")
        responses = self.query_all(after=self.after)
        print(f"Got {len(responses)} answers !")

        print("Cleaning the answers...")
        clean_responses = self.clean_api_response(responses)
        print(f"Cleaned {len(clean_responses)} responses !")

        print(f"Saving the answers to {self.answers_file_name}...")
        fs = gcsfs.GCSFileSystem(project="passculture-data-ehp")
        with fs.open(self.answers_file_name, "w") as json_file:
            for item in clean_responses:
                json_file.write(json.dumps(item, ensure_ascii=False) + "\n")

        print(f"Now getting and saving the questions to {self.questions_file_name}")
        questions = self.get_form_questions()
        questions_csv = questions.to_csv(
            "gcs://" + self.questions_file_name, index=False
        )

        print("Done !")

    def query_page(self, nb_items_per_page=1000, after=None):
        """
        Typeform responses are stored in pages of {nb_items_per_page}
        items each (except possibly the last one).
        Query a page of responses, possibly submitted until a specified time.

        Parameters
        ----------
        nb_items_per_page : int, default=1000
            Number of items in each page

        before : string, default=None
            Limit request to responses submitted before a request token.

        Returns
        -------
        result : requests.models.Response
            A requests response.
        """
        request_url = f"https://api.typeform.com/forms/{self.form_id}/responses?page_size={nb_items_per_page}"
        if after is not None:
            request_url += f"&after={after}"

        result = requests.get(
            request_url, headers={"Authorization": f"Bearer {self.api_key}"}
        )
        if result.status_code != 200:
            raise ApiQueryError(
                f"Error {result.status_code} when querying typeform API on {self.form_id} form. Request was: {request_url}"
            )
        return result

    def query_all(self, nb_items_per_page=1000, after=None):
        """
        Query all typeform responses.

        Parameters
        ----------
        nb_items_per_page : int, default=1000
            The number of items per page chosen to query typeform responses.

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

    def clean_api_response(self, results):
        """
        Cleaning the responses, format each response as a dict.

        Returns
        -------

        results_clean : list[dict]
            A list of responses as dicts.
            Keys are culturalsurvey_id, form_id, landed_at,
            submitted_at, platform, and answers.
        """
        results_clean = []
        for result in results:
            user_data = {}
            try:
                user_id = result["hidden"]["userid"]
            except:
                try:
                    user_id = parse_qs(urlparse(result["metadata"]["referer"]).query)[
                        "userId"
                    ][0]
                except:
                    user_id = None

            user_data.update(
                {
                    "culturalsurvey_id": user_id,
                    "form_id": result["landing_id"],
                    "landed_at": result["landed_at"],
                    "submitted_at": result["submitted_at"],
                    "platform": result["metadata"]["platform"],
                }
            )
            user_data.update({"answers": self.extract_answers(result["answers"])})
            if user_data:
                results_clean.append(user_data)
        return results_clean

    def extract_answers(self, answers):
        """
        Utility method to extract the user answers
        from the json, handling the different question types.

        Parameters
        ----------
        answers : list[dict]
            List of user's answers

        Returns
        -------
        extracted_answers : list[dict]
            List of dicts with keys question_id, choices,choice and other
        """
        extracted_answers = []
        if answers is None:
            return {}
        for answer in answers:
            if answer["type"] == "choice":
                extracted_answers.append(
                    {
                        "question_id": answer["field"]["id"],
                        "choices": [],
                        "choice": answer["choice"]["label"],
                        "other": None,
                    }
                )
            elif answer["type"] == "choices":
                choices = []
                other = None

                if "labels" in answer["choices"]:
                    choices = answer["choices"]["labels"]
                if "other" in answer["choices"]:
                    other = answer["choices"]["other"]

                extracted_answers.append(
                    {
                        "question_id": answer["field"]["id"],
                        "choices": choices,
                        "choice": None,
                        "other": other,
                    }
                )
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

        questions["choices"] = questions.apply(
            lambda row: [c["label"] for c in row["properties"]["choices"]], axis=1
        )

        questions.drop(
            ["ref", "properties", "validations", "type"], axis=1, inplace=True
        )

        return questions
