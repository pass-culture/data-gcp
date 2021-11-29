import contentful
from utils import SPACE_ID, TOKEN
from datetime import datetime


class ContentfulClient:
    def __init__(self, env="testing", timeout=1) -> None:
        self.client = contentful.Client(
            SPACE_ID,  # This is the space ID. A space is like a project folder in Contentful terms
            TOKEN,  # This is the access token for this space.
            environment=env,
            timeout_s=timeout,
        )

    def get_algolia_modules(self):
        algolia_modules = self.client.entries(
            {"content_type": "algolia", "include": 1, "limit": 1000}
        )
        print(f"Found {len(algolia_modules)} modules !")
        print("Processing modules...")
        tags = []
        playlists = []
        for module in algolia_modules:
            playlist = dict()
            try:
                algolia_parameters = module.algolia_parameters.fields()
                display_parameters = module.display_parameters.fields()
            except AttributeError:
                continue
            if algolia_parameters.get("tags") is not None:
                playlist["tag"] = module.algolia_parameters.tags[0]
                if playlist["tag"] in tags:
                    print(
                        f"Duplicates for tag : {playlist['tag']}, keeping last version."
                    )
                else:
                    playlist["module_id"] = module.id
                    playlist["module_type"] = module.content_type.resolve(
                        self.client
                    ).name
                    # Algolia parameters
                    playlist["title"] = algolia_parameters.get("title")
                    playlist["is_geolocated"] = algolia_parameters.get("is_geolocated")
                    playlist["around_radius"] = algolia_parameters.get("around_radius")
                    playlist["categories"] = str(algolia_parameters.get("categories"))
                    playlist["is_digital"] = algolia_parameters.get("is_digital")
                    playlist["is_thing"] = algolia_parameters.get("is_thing")
                    playlist["is_event"] = algolia_parameters.get("is_event")
                    playlist["beginning_datetime"] = algolia_parameters.get(
                        "beginning_datetime"
                    )
                    playlist["ending_datetime"] = algolia_parameters.get(
                        "ending_datetime"
                    )
                    playlist["is_free"] = algolia_parameters.get("is_free")
                    playlist["price_min"] = algolia_parameters.get("price_min")
                    playlist["price_max"] = algolia_parameters.get("price_max")
                    playlist["is_duo"] = algolia_parameters.get("is_duo")
                    playlist["newest_only"] = algolia_parameters.get("newest_only")
                    playlist["hits_per_page"] = algolia_parameters.get("hits_per_page")
                    # Display parameters
                    playlist["layout"] = display_parameters.get("layout")
                    playlist["min_offers"] = display_parameters.get("min_offers")

                    playlist["date_updated"] = datetime.today()

                    if module.fields().get("additional_algolia_parameters") is not None:
                        try:
                            playlist["child_playlists"] = str(
                                [
                                    add.tags[0]
                                    for add in module.additional_algolia_parameters
                                ]
                            )
                        except AttributeError as error:
                            print(f"Error: no tags in additional_algolia_parameters :{error}")
                    else:
                        playlist["child_playlists"] = None

                    tags.append(playlist["tag"])
                    playlists.append(playlist)

        print(f"Processed {len(playlists)} playlists !")
        return playlists
