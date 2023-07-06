import contentful
import pandas as pd
from utils import SPACE_ID, TOKEN
from datetime import datetime

contentful_modules = [
    {
        "name": "homepageNatif",
        "additional_fields": ["title", "modules"],
        "children": [],
    },
    {
        "name": "venuesPlaylist",
        "additional_fields": [
            "title",
            "venues_search_parameters",
            "display_parameters",
        ],
        "children": [
            {
                "name": "venues_search_parameters",
                "additional_fields": [
                    "title",
                    "is_geolocated",
                    "around_radius",
                    "tags",
                    "hits_per_page",
                    "venue_types",
                ],
                "type": "list",
            },
            {
                "name": "display_parameters",
                "additional_fields": ["title", "layout", "min_offers"],
                "type": "unique",
            },
        ],
    },
    {
        "name": "algolia",
        "additional_fields": [
            "title",
            "cover",
            "display_parameters",
            "algolia_parameters",
            "additional_algolia_parameters",
        ],
        "children": [
            {
                "name": "display_parameters",
                "additional_fields": ["title", "layout", "min_offers"],
                "type": "unique",
            },
            {
                "name": "algolia_parameters",
                "additional_fields": [
                    "title",
                    "is_geolocated",
                    "around_radius",
                    "tags",
                    "hits_per_page",
                    "categories",
                    "subcategories",
                    "is_thing",
                    "beginning_datetime",
                    "ending_datetime",
                    "is_event",
                    "is_duo",
                    "is_digital",
                    "newest_only",
                    "price_max",
                    "price_min",
                    "image_full_screen",
                ],
                "type": "unique",
            },
            {
                "name": "additional_algolia_parameters",
                "additional_fields": [
                    "title",
                    "is_geolocated",
                    "around_radius",
                    "tags",
                    "hits_per_page",
                    "categories",
                    "subcategories",
                    "is_thing",
                    "beginning_datetime",
                    "ending_datetime",
                    "is_event",
                    "is_duo",
                    "is_digital",
                    "newest_only",
                    "price_max",
                    "price_min",
                    "image_full_screen",
                ],
                "type": "list",
            },
        ],
    },
    {
        "name": "business",
        "additional_fields": [
            "title",
            "first_line",
            "second_line",
            "image",
            "url",
            "target_not_connected_users_only",
            "left_icon",
        ],
        "children": [],
    },
    {
        "name": "exclusivity",
        "additional_fields": [
            "title",
            "alt",
            "image",
            "offer_id",
            "display_parameters",
        ],
        "children": [
            {
                "name": "display_parameters",
                "additional_fields": ["title", "is_geolocated", "around_radius"],
                "type": "unique",
            }
        ],
    },
    {
        "name": "categoryList",
        "additional_fields": [
            "title",
            "category_block_list",
        ],
        "children": [
            {
                "name": "category_block_list",
                "additional_fields": ["title", "image", "home_entry_id"],
                "type": "list",
            }
        ],
    },
    {
        "name": "recommendation",
        "additional_fields": [
            "title",
            "display_parameters",
            "recommendation_parameters",
        ],
        "children": [
            {
                "name": "display_parameters",
                "additional_fields": ["title", "layout", "min_offers"],
                "type": "unique",
            },
            {
                "name": "recommendation_parameters",
                "additional_fields": [
                    "title",
                    "is_event",
                    "beginning_datetime",
                    "ending_datetime",
                    "categories",
                    "subcategories",
                    "price_max",
                ],
                "type": "unique",
            },
        ],
    },
    {
        "name": "thematicHighlight",
        "additional_fields": [
            "title",
            "thematic_highlight_info",
            "thematic_home_entry_id",
        ],
        "children": [
            {
                "name": "thematic_highlight_info",
                "additional_fields": ["title", "beginning_datetime", "ending_datetime"],
                "type": "unique",
            }
        ],
    },
    {
        "name": "video",
        "additional_fields": [
            "title",
            "displayed_title",
            "video_title",
            "duration_in_minutes",
            "video_publication_date",
            "youtube_video_id",
            "algolia_parameters",
            "color",
            "video_tag",
            "offer_title",
            "video_description",
        ],
        "children": [
            {
                "name": "algolia_parameters",
                "additional_fields": [
                    "title",
                    "is_geolocated",
                    "around_radius",
                    "tags",
                    "hits_per_page",
                    "categories",
                    "subcategories",
                    "is_thing",
                    "beginning_datetime",
                    "ending_datetime",
                    "is_event",
                    "is_duo",
                    "is_digital",
                    "newest_only",
                    "price_max",
                    "price_min",
                    "image_full_screen",
                ],
                "type": "unique",
            },
        ],
    },
]


class ContentfulClient:
    def __init__(self, env="testing", timeout=1) -> None:
        self.client = contentful.Client(
            SPACE_ID,  # This is the space ID. A space is like a project folder in Contentful terms
            TOKEN,  # This is the access token for this space.
            environment=env,
            timeout_s=timeout,
        )
        self.df_modules = pd.DataFrame()
        self.df_links = pd.DataFrame(columns=["parent", "child"])
        self.df_tags = pd.DataFrame(columns=["tag_id", "tag_name", "entry_id"])
        self.datetime = datetime.today()

    def add_parent_child_to_df(self, parent_id, child_id):
        values_to_add = {"parent": parent_id, "child": child_id}
        row_to_add = pd.Series(values_to_add)
        self.df_links = self.df_links.append(row_to_add, ignore_index=True)

    def add_tag_to_df(self, tag_id, tag_name, entry_id):
        values_to_add = {"tag_id": tag_id, "tag_name": tag_name, "entry_id": entry_id}
        row_to_add = pd.Series(values_to_add)
        self.df_tags = self.df_tags.append(row_to_add, ignore_index=True)

    def get_basic_fields(self, module):
        sys_fields = [
            "space",
            "id",
            "type",
            "created_at",
            "updated_at",
            "environment",
            "revision",
            "content_type",
            "locale",
        ]
        module_infos = dict()
        if "title" in module.fields():
            module_infos["title"] = module.title
        else:
            module_infos["title"] = None
        module_infos["date_imported"] = self.datetime

        contentful_tags_id = []
        contentful_tags_name = []
        for tag in module._metadata["tags"]:
            tag_name = self.client._http_get(
                self.client.environment_url(f"/tags/{tag.id}"), {}
            ).json()["name"]
            contentful_tags_id.append(tag.id)
            self.add_tag_to_df(tag.id, tag_name, module.id)
            contentful_tags_name.append(tag_name)
        module_infos["contentful_tags_id"] = str(contentful_tags_id)
        module_infos["contentful_tags_name"] = str(contentful_tags_name)

        for sys_info in sys_fields:
            if sys_info in ["space", "environment", "content_type"]:
                module_infos[f"{sys_info}"] = module.sys[sys_info].id
            else:
                module_infos[f"{sys_info}"] = module.sys[sys_info]
        return module_infos

    def add_other_fields(self, basic_fields, other_fields, module_details):
        infos_to_get = module_details["additional_fields"]
        true_fields = [field for field in other_fields]
        fields_not_taken = [x for x in true_fields if x not in infos_to_get]
        if len(fields_not_taken) > 0:
            print(
                f"WARNING fields not imported from {module_details['name']} : {fields_not_taken}"
            )
        for field in infos_to_get:
            if (
                field
                in [
                    "recommendation_parameters",
                    "display_parameters",
                    "algolia_parameters",
                ]
                and other_fields.get(field) is not None
            ):
                basic_fields[field] = str(other_fields.get(field).id)
            elif (
                field in ["additional_algolia_parameters"]
                and other_fields.get(field) is not None
            ):
                basic_fields["algolia_parameters"] = [
                    basic_fields["algolia_parameters"]
                ].append([add_algo.id for add_algo in other_fields.get(field)])
            elif (
                field in ["venues_search_parameters", "modules"]
                and other_fields.get(field) is not None
            ):
                basic_fields[f"{field}"] = str(
                    [add_algo.id for add_algo in other_fields.get(field)]
                )
            else:
                basic_fields[f"{field}"] = str(other_fields.get(field))
        return basic_fields

    def get_all_fields(self, module, module_details):
        basic_infos = self.get_basic_fields(module)
        other_infos = module.fields()
        all_infos = self.add_other_fields(basic_infos, other_infos, module_details)
        return all_infos

    def add_module_infos_to_modules_dataframe(self, module_infos):
        row_to_add = pd.Series(module_infos)
        self.df_modules = self.df_modules.append(row_to_add, ignore_index=True)

    def get_all_playlists(self):
        for module_details in contentful_modules:
            # Here we get all the modules matching the type desired
            modules = self.client.entries(
                {"content_type": module_details["name"], "include": 1, "limit": 1000}
            )
            for module in modules:
                # Get all the infos from the module and add it to the final dataframe
                all_infos = self.get_all_fields(module, module_details)
                self.add_module_infos_to_modules_dataframe(all_infos)

                # Special case for homepages where we don't unfold submodules so we need to get the child-parent relationship here
                if module_details["name"] == "homepageNatif":
                    # Get parent-child relationships
                    submodules = module.fields().get("modules")
                    for submodule in submodules:
                        self.add_parent_child_to_df(module.id, submodule.id)

                for submodule_details in module_details["children"]:
                    try:
                        if submodule_details["type"] == "unique":
                            submodules = [module.fields()[submodule_details["name"]]]
                        else:
                            submodules = module.fields()[submodule_details["name"]]
                        for submodule in submodules:
                            if submodule is not None:
                                submodule_infos = self.get_all_fields(
                                    submodule, submodule_details
                                )
                                self.add_module_infos_to_modules_dataframe(
                                    submodule_infos
                                )
                                self.add_parent_child_to_df(module.id, submodule.id)
                    except KeyError as E:
                        continue

        self.df_modules = self.df_modules.replace("None", float("nan"))
        return self.df_modules, self.df_links, self.df_tags
