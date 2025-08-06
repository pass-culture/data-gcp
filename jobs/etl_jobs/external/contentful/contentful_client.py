from datetime import datetime

import contentful
import pandas as pd

from utils import SPACE_ID

BLOCK_PARAMETERS = {
    "algolia_parameters": {
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
    "additional_algolia_parameters": {
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
    "display_parameters": {
        "name": "display_parameters",
        "additional_fields": ["title", "layout", "min_offers", "is_geolocated"],
        "type": "unique",
    },
    "venues_search_parameters": {
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
    "recommendation_parameters": {
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
    "category_block_list": {
        "name": "category_block_list",
        "additional_fields": ["title", "image", "home_entry_id"],
        "type": "list",
    },
    "thematic_highlight_info": {
        "name": "thematic_highlight_info",
        "additional_fields": ["title", "beginning_datetime", "ending_datetime"],
        "type": "unique",
    },
    "video_carousel_item": {
        "name": "items",
        "additional_fields": [
            "title",
            "duration_in_minutes",
            "youtube_video_id",
            "offer_id",
            "tag",
            "home_entry_id",
        ],
        "type": "list",
    },
    "trend_block": {
        "name": "items",
        "additional_fields": ["title", "image", "home_entry_id"],
        "type": "list",
    },
}

CONTENTFUL_MODULES = [
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
            BLOCK_PARAMETERS["venues_search_parameters"],
            BLOCK_PARAMETERS["display_parameters"],
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
            BLOCK_PARAMETERS["display_parameters"],
            BLOCK_PARAMETERS["algolia_parameters"],
            BLOCK_PARAMETERS["additional_algolia_parameters"],
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
            BLOCK_PARAMETERS["display_parameters"],
        ],
    },
    {
        "name": "highlightOffer",
        "additional_fields": [
            "offer_title",
            "highlight_title",
            "is_geolocated",
            "around_radius",
            "offer_image",
            "offer_tag",
            "offer_id",
        ],
        "children": [],
    },
    {
        "name": "categoryList",
        "additional_fields": [
            "title",
            "category_block_list",
        ],
        "children": [BLOCK_PARAMETERS["category_block_list"]],
    },
    {
        "name": "recommendation",
        "additional_fields": [
            "title",
            "display_parameters",
            "recommendation_parameters",
        ],
        "children": [
            BLOCK_PARAMETERS["display_parameters"],
            BLOCK_PARAMETERS["recommendation_parameters"],
        ],
    },
    {
        "name": "thematicHighlight",
        "additional_fields": [
            "title",
            "thematic_highlight_info",
            "thematic_home_entry_id",
        ],
        "children": [BLOCK_PARAMETERS["thematic_highlight_info"]],
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
            BLOCK_PARAMETERS["algolia_parameters"],
        ],
    },
    {
        "name": "trends",
        "additional_fields": ["title", "items"],
        "children": [
            BLOCK_PARAMETERS["trend_block"],
        ],
    },
    {
        "name": "videoCarousel",
        "additional_fields": ["title", "color", "items"],
        "children": [
            BLOCK_PARAMETERS["video_carousel_item"],
        ],
    },
    {
        "name": "gtlPlaylist",
        "additional_fields": ["title", "algolia_parameters", "display_parameters"],
        "children": [
            BLOCK_PARAMETERS["display_parameters"],
            BLOCK_PARAMETERS["algolia_parameters"],
        ],
    },
]


class ContentfulClient:
    def __init__(self, config_env, playlists_names=[], timeout=1) -> None:
        self.client = contentful.Client(
            SPACE_ID,  # This is the space ID. A space is like a project folder in Contentful terms
            access_token=config_env[
                "access_token"
            ],  # This is the access token for this space.
            api_url=config_env["api_url"],
            environment=config_env["env"],
            timeout_s=timeout,
        )
        self.df_modules = pd.DataFrame()
        self.df_links = pd.DataFrame(columns=["parent", "child"])
        self.df_tags = pd.DataFrame(columns=["tag_id", "tag_name", "entry_id"])
        self.datetime = datetime.today()
        self.page_size = 500
        self.contentful_modules = self.set_contentful_modules(playlists_names)

    def set_contentful_modules(self, playlists_names):
        return (
            CONTENTFUL_MODULES
            if not playlists_names
            else [
                module
                for module in CONTENTFUL_MODULES
                if module["name"] in playlists_names
            ]
        )

    def add_parent_child_to_df(self, parent_id, child_id):
        self.df_links = pd.concat(
            [self.df_links, pd.DataFrame([{"parent": parent_id, "child": child_id}])],
            ignore_index=True,
        )

    def add_tag_to_df(self, tag_id, tag_name, entry_id):
        self.df_tags = pd.concat(
            [
                self.df_tags,
                pd.DataFrame(
                    [{"tag_id": tag_id, "tag_name": tag_name, "entry_id": entry_id}]
                ),
            ],
            ignore_index=True,
        )

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
                field in ["venues_search_parameters", "modules", "items"]
                and other_fields.get(field) is not None
            ):
                basic_fields[f"{field}"] = str(
                    [add_algo.id for add_algo in other_fields.get(field)]
                )
            elif field in ["duration_in_minutes"]:
                basic_fields[f"{field}"] = float(other_fields.get(field, 0))
            elif field in ["video_publication_date"]:
                basic_fields[f"{field}"] = other_fields.get(field)
            else:
                basic_fields[f"{field}"] = str(other_fields.get(field))
        return basic_fields

    def get_all_fields(self, module, module_details):
        basic_infos = self.get_basic_fields(module)
        other_infos = module.fields()
        all_infos = self.add_other_fields(basic_infos, other_infos, module_details)
        return all_infos

    def add_module_infos_to_modules_dataframe(self, module_infos):
        self.df_modules = pd.concat(
            [self.df_modules, pd.DataFrame([module_infos])], ignore_index=True
        )

    def get_paged_modules(self, module_details):
        content_type = module_details["name"]

        try:
            # Set initial query parameters
            query = {
                "content_type": content_type,
                "include": 1,
                "order": "sys.updatedAt",
                "limit": self.page_size,
                "skip": 0,
            }
            all_entries = []

            # Retrieve the total number of entries
            num_entries = self.client.entries(
                {"content_type": content_type, "limit": 1, "include": 1}
            ).total
            print(f"Found {num_entries} for {content_type}")

            # Iterate through pages
            for i in range((num_entries // self.page_size) + 1):
                query["skip"] = i * self.page_size
                page = self.client.entries(query)
                all_entries.extend(page)

            print(f"Retrieved {len(all_entries)} entries")
            return all_entries

        except Exception as e:
            if "BadRequestError: HTTP status code: 400" in str(e):
                # Content type doesn't exist or has bad config - skip it
                print(f"SKIP: {content_type} (HTTP 400 - not accessible)")
                print(f"  {e}")
                return []
            else:
                # Some other error - show full details
                print(f"ERROR: {content_type} failed:")
                print(f"  {e}")
                return []

    def get_playlists(self):
        for module_def in self.contentful_modules:
            for entry in self.get_paged_modules(module_def):
                self._process_module(entry, module_def)

        # final clean-up
        self.df_modules = self.df_modules.replace([None, "None"], float("nan"))
        return self.df_modules, self.df_links, self.df_tags

    def _process_module(self, module, module_def):
        # 1) core fields: extract and store the module itself
        infos = self.get_all_fields(module, module_def)
        self.add_module_infos_to_modules_dataframe(infos)

        # 2) flat homepage modules: special case for homepageNatif
        if module_def["name"] == "homepageNatif":
            for child in module.fields().get("modules", []):
                self.add_parent_child_to_df(module.id, child.id)

        # 3) recursive children: for each declared child block, fetch and store submodules
        for child_def in module_def.get("children", []):
            try:
                subs = module.fields()[child_def["name"]]
            except KeyError:
                # this module has no such field → skip
                continue

            # normalize to a list if it's declared "unique", else ensure it's a list or empty
            subs = [subs] if child_def.get("type") == "unique" else (subs or [])

            for sub in subs:
                if sub is None:
                    continue
                # extract and store each submodule’s fields
                sub_infos = self.get_all_fields(sub, child_def)
                self.add_module_infos_to_modules_dataframe(sub_infos)
                # record the parent→child link
                self.add_parent_child_to_df(module.id, sub.id)
