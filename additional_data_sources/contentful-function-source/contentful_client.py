import contentful
import pandas as pd
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

    def add_parent_child_to_df(self, df, parent_id, child_id):
        values_to_add = {'parent': parent_id, 'child': child_id}
        row_to_add = pd.Series(values_to_add)
        df = df.append(row_to_add, ignore_index=True)
        return df

    def get_basic_fields(self, module):
        module_infos = dict()
        module_infos["id"] = module.id
        module_infos["title"] = module.title
        module_infos["date_updated"] = datetime.today()
        for sys_info in module.sys:
            if sys_info in ['space', 'environment', 'content_type']:
                module_infos[f"sys_{sys_info}"] = module.sys[sys_info].id
            else:
                module_infos[f"sys_{sys_info}"] = module.sys[sys_info]
        return module_infos

    def add_other_fields(self, basic_fields, other_fields):
        if other_fields.get("tags") is not None:
            other_fields["tag"] = other_fields.tags[0]
            del other_fields['tags']
        if other_fields.get("image_full_screen") is not None:
            print(other_fields["image_full_screen"])
            print(other_fields["image_full_screen"].get('fields'))
            other_fields["image_full_screen"] = other_fields["image_full_screen"].url
        all_fields = {**basic_fields, **other_fields}
        return all_fields

    def get_homepages(self):
        homepages = self.client.entries({"content_type": "homepageNatif"})
        df_homepages = pd.DataFrame()
        df_links = pd.DataFrame(columns=['parent', 'child'])
        for homepage in homepages:
            homepage_infos = self.get_basic_fields(homepage)
            row_to_add = pd.Series(homepage_infos)
            df_homepages = df_homepages.append(row_to_add, ignore_index=True)

            # Get parent-child relationships
            modules = homepage.fields().get("modules")
            for module in modules:
                df_links = self.add_parent_child_to_df(df_links, homepage.id, module.id)

        return df_homepages, df_links

    def get_algolia_modules(self):
        algolia_modules = self.client.entries(
            {"content_type": "algolia", "include": 1, "limit": 1000}
        )
        df_modules = pd.DataFrame()
        df_links = pd.DataFrame(columns=['parent', 'child'])

        for module in algolia_modules:
            module_infos = self.get_basic_fields(module)
            row_to_add = pd.Series(module_infos)
            df_modules = df_modules.append(row_to_add, ignore_index=True)

            try:
                algolia_parameters = module.algolia_parameters
                basic_infos = self.get_basic_fields(algolia_parameters)
                other_infos = algolia_parameters.fields()
                all_infos = self.add_other_fields(basic_infos, other_infos)
                row_to_add = pd.Series(all_infos)
                df_modules = df_modules.append(row_to_add, ignore_index=True)
                df_links = self.add_parent_child_to_df(df_links, module.id, algolia_parameters.id)
            except AttributeError:
                continue

            try:
                display_parameters = module.display_parameters
                basic_infos = self.get_basic_fields(display_parameters)
                other_infos = display_parameters.fields()
                all_infos = self.add_other_fields(basic_infos, other_infos)
                row_to_add = pd.Series(all_infos)
                df_modules = df_modules.append(row_to_add, ignore_index=True)
                df_links = self.add_parent_child_to_df(df_links, module.id, display_parameters.id)
            except AttributeError:
                continue

            try:
                additionals_algolia_parameters = module.additional_algolia_parameters
                for additional_parameters in additionals_algolia_parameters:
                    basic_infos = self.get_basic_fields(additional_parameters)
                    other_infos = additional_parameters.fields()
                    all_infos = self.add_other_fields(basic_infos, other_infos)
                    row_to_add = pd.Series(all_infos)
                    df_modules = df_modules.append(row_to_add, ignore_index=True)
                    df_links = self.add_parent_child_to_df(df_links, module.id, additional_parameters.id)
            except AttributeError:
                continue

        return df_modules, df_links

    def get_all_contentful(self):
        homepages, homepages_links = self.get_homepages()
        algolia_modules, algolia_links = self.get_algolia_modules()
        all_modules = [homepages, algolia_modules]
        contentful_modules = pd.concat(all_modules)
        all_links = [homepages_links, algolia_links]
        print(homepages_links.shape)
        print(algolia_links.shape)
        contentful_links = pd.concat(all_links)

        return contentful_modules, contentful_links
