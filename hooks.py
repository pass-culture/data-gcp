from jinja2 import Environment, nodes, FileSystemLoader
from jinja2.ext import Extension
import pandas as pd
import markdown
import shutil
import os
import json

BASE_FOLDER = "docs/dbt"
DBT_COPY_HOOK = {
    "glossary" : {
        "type" : "folder",
        "source" : "orchestration/dags/data_gcp_dbt/models/_documentation/",
        "destination" : "docs/dbt/"
    },
}
DBT_MANIFEST = "orchestration/dags/data_gcp_dbt/target/manifest.json"


class DBTDocsParser:
    def __init__(self):
        with open(DBT_MANIFEST) as f:
            self.json = json.load(f)
        
        self.dbt_prefix_project = "model.data_gcp_dbt"
    
    def get_model_documentation(self, model_name: str):
        model_uri = f"{self.dbt_prefix_project}.{model_name}"
        model_node = self.json.get("nodes", {}).get(model_uri, {})
        model_columns = model_node.get("columns", {})
        data = []
        for k,v in model_columns.items():
            data.append({"name": v['name'], "data_type" : v['data_type'], "description": v['description']})
        df = pd.DataFrame(data)
        return df.to_markdown(index=False)

            
        

class DocsStatementExtension(Extension):
    tags = {"docs"}
    dbt_parser = DBTDocsParser()
    

    def parse(self, parser):
        # Capture the line number for potential error reporting
        lineno = next(parser.stream).lineno
        token = next(parser.stream).value
        arg = nodes.Const(token)
        # Parse the content/body between {% docs %} and {% enddocs %}
        body = parser.parse_statements(['name:enddocs'], drop_needle=True)

        # Return a call to _render_custom_statement with parsed args and body
        return nodes.CallBlock(
            self.call_method("_render_custom_statement", [arg]),
            [],  # No positional arguments
            [],  # No keyword arguments
            body  # Content between `{% docs %}...{% enddocs %}`
        ).set_lineno(lineno)

    def _render_custom_statement(self, arg, caller):
        #
        # `arg` is the identifier, `caller()` is the content between the tags
        definition = caller()  # Get the content between {% docs %}...{% enddocs %}

        # Handle cases where arg may be undefined or None
        if arg and isinstance(arg, str) and arg.startswith("column__"):
            # Extract the part after "column__" if it matches the pattern
            column_name = arg.split("__", 1)[1]
            formatted_output = markdown.markdown(f"**{column_name}**: {definition}")
        elif arg and isinstance(arg, str) and arg.startswith("table__"):
            model_name = arg.split("__", 1)[1]
            formatted_output = self.dbt_parser.get_model_documentation(model_name)
        else:
            formatted_output = markdown.markdown(f"{definition}")
        # Render the output as Markdown
        
        return formatted_output

# Global Jinja environment
env = None

def on_config(config):
    global env
    env = Environment(loader=FileSystemLoader(config["docs_dir"]))
    env.add_extension(DocsStatementExtension)
    return config

def on_page_markdown(markdown_content, **kwargs):
    template = env.from_string(markdown_content)
    return template.render()

def on_pre_build(config, **kwargs) -> None:
    copy_dbt()
    
def copy_dbt():
    for key, params in DBT_COPY_HOOK.items():
        source = params['source']
        destination = params['destination']
        type = params['type']
        os.makedirs(destination, exist_ok=True)
        if type == "file":
            shutil.copy(source, destination)
        elif type == "folder":
            shutil.copytree(source, destination, dirs_exist_ok=True)