import json
from typing import Any, Dict, Optional

import markdown
import pandas as pd
from jinja2 import Environment, FileSystemLoader, nodes
from jinja2.ext import Extension

DBT_MANIFEST = "orchestration/dags/data_gcp_dbt/target/manifest.json"


class DBTDocsParser:
    """
    Parses the DBT manifest.json file to extract documentation for models and their columns.
    """

    def __init__(
        self,
        manifest_path: str = DBT_MANIFEST,
        prefix_project: str = "model.data_gcp_dbt",
    ):
        self.dbt_prefix_project = prefix_project
        self.json = self._load_manifest(manifest_path)

    def _load_manifest(self, path: str) -> Dict[str, Any]:
        """Load the DBT manifest JSON file."""
        with open(path) as f:
            return json.load(f)

    def get_model_documentation(self, model_name: str) -> str:
        """
        Generate markdown documentation for a DBT model's columns.

        Args:
            model_name (str): The DBT model name.

        Returns:
            str: A markdown table with column documentation.
        """
        model_uri = f"{self.dbt_prefix_project}.{model_name}"
        model_node = self.json.get("nodes", {}).get(model_uri, {})
        model_columns = model_node.get("columns", {})

        data = [
            {
                "name": col["name"],
                "data_type": col["data_type"],
                "description": col["description"],
            }
            for col in model_columns.values()
        ]
        df = pd.DataFrame(data)
        return df.to_markdown(index=False)


class DocsStatementExtension(Extension):
    """
    Jinja2 extension to render custom DBT documentation statements in templates.
    """

    tags = {"docs"}
    dbt_parser = DBTDocsParser()

    def parse(self, parser) -> nodes.CallBlock:
        lineno = next(parser.stream).lineno
        token = next(parser.stream).value
        arg = nodes.Const(token)
        body = parser.parse_statements(["name:enddocs"], drop_needle=True)

        return nodes.CallBlock(
            self.call_method("_render_custom_statement", [arg]), [], [], body
        ).set_lineno(lineno)

    def _render_custom_statement(self, arg: Optional[str], caller) -> str:
        """
        Render documentation based on the tag argument (column or table).

        Args:
            arg (Optional[str]): Argument indicating column or table reference.
            caller: Function to fetch content between `{% docs %}` and `{% enddocs %}` tags.

        Returns:
            str: Rendered markdown content.
        """
        definition = caller()

        if arg and isinstance(arg, str):
            if arg.startswith("column__"):
                column_name = arg.split("__", 1)[1]
                return markdown.markdown(f"**{column_name}**: {definition}")
            elif arg.startswith("table__"):
                model_name = arg.split("__", 1)[1]
                return self.dbt_parser.get_model_documentation(model_name)
        return markdown.markdown(definition)


class HideStatementExtension(Extension):
    """
    Jinja2 extension to hide statements in templates.
    """

    tags = {"hide"}

    def parse(self, parser) -> nodes.CallBlock:
        lineno = next(parser.stream).lineno
        token = next(parser.stream).value
        arg = nodes.Const(token)
        body = parser.parse_statements(["name:endhide"], drop_needle=True)

        return nodes.CallBlock(
            self.call_method("_render_custom_statement", [arg]), [], [], body
        ).set_lineno(lineno)

    def _render_custom_statement(self, arg: Optional[str], caller) -> str:
        """
        Render documentation based on the tag argument (column or table).

        Args:
            arg (Optional[str]): Argument indicating column or table reference.
            caller: Function to fetch content between `{% hide %}` and `{% endhide %}` tags.

        Returns:
            str: Rendered markdown content.
        """
        return markdown.markdown("")


class DocsBuilder:
    """
    Class to handle DBT documentation build processes including setup, markdown processing, and file copying.
    """

    def __init__(self):
        self.env: Optional[Environment] = None

    def configure_environment(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Configure the Jinja2 environment.

        Args:
            config (Dict[str, Any]): Configuration dictionary.

        Returns:
            Dict[str, Any]: Updated configuration with Jinja2 environment.
        """
        self.env = Environment(loader=FileSystemLoader(config["docs_dir"]))
        self.env.add_extension(DocsStatementExtension)
        self.env.add_extension(HideStatementExtension)
        return config

    def render_markdown(self, markdown_content: str) -> str:
        """
        Render markdown content using Jinja2 templates.

        Args:
            markdown_content (str): Raw markdown content.

        Returns:
            str: Rendered markdown content.
        """
        if self.env:
            template = self.env.from_string(markdown_content)
            return template.render()
        return markdown_content

    def pre_build_setup(self) -> None:
        """Prepare environment before building documentation."""
        pass


# Instantiate DocsBuilder and setup hooks for documentation generation
docs_builder = DocsBuilder()


def on_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Hook to configure environment."""
    return docs_builder.configure_environment(config)


def on_page_markdown(markdown_content: str, **kwargs) -> str:
    """Hook to render page markdown."""
    return docs_builder.render_markdown(markdown_content)


def on_pre_build(config: Dict[str, Any], **kwargs) -> None:
    """Hook to execute pre-build actions."""
    docs_builder.pre_build_setup()
