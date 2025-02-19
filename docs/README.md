# Documentation Guide

This guide covers everything from launching MkDocs locally to writing documentation for dbt models and columns. Let's get started!

## Table of Contents

- [Introduction](#introduction)
- [Quick Start](#quick-start)
- [Launching MkDocs Locally](#launching-mkdocs-locally)
- [Project Structure](#project-structure)
- [Writing dbt Documentation](#writing-dbt-documentation)
  - [Defining Columns](#defining-columns)
  - [Defining Models](#defining-models)
  - [Hiding Fields](#hiding-fields)
- [Writing Team Documentation](#writing-team-documentation)
- [Markdown Tips](#markdown-tips)
- [Additional Notes](#additional-notes)

## Introduction

Our documentation is powered by **MkDocs** with the **Material** theme. We use **Markdown** for writing content, and the documentation is hosted on **GitHub Pages**. Integration with dbt documentation is achieved using symbolic links (**symlinks**). We also utilize custom **Jinja** properties and hooks to compile the project, along with custom MkDocs libraries to auto-generate distinct Markdown files.

## Quick Start

- **Documentation Generator**: MkDocs + Material theme.
- **Content Format**: Markdown.
- **Hosting Platform**: GitHub Pages.
- **Integration Method**: Symlinks for linking dbt documentation.
- **Customization**: Custom Jinja properties and hooks for compilation.
- **Automation**: Custom MkDocs libraries for auto-generating `.md` files.

## Launching MkDocs Locally

To view and edit the documentation locally, follow these steps:

### 1. Install Dependencies

Run the following command to install all necessary dependencies:

```bash
make install_analytics
```

### 2. Set Up the dbt Project

Ensure the dbt project is installed and the models are built. If not, execute:

```bash
export DBT_PROJECT_DIR=orchestration/dags/data_gcp_dbt
export DBT_PROFILES_DIR=orchestration/dags/data_gcp_dbt
dbt deps
dbt compile
```

### 3. Run MkDocs

Start the MkDocs server with:

```bash
make docs_run
```

### 4. Access the Documentation

Open your web browser and navigate to:

> [http://127.0.0.1:8000/data-gcp/](http://127.0.0.1:8000/data-gcp/)

You should now see the documentation running locally.

## Project Structure

Our documentation's structure is defined by `mkdocs.yml` and mirrored in the `docs/` directory:

- **`assets/`**: Contains custom CSS, HTML, and JavaScript files for styling and functionality.
- **`dbt/`**: A [symlink](https://en.wikipedia.org/wiki/Symbolic_link) to `orchestration/dags/data_gcp_dbt/models/_dbt_docs`. This integrates dbt documentation.
  - *See*: [Writing dbt Documentation](#writing-dbt-documentation)
- **`team/`**: Contains team-related documentation.
  - **`ds/`**: Data Science documentation.
  - **`de/`**: Data Engineering documentation.
  - **`da/`**: Data Analytics documentation.
  - *See*: [Writing Team Documentation](#writing-team-documentation)

## Writing dbt Documentation

Our dbt documentation within MkDocs includes **Glossary terms** and **model definitions**. We primarily focus on `mart` concepts. This documentation is also utilized by `dbt docs`, which provides lineage, tests, and technical details for the data engineering team.

### Defining Columns

Columns may appear in multiple tables, but their definitions should be centralized. Define each column in the Glossary within the relevant Markdown file.

#### Syntax

To define a column, use the following format:

```markdown
{% docs column__<column_name> %}
<column-description>
{% enddocs %}
```

#### Example

In `column__stock_id.md`:

```markdown
{% docs column__stock_id %}
Unique identifier for the stock.
{% enddocs %}
```

#### Explanation

- **`{% docs column__stock_id %}`**: Begins the documentation block for `stock_id`.
- **`Unique identifier for the stock.`**: Description of the column.
- **`{% enddocs %}`**: Ends the documentation block.

#### Notes

- **`column`**: Indicates a column definition.
- **`__`**: Separator between tags.
- **`stock_id`**: Name of the column.

### Defining Models

In dbt, a model includes descriptions, types, tests, and columns. In MkDocs, a model page explains the table's objective and key concepts.

#### Template

Each model should follow this structure:

```markdown
---
title: <Page Title in MkDocs>
description: <Page Description>
---

{% docs description__<model_name> %}
Brief description for dbt docs and BigQuery schema metadata.
{% enddocs %}

## Key Concepts

Detailed explanation of the table's key concepts.

## Table Description

Automatically generated schema and columns documentation.

{% docs table__<model_name> %}{% enddocs %}
```

#### Explanation

- **Front Matter (`---`)**: Sets the page title and description for MkDocs.
- **`{% docs description__<model_name> %}`**: Provides a brief description for dbt docs and BigQuery.
- **`## Key Concepts`**: Section for detailed explanations in MkDocs.
- **`{% docs table__<model_name> %}`**: Generates the table schema and columns documentation.

### Hiding Fields

If you need to include notes or content that should not be displayed publicly but still remain in the code, use:

```markdown
{% hide columns %}
Content to be hidden from MkDocs.
{% endhide columns %}
```

This will prevent the enclosed content from appearing in MkDocs but keep it available for dbt documentation or future reference.

## Writing Team Documentation

Team documentation is organized within the `team/` directory and divided into subfolders:

- **`ds/`**: Data Science documentation.
- **`de/`**: Data Engineering documentation.
- **`da/`**: Data Analytics documentation.

Each team should create Markdown files within their respective folders, following standard Markdown practices and leveraging MkDocs features for consistency and clarity.

## Markdown Tips

Enhance your documentation by utilizing advanced Markdown features and MkDocs extensions:

- **MkDocs Material Features**: Explore the [MkDocs Material](https://squidfunk.github.io/mkdocs-material/) documentation for components like tabs, callouts, and more.
- **MkDocs Plugins**: Browse the [MkDocs Plugin Catalog](https://github.com/mkdocs/catalog) for plugins that add functionality like code highlighting, diagrams, and search enhancements.

## Additional Notes

### Creating Symlinks

To link the dbt documentation into MkDocs, create a symlink:

```bash
ln -s ../orchestration/dags/data_gcp_dbt/models/_dbt_docs docs/dbt
```

This command links the dbt docs directory to `docs/dbt`, ensuring that dbt documentation is included in the MkDocs site.

---
