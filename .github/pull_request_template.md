# PR title format (except for MEP)

(ticket) type(topic): comment

with:

- ticket surrounded by parenthesis:
The first part must be one of the following strings: DA, DE, AE, DS, HF, BSR, PC.
Optionally, a hyphen followed by one or more digits (e.g., -1234)

- type :
The second part to specify the type of change one of the following
build, lint, ci, docs, feat, fix, perf, refactor, test, core, dbt

- topic within parenthesis: 1 word e.g., (dag)

- comment: tell us your life

examples:

- :white_check_mark: (DE-124) refactor(firebase): update source field
- :x: (DE-124) refactor (firebase): update source field
- :x: (DE-504) airflow(dbt): add operator in dbt dag
- :white_check_mark: (BSR) docs(github): add PR title valid format in template

## Select PR template in preview mode

- [Hotfix](?expand=1&template=Hotfix_template.md)

- [DA](?expand=1&template=DA_ticket_template.md)
- [DE](?expand=1&template=DE_ticket_template.md)
- [DS](?expand=1&template=DS_ticket_template.md)
- [MEP](?expand=1&template=MEP_template.md)

<!-- Markdown tips:

To tick boxe replace [ ] with [x]

 -->

 <!--
TO DO: check with repo admin : [https://passculture.atlassian.net/browse/PC-<num>](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/managing-repository-settings/configuring-autolinks-to-reference-external-resources)

JIRA-replace_with_ticket_number

[Notion-link](paste within parenthesis)
-->
