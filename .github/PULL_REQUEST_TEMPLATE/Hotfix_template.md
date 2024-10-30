# Hotfix

## Describe your changes

Please include a summary of the changes:

* This PR [adds/removes/fixes/replaces] the [feature/bug/etc].
* Tag a reviewer if necessacy  @github/username

## Jira ticket number and/or notion link

JIRA-ticket_number

### Checklist before requesting a review

* [ ] I have performed a self-review of my code
* [ ] My code passes CI/CD tests
* [ ] I have made corresponding changes to the [tables documentation](https://www.notion.so/passcultureapp/Documentation-Tables-175a397a8e854ff4a55ae4f3620dbe3b)
* [ ] I have made corresponding changes to the [fields glossary](https://www.notion.so/passcultureapp/854a436a8f1541e1b6ec2a65f8bab600?v=798024ba90404b139e5a17407a3bc604)
* [ ] I have updated the dag
* [ ] I will create a review on slack. The review task should be short (<10min).

### PR title format (except for MEP)

There is a linter on the PR title format. Please respect the following format:

<details>
<summary>(ticket) type(topic): comment</summary>

* ticket surrounded by parenthesis, with optionnaly a hyphen followed by one or more digits (e.g., -1234). The first part must be one of the following strings:
  * DA
  * DE
  * AE
  * DS
  * HF
  * BSR
  * PC

* type :
The second part to specify the type of change one of the following :
  * build
  * lint
  * ci
  * docs
  * feat
  * fix
  * perf
  * refactor
  * test
  * core
  * dbt

* topic within parenthesis: 1 word e.g., (dag)

* comment: tell us your life

examples:

* :white_check_mark: (DE-124) refactor(firebase): update source field
* :x: (DE-124) refactor (firebase): update source field **(space between type and topic)**
* :x: (DE-124) airflow(firebase): update source fiedd in DAG **(wrong type)**
* :x: (DE-124) (DE-124) refactor(firebase refacto): update source field **(topic in two words)**
* :white_check_mark: (BSR) docs(github): add PR title valid format in template

</details>
