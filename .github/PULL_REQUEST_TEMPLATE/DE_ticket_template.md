# DE PR

## Describe your changes

Please include a summary of the changes:

* This PR [adds/removes/fixes/replaces] the [feature/bug/etc].
* Tag a reviewer if necessacy  @github/username

## Jira ticket number and/or notion link

JIRA-ticket_number

### Type of change

* [ ] Fix (non-breaking change which corrects expected behavior)
* [ ] New fields (non-breaking change)
* [ ] New table (non-breaking change)
* [ ] Concept change (potentially breaking change which modifies fields according to new or evolving business concepts)
* [ ] Table deletion (potentially breaking change which adds functionality/ table)

### Checklist before requesting a review

* [ ] I have performed a self-review of my code

* [ ] My code passes CI/CD tests
* [ ] I updated README.md
* [ ] I have updated the dag
* [ ] If my changes concern incremental table, I have altered their schema to accomodate with field's creation/deletion
* [ ] I will create a review on slack and ensure to specify the duration of the review task: short (<10min), medium (<30min), long (>30min)

### Added tests?

* [ ] 👍 yes
* [ ] 🙅 no, because they aren't needed
* [ ] 🙋 no, because I need help
* [ ] ⏰ no, but I created a ticket

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
  * chore
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
