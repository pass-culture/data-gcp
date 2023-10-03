<!-- (go the the `Preview` tab and to preview rendered PR)

To tick boxe replace [ ] with [x]

 -->
- [ ] **MEP**
version:

- [ ] **Other**
<details>

## Describe your changes

<!-- Please include a summary of the changes:

This PR [adds/removes/fixes/replaces] the [feature/bug/etc]. 

Tag a reviewer if necessacy  @github/username 
-->



## Jira ticket number and/or notion link
<!-- 
TO DO repo admin: [https://passculture.atlassian.net/browse/PC-<num>](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/managing-repository-settings/configuring-autolinks-to-reference-external-resources)

JIRA-replace_with_ticket_number

[Notion-link](paste within parenthesis) 
-->

## PR type

<!-- Unfold relevant PR type and delete others -->

<details>

<summary>Hotfix</summary>

### Checklist before requesting a review
- [ ] I have performed a self-review of my code
- [ ] My code passes CI/CD tests
- [ ] I have made corresponding changes to the [tables documentation](https://www.notion.so/passcultureapp/Documentation-Tables-175a397a8e854ff4a55ae4f3620dbe3b)
- [ ] I have made corresponding changes to the [fields glossary](https://www.notion.so/passcultureapp/854a436a8f1541e1b6ec2a65f8bab600?v=798024ba90404b139e5a17407a3bc604)
- [ ] I have updated the dag
- [ ] I will create a review on slack. The review task should be short (<10min).


</details>



<details>

<summary>DA PR</summary>

### Type of change
- [ ] Fix (non-breaking change which corrects expected behavior)
- [ ] New fields (non-breaking change)
- [ ] New table (non-breaking change)
- [ ] Concept change (potentially breaking change which modifies fields according to new or evolving business concepts) 
- [ ] Table deletion (potentially breaking change which adds functionality/ table)
      
### Checklist before requesting a review
- [ ] I have performed a self-review of my code
- [ ] Fields have been snake_cased
- [ ] I have checked my modifications don't break downstream models
- [ ] If my changes concern incremental table, I have altered their schema to accomodate with field's creation/deletion
- [ ] I have made corresponding changes to the [tables documentation](https://www.notion.so/passcultureapp/Documentation-Tables-175a397a8e854ff4a55ae4f3620dbe3b)
- [ ] I have made corresponding changes to the [fields glossary](https://www.notion.so/passcultureapp/854a436a8f1541e1b6ec2a65f8bab600?v=798024ba90404b139e5a17407a3bc604)
- [ ] I have updated the dag in cases of dependencies
- [ ] My code passes CI/CD tests
- [ ] I will post on slack review channel and ensure to specify the duration of the review task: short (<10min), medium (<30min), long (>30min)

</details>


<details>

<summary>DE PR</summary>

### Type of change
- [ ] Fix (non-breaking change which corrects expected behavior)
- [ ] New fields (non-breaking change)
- [ ] New table (non-breaking change)
- [ ] Concept change (potentially breaking change which modifies fields according to new or evolving business concepts) 
- [ ] Table deletion (potentially breaking change which adds functionality/ table)
      
### Checklist before requesting a review
- [ ] I have performed a self-review of my code
- [ ] My code passes CI/CD tests
- [ ] I updated README.md
- [ ] I have updated the dag
- [ ] If my changes concern incremental table, I have altered their schema to accomodate with field's creation/deletion
- [ ] I have made corresponding changes to the [tables documentation](https://www.notion.so/passcultureapp/Documentation-Tables-175a397a8e854ff4a55ae4f3620dbe3b)
- [ ] I have made corresponding changes to the [fields glossary](https://www.notion.so/passcultureapp/854a436a8f1541e1b6ec2a65f8bab600?v=798024ba90404b139e5a17407a3bc604)
- [ ] I will create a review on slack and ensure to specify the duration of the review task: short (<10min), medium (<30min), long (>30min)


### Added tests?
- [ ] 👍 yes
- [ ] 🙅 no, because they aren't needed
- [ ] 🙋 no, because I need help
- [ ] ⏰ no, but I created a ticket

</details>

<details>

<summary>DS PR</summary>

### Type of change
- [ ] hotfix (non-breaking change which fixes an issue)
- [ ] New model
- [ ] Bug Fix
- [ ] Code Refacto
- [ ] Performance Improvements
- [ ] Test
- [ ] CI
- [ ] Config

      
### Checklist before requesting a review
- [ ] I have performed a self-review of my code
- [ ] My code passes CI/CD tests
- [ ] I updated README.md
- [ ] I have updated the dag
- [ ] If my changes concern incremental table, I have altered their schema to accomodate with field's creation/deletion
- [ ] I have documented the corresponding [notion page](https://www.notion.so/passcultureapp/Team-Data-engineering-Data-science-22ab0eb5ddf34dc2a854d9f0e596e91b)
- [ ] I have made corresponding changes to the [tables documentation](https://www.notion.so/passcultureapp/Documentation-Tables-175a397a8e854ff4a55ae4f3620dbe3b)
- [ ] I have made corresponding changes to the [fields glossary](https://www.notion.so/passcultureapp/854a436a8f1541e1b6ec2a65f8bab600?v=798024ba90404b139e5a17407a3bc604)
- [ ] I will create a review on slack and ensure to specify the duration of the review task: short (<10min), medium (<30min), long (>30min)

### Added tests?
- [ ] 👍 yes
- [ ] 🙅 no, because they aren't needed
- [ ] 🙋 no, because I need help
- [ ] ⏰ no, but I created a ticket


</details>


## Reviewer's checklist
- [ ] I have thoroughly read the code and validated the changes
- [ ] I have checked and approved the documentation

</details>