Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### Environement variables : 
- FIREBASE_DAG_TYPE : specify intraday/daily
- ENV_SHORT_NAME : specify dev/stg/prod to select source data.


### CLI options : 
-- target [dev/stg/prod] to select destination dbt transformed data.


### Tests : 
1. **Generic**

dbt comes with 4 default tests : 
- unique : a column values should be unique
- not_null : a column should not contain null values
- accepted_values : a column values should be one of an element of a list
- relationships : each column value in model_a should exists in a column of model_b
2. **Packages**
Some packages contains tests.
- dbt_expectations
    - List of available expectations/tests is [here](https://github.com/calogica/dbt-expectations)
- dbt_utils contains predefined macro to test data
    - the list of tests is available [here](https://github.com/dbt-labs/dbt-utils)
3. We defined **custom tests** here : `test/generic folder`
- greater_than_other_column : test if a column is greater than another one
- sum_2_columns_equals_another_column : test

### How to add existent test to a column ?
Find or create the .yml file associated to the table we want to add test. 
1. Add the test in `tests` section

```yaml
version: 2

models: 
  - name: enriched_venue_tags_data
    description: Venue tags data
    columns: 
      - name: venue_id
        description: Unique identifier of a venue
        tests:
          - unique:
              severity: error
          - not_null:
              severity: error
```

2. By default, in case of test failure, it will raise a warning. To raise an error, add `severity: error` below the test name. Be conscient that a warning will not block the ingestion of children models while an error will. 

3. run `dbt test` to execute all tests OR run `dbt test --select model_name` to execute all tests associalted to `model_name`.

### How to add a new test to a column ?

1. Create the test with a macro in the folder `tests/generic`. The test should be documented to be reusable by your colleague.

2. Do the same as previous section.
