# Cost Optimization Action Plan: ml_feat__item_embedding Model

## Current Situation Analysis

The `ml_feat__item_embedding.sql` model has significant cost issues due to:

1. **Full table scans** on large source tables without date filtering
2. **Expensive joins** between complete datasets
3. **Unpartitioned source tables** causing BigQuery to scan all historical data
4. **Resource-intensive window functions** processing entire datasets
5. **Recent removal of date filter** from `ml_input__item_metadata` increasing data volume

## Impact Assessment

- **Current Cost Driver**: Full scan of `ml_preproc.item_embedding_extraction`
- **Compounding Factor**: Removal of `offer_updated_date` filter increases `ml_input__item_metadata` volume
- **Estimated Impact**: 10-50x more data processed per run

## Action Plan

### Phase 1: Immediate Wins (High Impact, Low Risk)

#### 1.1 Add Date Filtering to Main Model
**File**: `models/machine_learning/feature/ml_feat__item_embedding.sql`
**Change**: Add WHERE clause to limit extraction data
```sql
from {{ source("ml_preproc", "item_embedding_extraction") }} ie
where ie.extraction_date >= date_sub(current_date, interval 30 day)
inner join {{ ref("ml_input__item_metadata") }} im on ie.item_id = im.item_id
```

**Estimated Cost Reduction**: 70-90%
**Risk**: Low - maintains same business logic with recent data focus

#### 1.2 Configure Source Table Partitioning
**File**: `models/sources.yml`
**Change**: Add partitioning to source definition
```yaml
- name: item_embedding_extraction
  partition_by:
    field: extraction_date
    data_type: date
  cluster_by: ["item_id"]
```

**Estimated Cost Reduction**: 60-80% (when combined with filtering)
**Risk**: Low - infrastructure optimization

### Phase 2: Structural Improvements (High Impact, Medium Risk)

#### 2.1 Convert to Incremental Model
**File**: `models/machine_learning/feature/ml_feat__item_embedding.sql`
**Change**: Add incremental configuration
```sql
{{
    config(
        materialized='incremental',
        unique_key='item_id',
        partition_by={'field': 'extraction_date', 'data_type': 'date'},
        on_schema_change='append_new_columns'
    )
}}
```

**Estimated Cost Reduction**: 85-95% for subsequent runs
**Risk**: Medium - requires testing incremental logic

#### 2.2 Optimize Window Function Performance
**File**: `models/machine_learning/feature/ml_feat__item_embedding.sql`
**Change**: Pre-filter before window function
```sql
with recent_extractions as (
    select *
    from {{ source("ml_preproc", "item_embedding_extraction") }}
    where extraction_date >= date_sub(current_date, interval 30 day)
),
latest_per_item as (
    select *
    from recent_extractions
    qualify row_number() over (partition by item_id order by extraction_datetime desc) = 1
)
```

**Estimated Cost Reduction**: 40-60% on window function processing
**Risk**: Medium - changes query structure

### Phase 3: Advanced Optimizations (Medium Impact, Low Risk)

#### 3.1 Add Clustering to Improve Joins
**File**: `models/machine_learning/feature/ml_feat__item_embedding.sql`
**Change**: Configure clustering
```sql
{{
    config(
        cluster_by=['item_id', 'extraction_date']
    )
}}
```

**Estimated Cost Reduction**: 20-30% on join operations
**Risk**: Low - performance optimization

#### 3.2 Consider Materialization Strategy
**File**: `models/machine_learning/feature/ml_feat__item_embedding.sql`
**Change**: Evaluate table vs view materialization
```sql
{{ config(materialized="table") }}  # vs current default
```

**Estimated Cost Reduction**: Variable based on downstream usage
**Risk**: Low - can be easily reverted

### Phase 4: Data Retention Policy (High Impact, Low Risk)

#### 4.1 Implement Retention Policy
**Recommendation**: Establish data retention periods
- **Embedding extractions**: 30-90 days (adjust based on ML requirements)
- **Historical data**: Archive to cheaper storage after 6 months

**Implementation**: Add lifecycle management to BigQuery tables

## Implementation Timeline

### Week 1: Quick Wins
- [ ] Add date filtering to main model (1.1)
- [ ] Configure source partitioning (1.2)
- [ ] Test and validate results

### Week 2: Incremental Setup
- [ ] Convert to incremental model (2.1)
- [ ] Optimize window functions (2.2)
- [ ] Performance testing

### Week 3: Fine-tuning
- [ ] Add clustering configuration (3.1)
- [ ] Evaluate materialization strategy (3.2)
- [ ] Monitor cost improvements

### Week 4: Governance
- [ ] Implement retention policies (4.1)
- [ ] Document optimizations
- [ ] Set up cost monitoring alerts

## Success Metrics

- **Primary**: BigQuery slot hours consumed per model run
- **Secondary**: Query execution time
- **Tertiary**: Data freshness maintained

**Target**: 80-90% cost reduction while maintaining data quality and freshness

## Risk Mitigation

1. **Backup Strategy**: Keep current model as `ml_feat__item_embedding_legacy` during transition
2. **Validation**: Compare row counts and key metrics between old/new models
3. **Rollback Plan**: Documented steps to revert each optimization
4. **Monitoring**: Set up BigQuery cost alerts for the model

## Next Steps

1. Review and approve this action plan
2. Create feature branch for optimizations
3. Implement Phase 1 changes
4. Run cost comparison analysis
5. Proceed with subsequent phases based on results

---
*Generated: 2025-09-02*
*Model: ml_feat__item_embedding.sql*
*Focus: BigQuery cost optimization*
