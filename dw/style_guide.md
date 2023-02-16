# dbt Model, Project Structure, and Style Guide

[For reference, dbt's official style guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)

## General Naming and Field Conventions
A lot of dbt work involves taking some base table — application data, Stripe data, whatever — and cleaning it up for easier use in data work. As you're cleaning up the data, follow these guidelines:

* Schema, table and column names should be in `snake_case` (all lower case, using "_" instead of spaces).
* Use names based on the _business_ terminology, rather than the source terminology.
* Each model should have a primary key.
* Name the primary key of a model in the pattern `<object>_id`. For example, the primary key of the `accounts` table should be `account_id`. This makes it easier to know what `id` is being referenced in downstream joined models.
* For base/staging models, fields should be ordered in categories, where identifiers are first and timestamps are at the end.
* Name timestamp columns in the pattern `<event>_at`, e.g. `created_at`. UTC is the default timezone. If you're using a different timezone, indicate it with a suffix, e.g `created_at_pt`, or `created_at_local_tz` if the timezone is row-by-row specific.
* Prefix booleans with `is_` or `has_` or `was_`.
* Format price/revenue fields in decimal currency (e.g. `19.99` for $19.99; many app databases store prices as integers in cents). If non-decimal currency is used, indicate this with suffix, e.g. `price_in_cents`.
* Avoid reserved words as column names. If a source table uses reserved words as column names (e.g. `description`), put it in quotes.
* Consistency is key! Use the same field names across models where possible, e.g. a key to the `clients` table should be named `client_id` rather than `user_id`.

## Project Structure and Model Naming

Our models are organized into file and folder structures as follows (using the Stripe data source and a few models based on it as an example):

```
├── dbt_project.yml
└── models
    ├── 1_staging
    |   └── stripe
    |       ├── mobile
    |       |   ├── _stripe_mobile__models.yml
    |       |   ├── _stripe_mobile__sources.yml
    |       |   ├── stg_stripe_mobile__balance_transactions.sql
    |       |   ├── stg_stripe_mobile__charges.sql
    |       |   ├── stg_stripe_mobile__invoices.sql   
    ├── 3_business_rule
    |   └── stripe
    |       ├── mobile
    |       |   ├── _br_stripe_mobile__models.yml
    |       |   ├── financial_transaction
    |       |   |   ├── base
    |       |   |   |   ├── br_stripe_mobile__balance_transactions.sql
    |       |   |   |   ├── br_stripe_mobile__charges.sql
    |       |   |   ├── br_balance_transaction.sql
    |       |   |   ├── br_charge.sql
    ├── 4_info_mart
    |   ├── _info_mart__models.yml
    |   └── current_value
    |       ├── financial_transaction
    |       |   ├── im_balance_transaction.sql
    |       |   ├── im_charge.sql
    |       |   ├── vim_balance_transaction.sql
    |       |   ├── vim_charge.sql
    |   └── historical
    |       ├── financial_transaction
    |       |   ├── im_balance_transaction_hist.sql
    |       |   ├── im_charge_hist.sql
    |   └── point_in_time
    |       ├── financial_transaction
    |       |   ├── im_balance_transaction_pit.sql
    |       |   ├── im_charge_pit.sql
    ├── 5_metrics_rule
    |   ├── _metrics_rule__models.yml
    |   └── financial_transaction
    |       ├── mr_balance_transaction.sql
    ├── 6_metrics_mart
    |   ├── _metrics_mart__models.yml
    |   └── current_value
    |       ├── financial_transaction
    |       |   ├── mm_balance_transaction.sql
    |       |   ├── vmm_balance_transaction.sql
    |   └── historical
    |       ├── financial_transaction
    |       |   ├── mm_balance_transaction_hist.sql
    |   └── point_in_time
    |       ├── financial_transaction
    |       |   ├── mm_balance_transaction_pit.sql
└── snapshots
    ├── 2_raw_vault
    |   └── stripe_mobile
    |       ├── rv_stripe_mobile.sql
```

### 1. Staging Models

* Staging models are named and organized into folders and sub-folders corresponding to the data source, schema, and table from which they're loaded.
* The naming convention to follow is `stg_<source_schema_name>__<source_table_name>`, e.g. `stg_stripe_mobile__balance_transactions`.
* _NB: two underscores separate schema name from table name._
* Dependencies: Staging models should be built from exactly one source.
* Model materialization: table (full reload)
* Primary folder: 1_staging\data_source_name\schema_name

### 2. Snapshot Models ("Raw Vault")

* Staging models are snapshotted into tables in what's referred to as the Raw Vault, using dbt `snapshot` functionality.
* The naming convention to follow for raw vault snapshots is `rv_<source_schema_name>__<source_table_name>_snapshot`, e.g. `rv_stripe_mobile__balance_transactions_snapshot`.
* _NB: two underscores separate schema name from table name._
* Raw vault snapshots are considered separate models, but they are configured simply in files named for data source schemas.
* Each schema has one file containing all the configured snapshots for its tables — one for `stripe_mobile`, one for `talktala_production`, etc.
* Dependencies: Raw vault snapshots should be built from exactly one staging model.
* Model materialization: not configurable (but snapshots behave like incremental tables)
* Primary folder: snapshots\2_raw_vault\schema_name

### 3. Business Rule Models

* Business rule models are named and organized into folders and sub-folders corresponding to either the table and schema from which they are being loaded, or the business concept they represent.
* Dependencies: Business rule models should be built from raw vault models or other business rule models (more details provided below)
* Model materialization: view
* Primary folder: 3_business_rule\data_source_name\schema_name\concept_name

* _NB: There are two types of business rule models, each with a specific nomenclature and file structure:_

#### Type 1: "Base" Business Rules

* Type-1 `base` business rules prepare raw vault tables for later use. Base models rename fields to business-friendly terms, do general data cleanup, coalesce nulls, perform basic field calculations, or other prep work. 
* Base business rules are named in the format `br_<source_schema_name>_<source_table_name>`, e.g. `br_stripe_mobile_balance_transactions`. (_NB: only one underscore between schema name and table name.) Base business rules also live in their own subfolder within the business rules folder.
* Dependencies: In most cases, `base` business rule models will be built from one raw vault model.

#### Type 2: "Concept" Business Rules

* Type-2 `concept` business rules represent business _concepts_. They go a step further from base br models, and begin to shape, transform, and alter data, _or_ join together multiple models to implement more complex rules. 
* Business concept models are named according to the primary business concept they represent, in the format `br_<business_concept_name>`, e.g. `br_claim`. (_NB: the business concept name is singular, not plural._)
* Dependencies: Business concept models can built from one or more other business rule models, or raw vault models.

### 4. Information Mart Models

* Information mart models are named and organized into folders and sub-folders corresponding to the business concepts they represent, as well as the temporal nature of their data.
* There are three types of information mart models, described in more detail below.

#### A. Current-Value Information Marts

* Information marts that provide current-value (i.e. "present-tense") data should be in the format `im_<concept_name>`. For example, `im_claim` is a claim information mart containing only current-value data for all claim attributes.
* Current-value information marts are the most basic and should be implemented first.
* Dependencies: Current-value information marts are typically dependent on one or more business rule models, and may depend on other information marts. For example, we might base an information mart on another information mart according to how much data processing we need to create an output. If multiple information marts rely on a resource-intensive calculation, then it's probably best to do the calculation _once_ in one information mart. From there, any other information marts that need the same calculation can be built leveraging the calculation that was done in the first information mart.
* Model materialization: table (full reload)
* Primary folder: 4_info_mart\concept_name\current

#### B. Historical Data Information Marts

* Information marts that provide historical data should be in the format `im_<concept_name>_<hist>`; for example, `im_claim_hist` is a claim information mart containing a _history_ of how claims changed over time. We can track historical data in two ways:
    * Option 1: incrementally insert new or changed records using a primary key and a hash-diff comparison.
    * Option 2: Alternately, use the dbt `snapshot` functionality if we need something that more closely resembles a [type-2 slowly changing dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).
* Dependencies: We can build historical-value information marts directly from current-value information marts (hence why we should create current-value information marts first).
* Model materialization: incremental (table)
* Primary folder: 4_info_mart\concept_name\historical

#### C. Point in Time (PIT) Data Information Marts

* Point-in-time information marts provide a periodic image of what data looked like at a certain point in time. Name them in the format `im_<concept_name>_pit`. For example,  `im_claim_pit` is a claim information mart containing a record of all claims _as of_ a specified periodic interval (e.g. monthly, quarterly, etc.)
* _NB: The difference between a `_hist` and `_pit` mart is that a `_hist` mart tracks changes only whereas a `_pit` mart contains a record for each entity regardless of any changes that have or have not occurred since the prior `pit` load._
    * `_hist` example: Let's say `im_claim_hist` is being loaded several times a day, and that we have a claim in the mart, `claim_id 24601`. Every time we load the mart table, dbt creates a new record _only_ if `claim_id 24601` has not already been loaded, or if a tracked attribute in `claim_id 24601` has changed since the prior load. 
    * `_pit` example: Let's say `im_claim_pit` is being loaded monthly, and that we still have `claim_id 24601`. With each monthly load, a new record is inserted for `claim_id 24601` each time we take the monthly `pit` image, even if nothing in `claim_id 24601` has changed since the prior load. This is because the purpose of a `_pit` mart is to summarize all activity (or non-activity) during or at the end of a time span, rather than capture changes. This is akin to a type of fact table in dimensional modeling known as a *periodic snapshot*.
    * As alluded to in the example, all reporting entities will appear in each `_pit` image, even if there's no activity since the prior load.
* Dependencies: Point-in-time information marts can be built directly from current-value information marts, hence the need to create current value information marts first.
* Model materialization: incremental (table)
* Primary folder: 4_info_mart\concept_name\point_in_time

### 5. Metrics Rule Models

* Metrics rule models are named and organized into folders and sub-folders corresponding to business concepts and the specific metrics or report output they represent.
* These models should be in the format `mr_[metrics_report_name]`, e.g. `mr_balance_transaction` is a specialized instance of balance transaction metrics, containing a subset of all balance transaction types.
* Dependencies: Metrics rule models can be built from one or more information marts.
* Model materialization: view
* Primary folder: 5_metrics_rule/concept_name

### 6. Metrics Mart Models

* Metrics mart models are named and organized into folders and sub-folders corresponding to business concepts, the specific metrics or report output they represent, as well as the temporal nature of their data.
* There are three types of metrics mart models and they mirror the three types of information mart models: current, historical, and point_in_time.
* Dependencies: Metrics mart models can be built from one or more metrics rule models or other metrics mart models.
* Model materialization: same as for the three types of information mart models (table, incremental, incremental).
* Primary folder: 6_metrics_mart/concept_name

## "Which Model Do I Use?": A Helpful Guide

### User-facing Mart Views

To recap, there are two mart layers: _information_ marts and _metrics_ marts.
* It's good practice to make information marts and metrics marts accessible to users only through **views**, because then it's easier to make modifications to any underlying physical artifacts (i.e. tables) the views are based on, without adversely impacting who or what may be accessing the data. In other words, the views function essentially as interfaces to the data, with the details of how those data are organized hidden behind the facade a user, business intelligence software, or other tool is presented with. 
* The view names should mirror the format of the original information marts or metrics marts they are selecting data from, but prefixed with a `v` to distinguish them from the original tables. For example, `vim_claim` is a user-facing information mart view based on the original `im_claim` table, and `vmm_balance_transaction` is a user-facing metrics mart view based on the physical `mm_balance_transaction` table.
* Dependencies: Information mart views are typically based on a single, physical information mart. Metrics mart views are typically based on a single, physical metrics mart.
* Model materialization: view
* Primary folder: 4_info_mart\concept_name, and 6_metrics_mart\concept_name

### Business Rule and Information Mart Models vs. Metrics-Level Models

Business rule and information mart models differ from their metrics-level counterparts (metrics rules and metrics marts, respectively) in their sense of scope and breadth.
* Think of business rule models and information marts as _solid, ready-to-go building blocks_. They produce data covering a broad business concept. They'll clean up the data from the original source tables, and may join together some different tables to create a single convenient table that's clean and ready to go. 
* Metrics rule models and metrics marts, on the other hand, provide _narrow, purpose-built slices of data and metrics_ representing a subset of, or specialized version of, a broader business concept. If you need data that starts with "only clients who have done these three things..." or "only sessions with this important characteristic," then you should consider building that as a metrics-level model.

#### Example: Stripe Balance Transactions

In Stripe data, "balance transactions" are the base unit of cash transactions. They form the basis of a _lot_ of Stripe data analysis.

* The Stripe `br_balance_transaction` model is the source for the `im_balance_transaction` information mart. Together, the business concept model and the information mart model provide the rules and output for a general, business-level concept: the Stripe balance transaction. If you're starting a new analysis from scratch, you'll probably want to start with the information mart.
* The `mr_balance_transaction` metrics rule model and `mm_balance_transaction` metrics mart model leverage the balance_transaction information mart (and others) to provide a specific "cash-basis" view based on a subset of transaction types, which we use specifically for a few important financial reports. They _don't_ necessarily include all the data from the original information mart, but they _do_ serve the reports we need.

## CTE Guidelines

- All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file.
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do.
- Generally, try to aggregate and join in separate CTEs, rather than aggregating and joining all at once. This is more performant and more modular.
- Comment the heck out of CTEs with confusing or noteable logic.
- CTEs that are duplicated across models should be pulled out into their own models — don't repeat yourself!
- Create a `final` or similar CTE that you select from as your last line of code. This makes it easier to debug code within a model (without having to comment out code!)
- CTEs should be formatted like this:

``` sql
with

events as (

    ...

),

-- CTE comments go here
filtered_events as (

    ...

)

select * 
from filtered_events
where true
```

## SQL Style Guidelines

- Use trailing commas.
- Always add `where true` condition, and add additional filters below with indents. 
- Indents should be four spaces (except for predicates, which should line up with the `where` keyword).
- No need to indent or work on the next line if you're only selecting from one table. For example:
``` sql
-- No need to indent one-line clauses, even if other clauses are multi-line:
select cattos.*
from pet_schema.cats as cattos
where true
    and cattos.name = "Sebastian"

-- Break out multi-line clauses:
select
    doggo_name,
    treato_id
from
    pet_schema.dogs as doggos
    inner join snacks.treatos
        on doggos.fav_treato_id = treatos.id
where true
    and doggos.is_good = 1
```

- In general, and especially in upstream models like staging and business rule models, organize fields by the following categories, in the following order, and comment them as such:
    1. primary key(s) (labeled `pks`)
    2. foreign keys (labeled `fks`)
    3. misc
    4. dates
    5. metrics
    6. calculated columns (including case statements)
    7. metadata

Here's an abbreviated example from the `session_report` information mart:
``` sql
 -- pks
 session_report_id,
 
 -- fks
 case_id,
 room_id,
 session_modality_id,

 -- misc
 report_position,
 report_name,
 is_automatic_submission,

 -- dates
 completed_at,
 started_at,
 ended_at,
 reopened_at,

 -- metrics
 max_cost_of_service,

  -- calculated columns
 case 
     when completed_at is not null
     then True
     else False
 end is_complete,

 -- metadata
 {{ select_dms_metadata_cols('session_report') }},
 {{ select_dbt_metadata_cols('session_report') }}
```

- Lines of SQL should be 80 characters or less.
- Lowercase all field names, function names, and sql keywords.
- Always use the `as` keyword when aliasing a field or table. This improves readability.
- In the select, state fields before aggregates / window functions.
- Execute aggregations as early as possible, before joining to another table.
- Ordering and grouping by a number (eg. group by 1, 2) is preferred over listing the column names (see [this rant](https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/) for why).
- Prefer `union all` to `union` [*](http://docs.aws.amazon.com/redshift/latest/dg/c_example_unionall_query.html).
- Do not use single-letter aliases for tables. If a table has a single-word name (e.g. `plan` or `charges`), keep the table name or use a shortened version (e.g. `chg` for `charges`).
- Be careful about abbreviating two-word tables with common abbreviations; `pt` could be `payment_transactions`, `payment_type`, `private_talks`, etc. In these scenarios, refer to the Confluence table documentation for suggested abbreviations (e.g. we customarily shorten `payment_transactions` to `tx`).
- If joining two or more tables, _always_ prefix your column names with the table alias. If only selecting from one table, you don't need prefixes, but they're encouraged.
- Be explicit about your join (i.e. write `inner join` instead of `join`). `left joins` are normally the most useful, `right joins` often indicate that you should change which table you select `from` and which one you `join` to.

- *DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEW LINES ARE CHEAP, BRAIN TIME IS EXPENSIVE*

### Example SQL
```sql
with

my_data as (

    select *
    from {{ ref('my_data') }}
    where true

),

some_cte as (

    select * 
    from {{ ref('some_cte') }}
    where true

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5
    from some_cte
    group by 1

),

final as (

    select distinct
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from
        my_data
        left join some_cte_agg  
            on my_data.id = some_cte_agg.id
    where true 
        and my_data.field_1 = 'abc'
        and (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )
    having count(*) > 1

)

select *
from final

```

- Your join should list the "left" table first (i.e. the table you are selecting `from`):
```sql
select
    trips.*,
    drivers.rating as driver_rating,
    riders.rating as rider_rating
from
    trips
    left join users as drivers
        on trips.driver_id = drivers.user_id
    left join users as riders
        on trips.rider_id = riders.user_id

```
