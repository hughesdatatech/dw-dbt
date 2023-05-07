# ds-dbt

Welcome to my dbt project for the Hinge Health Analytics Engineering challenge!

## Background

This project is based on a demo dbt framework I had created for previous work and demos.
The latest PR and merge to development includes all of the work/commits related to this specific challenge.
Therefore, you should be able to just browse the summary/description from those commits to follow my 
thinking, and see the steps I took to make this particular demo work.

## How is the project structured?

There is a general pattern/approach I have to structuring my dbt projects. A detailed document on the
approach will be provided in a separate communication. This demo is based on that but
skips some things for the sake of time. The models for this demo are structured as follows:

**Seed**

In a dbt project, Seed models are typically used for reference data that do not 
change frequently. Seed models are version controlled just like any other model. The Seed 
models in this project are used to simulate the input/source data for this particular 
challenge (i.e. the csv, tsv files).

The three Seed models in this demo are:
* __ref_hh_us_softball_league__members__ - Source input containing us softball member data.
* __ref_hh_unity_golf_club__members__ - Source input containing unity golf member data.
* __ref_hh_master__companies__ - Source input containing company data.

**Staging**

Staging models are used to select data from a source, add metadata related to the
data load for tracking/audit purposes, and perform any basic datatype conversions
that might be required in order to get the data to load. The point is to not modify
the incoming data in any way that alters/changes the semantic meaning of the data. 
This is so that we can preserve a record of exactly what was loaded, as it came from the source.

The three Staging models in this demo are:
* __stg_hh_us_softball_league__members__ - Staging model for us softball member data.
* __stg_hh_unity_golf_club__members__ - Staging model for unity golf member data.
* __stg_hh_master__companies__ - Staging model for company data.

**Raw Vault**

Raw Vault models are snapshots of staged data as mentioned in the prior section. The concept
is taken from data vault modeling with the intent being to preserve a full, traceable history 
of what data were loaded, supplemented with a variety of metadata columns for tracking/audit purposes. 
Please note that for this particular demo I did not create Raw Vault models, since the incoming data 
are being simulated using Seed models. In a real project that is not a one-time demo, I will always
use Raw Vault models. NB: This is similar in thinking to data lake "insert-only" patterns from the early
stages of the big data era.

**Business Rule**

Business Rule models are used to apply any transforms needed to alter, combine, conform, etc., 
raw data in order to begin to make it consumable by downstream users or systems, according
to your unique business requirements. If you are following a dimensional modeling approach, this 
is where the various types of facts and dimensions can begin to take shape.

The two Business Rule models in this demo are:
* __br_member__ - Business rule model that combines softball/golf members and adds in company data.
* __br_member_exception__ - Business rule model that identifies exceptions in the combined __br_member__ model.

**Information Mart**

Information Marts are the user-facing models that represent the materialization of business rule output.
These models are typically subject area focused and should be developed in a way to answer multiple business questions.
They are not "report-specific" although you can have models further down-stream that are more tailored towards fulfilling
the needs of specific reports. Information Marts can be thought of as shared "building-blocks" that are vetted, quality
checked datasets that can be combined with other Information Marts if need be.

The three Information Mart models in this demo are:
* __im_member__ - Physical materialization of valid member data based on the __br_member__ and __br_member_exception__ 
models.
* __im_member_exception__ - Physical materialization of invalid member data based on the __br_member_exception__ model.
* __vim_member_all__ - Virtual materialization of ALL data based on the __im_member__ and __im_member_exception__ models.

## Summary

To summarize how the data/model flow works, these are the high-level steps:

1. Member and Company data are input via Seed models, with schema/datatypes mostly defined in advance since they are known.
    NB: I had to fake the input of a tsv file by defining it as a single column in a csv file, since dbt only supports csv 
    Seed models.
3. Staging models add metadata and ensure dates are properly converted/formatted.
4. The __br_member__ model applies business rules per the specified requirements to the staged member data. It combines both datasets into a common, conformed model. It also adds in company name where there is a match based on company id, otherwise 
the value "Unknown" is used as the company name.
5. The __br_member_exception__ model is built based on __br_member__. It looks for any exceptions to rules that are defined 
in a rule mapping in the dbt_project.yml file. The exceptions that can be identified are:
    * joined_at < birth_date (you could not have joined before you were born)
    * last_active_at < birth_date (you could not have been active before you were born)
    * last_active_at < joined_at (you could not have been active before you joined)
NB: If a member record has multiple exceptions, those exceptions are aggregated into a descriptive list so that the 
granularity of the __br_member_exception__ model is one record per member who has 1 or more exceptions.
6. The __im_member__ model then leverages both of the __br__ models to output a single table having members who have NO 
exceptional data per the three defined rules.
7. The __im_member_exception__ model uses the __br_member_exception__ model to output a single table having members who DO 
have exceptional data per at least one of the three defined rules.
8. The __vim_member_all__ model uses both of the physical __im___ models to output a single view containing ALL members so 
that the entire dataset can be viewed as one.
9. Each of the three Information Mart models has a "unique" test defined on the member_hk column. The member_hk column is 
the unique key in each model. It is a surrogate hash key comprised of the combination of the member id and the source system from which the member record came.
10. The __vim_member_all__ Information Mart model defines a dbt-expectations row count test to ensure that ALL of the source data from the two input Seed models have been loaded.
11. For demo purposes, the __vim_member_all__ Information mart model also defines a test on the company_name field which is 
set to warn on those records where the company_name is "Unknown". As previously mentioned, we are loading the unmatched 
company records to the model, but it is still useful to be alerted to the number of records that do not match.

## TO DO

Here are some things that are not complete or that I would improve upon:

* A real project should carefully consider what data need to be fully reloaded (the worst case), or what can be 
incrementally loaded (the best case). The is of course dependent upon a variety of factors related to how you receive the 
data, how changes are identified in the source, and more. For the sake of simplicity, this demo rebuilds all models with 
each run.
* Macros should be leveraged in the Staging models (and elsewhere) to further automate the addition of common metadata 
fields and to facilite common data loading patterns. I have many macros already built but for demo purposes I wanted to 
limit the use to some extent so that people can more easily see what is happening in the models.
* The method used to derive member first and last name from the full member name is a very simple, basic split on the space 
in the full name. A more sophisticated solution might account for additional variation in the full name format such as name 
prefix, suffix, title, etc.
* The method used to derive two char state abbreviation is very simple. If the state has two parts then take the first 
letter from each part. Otherwise, take the first two chars from the single part. This is "overfitted" in the sense that it 
only works for these data. A better solution might use a reference table that maps state names to abbreviations and then 
does a "fuzzy" or "closest" match when there is no exact match.
* Company data should ideally live in a master, conformed dimension/Information Mart with a physical materialization. 
Company data seems like it would represent a foundational building block. Other models should reference the company data 
from there, not from the Staging model as is currently being done. The Staging model is being used for quick demo purposes 
only.
* I have not included a linter in the CI/CI process. SQL Fluff can be used to check coding standards in a similar way that I am using dbt-checkpoint. It would just need to be added to the install requirements, and then as step in the development 
build process.
* I would expand upon the documentation of course! All models (especially the user-facing ones) should have adequate 
documentation. One intent of using dbt-checkpoint is to enforce the requirement that all models have a description. That is 
just a starting point for proper documentation. There is much more that can and should be done.
* There should be additional data quality checks with better alerting, warning, or failing. This is purely dependent on your unique requirements. For example, if there is no matching company then I am using "Unknown" as a place-holder name value. 
This is common practice in many data warehouse environments. However, if it was absolutely expected that the company should 
match then you could warn (like the demo is currently doing) or fail the pipeline entirely if you wanted.

## How do I run this demo?

The best way to run the demo is to execute the development branch pipeline that is setup in my CircleCI sandbox.
Login instructions, etc. will be provided in a separate communication.

__Thank you for reading!__
