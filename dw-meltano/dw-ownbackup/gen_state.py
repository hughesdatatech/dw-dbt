#!/usr/bin/env python

import os
import shutil
import sys
import snowflake.connector
from dotenv import load_dotenv
from requests.utils import requote_uri

# Check argv to see if we should force a full extract or use an override value
max_date_end_override = ''
force_full_extract = False
max_time_period_date_end = "'1900-01-01T00:00:00.000'"

if len(sys.argv) > 1:
    max_date_end_override = sys.argv[1]
    force_full_extract = max_date_end_override == 'force_full_extract'
    if not force_full_extract:
        max_time_period_date_end = max_date_end_override

# Get vars for Snowflake connection
load_dotenv()
sf_user = os.environ.get('TARGET_SNOWFLAKE_USER') 
sf_pw = os.environ.get('TARGET_SNOWFLAKE_PASSWORD') 
sf_account = os.environ.get('TARGET_SNOWFLAKE_ACCOUNT')
sf_db = os.environ.get('TARGET_SNOWFLAKE_DBNAME')
sf_wh = os.environ.get('TARGET_SNOWFLAKE_WAREHOUSE')
sf_tgt_schema = os.environ.get('TARGET_SNOWFLAKE_DEFAULT_TARGET_SCHEMA')
sf_dbt_schema = os.environ.get('TARGET_SNOWFLAKE_DBT_SCHEMA')

# Build URI for incremental load
ctx = snowflake.connector.connect(
    user=sf_user,
    password=sf_pw,
    account=sf_account,
    database=sf_db,
    warehouse=sf_wh    
)
cs = ctx.cursor()
try:
    cs.execute("truncate table if exists " + sf_tgt_schema + ".opioid_stays")
    cs.execute("select count(1) from information_schema.tables where table_schema ilike '" + sf_dbt_schema + "' and table_name ilike 'rv_pagov__opioid_stays'")
    rv_count = cs.fetchone()[0]
    
    # Get the max time_period_date_end loaded to the raw vault if 1) we're not forcing a full extract, 2) are not overriding the value, and 3) the raw vault table exists
    if not force_full_extract and max_date_end_override == '' and rv_count != 0:
        sql = "select '''' || nvl(max(time_period_date_end), " + max_time_period_date_end + ") || '''' from " + sf_dbt_schema + ".rv_pagov__opioid_stays"
        cs.execute(sql)
        max_time_period_date_end = cs.fetchone()[0]
    
    soql = requote_uri('?$where=time_period_date_end>' + max_time_period_date_end)
    print('using soql == ' + soql)
finally:
    cs.close()
ctx.close()
#sys.exit()

# Setup vars and URI to maintain state between loads and write to temp file
env_sf_user = 'TARGET_SNOWFLAKE_USER=' + sf_user + '\n'
env_sf_pw = 'TARGET_SNOWFLAKE_PASSWORD=' + sf_pw + '\n'
env_sf_account = 'TARGET_SNOWFLAKE_ACCOUNT=' + sf_account + '\n'
env_sf_db = 'TARGET_SNOWFLAKE_DBNAME=' + sf_db + '\n'
env_sf_wh = 'TARGET_SNOWFLAKE_WAREHOUSE=' + sf_wh + '\n'
env_sf_sf_tgt_schema = 'TARGET_SNOWFLAKE_DEFAULT_TARGET_SCHEMA=' + sf_tgt_schema + '\n'
env_sf_dbt_schema = 'TARGET_SNOWFLAKE_DBT_SCHEMA=' + sf_dbt_schema + '\n'
env_sf_ff = 'TARGET_SNOWFLAKE_FILE_FORMAT=' + os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT') + '\n'
env_sf_bucket = 'TARGET_SNOWFLAKE_S3_BUCKET=' + os.environ.get('TARGET_SNOWFLAKE_S3_BUCKET') + '\n'
env_sf_stage = 'TARGET_SNOWFLAKE_STAGE=' + os.environ.get('TARGET_SNOWFLAKE_STAGE') + '\n'
env_aws_key_id = 'TARGET_SNOWFLAKE_AWS_ACCESS_KEY_ID=' + os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY_ID') + '\n'
env_aws_access_key = 'TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY=' + os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY') + '\n'
env_rest_path = 'TAP_REST_API_MSDK_PATH=' + soql + '\n'

f = open("._env","w+")
f.write(env_sf_user)
f.write(env_sf_pw)
f.write(env_sf_account)
f.write(env_sf_db)
f.write(env_sf_wh)
f.write(env_sf_sf_tgt_schema)
f.write(env_sf_dbt_schema)
f.write(env_sf_ff)
f.write(env_sf_bucket)
f.write(env_sf_stage)
f.write(env_aws_key_id)
f.write(env_aws_access_key)
f.write(env_rest_path)
f.close()
#sys.exit()

# Move temp file to become the new .env file
shutil.move('._env', '.env')
