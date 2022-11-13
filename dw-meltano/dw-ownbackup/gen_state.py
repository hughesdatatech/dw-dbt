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

if len(sys.argv) > 1:
    max_date_end_override = sys.argv[1]
    force_full_extract = max_date_end_override == 'force_full_extract'

# Get vars for Snowflake connection
load_dotenv()
sf_user = os.environ.get('TARGET_SNOWFLAKE_USER') 
sf_pw = os.environ.get('TARGET_SNOWFLAKE_PASSWORD') 
sf_account = os.environ.get('TARGET_SNOWFLAKE_ACCOUNT')    

# Build URI for incremental load
ctx = snowflake.connector.connect(
    user=sf_user,
    password=sf_pw,
    account=sf_account
    )
cs = ctx.cursor()
try:
    cs.execute("use warehouse dw_wh_xs")
    cs.execute("use database dw_dev")
    cs.execute("truncate table if exists pagov.opioid_stays")
    cs.execute("select count(1) from information_schema.tables where table_schema ilike 'dbt_steve' and table_name ilike 'rv_pagov__opioid_stays'")
    rv_count = cs.fetchone()[0]
    max_time_period_date_end = "'1900-01-01T00:00:00.000'"

    # Get the max time_period_date_end loaded to the raw vault if 1) we're not forcing a full extract, and 2) the raw vault table exists
    if not force_full_extract and max_date_end_override != '' and rv_count != 0:
        sql = "select '''' || nvl(max(time_period_date_end), " + max_time_period_date_end + ") || '''' from dbt_steve.rv_pagov__opioid_stays"
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
env_aws_key_id = 'TARGET_SNOWFLAKE_AWS_ACCESS_KEY_ID=' + os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY_ID') + '\n'
env_aws_access_key = 'TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY=' + os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY') + '\n'
env_rest_path = 'TAP_REST_API_MSDK_PATH=' + soql + '\n'
f= open("._env","w+")
f.write(env_sf_user)
f.write(env_sf_pw)
f.write(env_sf_account)
f.write(env_aws_key_id)
f.write(env_aws_access_key)
f.write(env_rest_path)
f.close()
#sys.exit()

# Move temp file to become the new .env file
shutil.move('._env', '.env')
