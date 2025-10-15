import pandas as pd
import pygsheets
import os
from sqlalchemy import create_engine, text
import warnings
import pendulum
from datetime import *

def get_gsheet_to_df(gsheet_name, 
                    api_path='/home/ec2-user/notebooks/joseph/useful_files/gsheets-api.json', 
                    worksheet_name=None):
    """
   Outputs pandas dataframe with parsed Google Sheet in it.
   Prerequisites: share viewer/editor access with API service account general-bi@general-bi-#automation.iam.gserviceaccount.com in the document settings
   :gsheet_name: title of Google Sheet
   :api_path: path to Google Drive JSON credentials
   :worksheet_name: name of the worksheet in the Google Sheet; selects first by default
   """
    client = pygsheets.authorize(service_file=api_path)
    sheet = client.open(gsheet_name)
    if worksheet_name:
        worksheet = sheet.worksheet('title', worksheet_name)
    else:
        worksheet = sheet[0]
    gsheet_df = pd.DataFrame(worksheet.get_all_records())
    return gsheet_df


#----mySQL Configuration BI team write-----
def data_conn():
    data_host = os.environ.get('data_host').strip('"')
    data_port = int(os.environ.get('data_port').strip('"'))
    data_dbname='data'
    data_user = os.environ.get('mysql_user').strip('"')
    data_password = os.environ.get('mysql_passw').strip('"')
    
    engine_string = f"mysql+pymysql://{data_user}:{data_password}@{data_host}:{data_port}/{data_dbname}"
    engine_data = create_engine(engine_string)
    return engine_data


#----mySQL Configuration read only-----
def nativead_conn():
    nativead_host = os.environ.get('nativead_host').strip('"')
    nativead_port = int(os.environ.get('nativead_port').strip('"'))
    nativead_dbname = 'nativead_production'
    nativead_user = os.environ.get('mysql_web_user').strip('"')
    nativead_password = os.environ.get('mysql_web_passw').strip('"')
    
    engine_string = f"mysql+pymysql://{nativead_user}:{nativead_password}@{nativead_host}:{nativead_port}/{nativead_dbname}"
    engine_nativead = create_engine(engine_string)
    return engine_nativead.connect().connection


#----mySQL Configuration appfigures-----
def domain_conn():
    domain_host = os.environ.get('rds_domain_info_host').strip('"')
    domain_port = int(os.environ.get('rds_domain_info_port').strip('"'))
    domain_dbname = 'app_info'
    domain_user= os.environ.get('rds_domain_info_user').strip('"')
    domain_password=os.environ.get('rds_domain_info_passw').strip('"')
    
    engine_string = f"mysql+pymysql://{domain_user}:{domain_password}@{domain_host}:{domain_port}/{domain_dbname}"
    engine_domain = create_engine(engine_string)
    return engine_domain.connect().connection


#----RedShift Configuration archive-----
def rs_archive_conn():
    redshift_endpoint = os.environ.get('redshift_permanent_host').strip('"')
    redshift_user = os.environ.get('redshift_permanent_user').strip('"')
    redshift_passw = os.environ.get('redshift_permanent_passw').strip('"')
    port = int(os.environ.get('redshift_permanent_port').strip('"'))
    dbname = "stackadaptdev"

    #_3b----Establishing Connection-----
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (redshift_user, redshift_passw, redshift_endpoint, port, dbname)
    engine_archive = create_engine(engine_string)
    return engine_archive.connect().connection


#----RedShift Configuration daily-----
def rs_daily_conn():
    redshift_endpoint = os.environ.get('redshift_daily_host').strip('"')
    redshift_user = os.environ.get('redshift_daily_user').strip('"')
    redshift_passw = os.environ.get('redshift_daily_passw').strip('"')
    port = int(os.environ.get('redshift_daily_port').strip('"'))
    dbname = "stackadaptdev"

    #_3b----Establishing Connection-----
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (redshift_user, redshift_passw, redshift_endpoint, port, dbname)
    engine_daily = create_engine(engine_string)
    return engine_daily.connect().connection


#----Staging Nativead Configuration daily-----
def staging_conn():
    staging_endpoint = "sa-prod-read-replica-3.cs7b27o2gcz7.us-east-1.rds.amazonaws.com"
    staging_user = os.environ.get('mysql_web_user').strip('"')
    staging_passw = os.environ.get('mysql_web_passw').strip('"')
    staging_port = int(os.environ.get('nativead_port').strip('"'))
    dbname = "nativead_production"

    #_3b----Establishing Connection-----
    engine_string = f"mysql+pymysql://{staging_user}:{staging_passw}@{staging_endpoint}:{staging_port}/{dbname}"
    engine_staging = create_engine(engine_string)
    return engine_staging.connect().connection

#----Staging Nativead Configuration daily-----
def sf_bi_conn():
    USER = os.environ.get('snowflake_bi_login').replace('"','')
    PWD = os.environ.get('snowflake_bi_passw').replace('"','').replace('@', '%40')
    ACC_ID = os.environ.get('snowflake_bi_account').replace('"','')
    REGION = os.environ.get('snowflake_bi_region').replace('"','')

    conn_string = f"snowflake://{USER}:{PWD}@{ACC_ID}.{REGION}?warehouse=STACKADAPT_BI_WAREHOUSE"
    engine = create_engine(conn_string)
    return engine

def sf_bi_example():
    to_return = """
        SELECT
            *
        FROM BI.PARTNERSHIPS.ACCOUNT_PROFITABILITY_DAILY
        LIMIT 10
        """
    return to_return

#----- Snowflake Reporting Configuration -----
def sf_reporting_conn():
    USER = os.environ.get('snowflake_login').replace('"','')
    PWD = os.environ.get('snowflake_passw').replace('"','')
    ACC_ID = os.environ.get('snowflake_account').replace('"','')
    REGION = os.environ.get('snowflake_region').replace('"','')

    conn_string = f"snowflake://{USER}:{PWD}@{ACC_ID}.{REGION}?warehouse=REPORTING_SERVICE_WAREHOUSE"
    engine_sf_reporting = create_engine(conn_string)
    return engine_sf_reporting.connect().connection

#----- Snowflake Finance Configuration -----
def sf_finance_conn():
    USER = os.environ.get('snowflake_fdw_login').replace('"','')
    PWD = os.environ.get('snowflake_fdw_passw').replace('"','')
    ACC_ID = os.environ.get('snowflake_fdw_account').replace('"','')
    REGION = os.environ.get('snowflake_fdw_region').replace('"','')
    
    conn_string = f"snowflake://{USER}:{PWD}@{ACC_ID}.{REGION}?warehouse=FINANCE_DATA_WAREHOUSE"
    engine_sf_finance = create_engine(conn_string)
    return engine_sf_finance.connect().connection

#----- Snowflake Finance Configuration -----
def sf_pa_conn():
    USER = os.environ.get('snowflake_pa_login').replace('"','')
    PWD = os.environ.get('snowflake_pa_passw').replace('"','')
    ACC_ID = os.environ.get('snowflake_pa_account').replace('"','')
    REGION = os.environ.get('snowflake_pa_region').replace('"','')
    
    conn_string = f"snowflake://{USER}:{PWD}@{ACC_ID}.{REGION}?warehouse=PATEAM"
    engine_sf_pa = create_engine(conn_string)
    return engine_sf_pa.connect().connection

#Help Statements
def bi_functions_help():
    to_return = """
    Names of functions are:
        1. get_gsheet_to_df(gsheet_name, api_path, worksheet_name)
        2. data_conn()
        3. nativead_conn()
        4. domain_conn()
        5. rs_archive_conn()
        6. rs_daily_conn()
        7. staging_conn()
        8. sf_bi_conn()
        9. sf_bi_example()
        10. all_done()
        11. query_sf_bi()

    Example connections:
        1. data_dbconn = data_conn()
            pd.read_sql(YOUR_MySQL_QUERY, data_dbconn)

        2. engine_daily = rs_daily_conn()
            pd.read_sql_query(YOUR_RedShift_SQL_QUERY, engine_daily)
        """
    return to_return

## Import up sound alert dependencies
from IPython.display import Audio, display
import random
from typing import Literal
_SOUNDS = Literal["screaming", "airport_gate", "lawnmower"]

#----All done notification-----
from IPython.display import Audio, display
import random
from typing import Literal, get_args
from multiprocessing import Process

_SOUNDS = Literal["screaming", "airport_gate", "lawnmower", "gong", "iron_man"]

POSSIBLE_SOUNDS = {
    "screaming": [
        "https://www.soundjay.com/human/man-scream-02.mp3",
        "https://www.soundjay.com/human/man-scream-03.mp3",
        "https://www.soundjay.com/human/man-scream-ahh-01.mp3",
        "https://www.soundjay.com/human/man-screaming-01.mp3",
    ],
    "airport_gate": [
        "https://www.soundjay.com/ambient/boarding-accouncement-1.mp3",
        "https://www.soundjay.com/ambient/check-point-1.mp3",
    ],
    "lawnmower": [
        "https://www.soundjay.com/mechanical/lawn-mower-01.mp3",
        "https://www.soundjay.com/mechanical/lawn-mower-02.mp3",
    ],
    'gong':[
        "https://www.soundjay.com/misc/sounds/bell-ring-01.mp3"
    ],
    "iron_man":[
        "https://www.soundjay.com/free-music/sounds/iron-man-01.mp3"
    ]
}


def all_done(sound: _SOUNDS = "gong", chaos=False):
    """
    This is useful function for notifying the completion of scripts.
    Select from three possible random choices of sounds including:
        1. screaming
        2. lawnmower
        3. airport_gate

    Args:
        sound (str, optional): select from a distinct curated list of notifcation sounds :) Defaults to airtport gate, supports screaming and lawnmower.
        chaos (bool, optional): defines if sounds are played altogether at the same time. Defaults to False.
    """

    if sound not in get_args(_SOUNDS):
        raise Exception(
            f"Selected sounds ({sound}) is not supported. Please choose from the following list: [{', '.join(sound for sound in get_args(_SOUNDS))}]"
        )

    selected_sounds = POSSIBLE_SOUNDS[sound]

    if chaos:
        processes = []
        for sound in selected_sounds:
            process = Process(
                target=display(Audio(url=sound, autoplay=True)), args=(sound,)
            )
            processes.append(process)
            process.start()

    else:
        selected_sound = random.choice(selected_sounds)
        display(Audio(url=selected_sound, autoplay=True))
        
def query_sf_bi(some_sql):
    
    #get all table names
    sql_sf = r"""
        SHOW TABLES IN ACCOUNT
        """

    df_table_names = pd.read_sql(sql_sf, sf_bi_conn())
    
    #get name of table you want assuming normal SQL statement
    start_index = some_sql.find("FROM") + len("FROM")
    end_index = some_sql.find("\n", start_index)
    string_wanted_table = some_sql[start_index:end_index].strip()
    
    #find your table wanted in all tables
    df_table_names_sub = df_table_names[df_table_names['name'] == string_wanted_table.upper()]
    
    #add relevent database and schema to your table desired
    string_database = df_table_names_sub['database_name'].tolist()[0]
    string_schema = df_table_names_sub['schema_name'].tolist()[0]
    string_full_location = f'{string_database}.{string_schema}.{string_wanted_table.upper()}'
    some_sql_updated = some_sql.replace(string_wanted_table, string_full_location)
    
    with warnings.catch_warnings():
        _return_df = pd.read_sql(some_sql_updated, sf_bi_conn())
        return _return_df
    
#steal query_shards hehe :)
from bubbles.utils.redshift.query_shards import query_shards

#----safety functino requiring inputing a date to prevent parts of scripts from running-----
from datetime import datetime
def safety_function():
    try:
        # Get current date, hour, and minutes from the user
        current_date = input("Enter current date (YYYY-MM-DD): ")
        current_time = input("Enter current time (HH:MM): ")

        # Combine date and time and convert to datetime object
        current_datetime_str = f"{current_date} {current_time}"
        current_datetime = datetime.strptime(current_datetime_str, "%Y-%m-%d %H:%M")

        # Your safety condition goes here (e.g., check if the date is valid)
        # For demonstration purposes, let's assume the condition is always True
        if True:
            print("Safety check passed. Proceed with the following code.")

            # Put your code here that you want to run if safety check passes

        else:
            print("Safety check failed. Incorrect date or time.")

    except ValueError as e:
        print(f"Error: {e}. Please enter a valid date and time format.")
        return this_is_an_undefined_variable_that_causes_errors

import json
 
# Data to be written
#gsheets = {
#    "auth_provider_x509_cert_url": os.environ.get("gsheets_bi_auth_provider_x509_cert_url").strip('"'),
#    "auth_uri": os.environ.get("gsheets_bi_auth_uri").strip('"'),
#    "client_email": os.environ.get("gsheets_bi_client_email").strip('"'),
#    "client_id": os.environ.get("gsheets_bi_client_id").strip('"'),
#    "client_x509_cert_url": os.environ.get("gsheets_bi_client_x509_cert_url").strip('"'),
#    "private_key":os.environ.get("gsheets_bi_private_key").strip("'").replace('\\n',"\n"),
#    "private_key_id": os.environ.get("gsheets_bi_private_key_id").strip('"'),
#    "project_id":os.environ.get("gsheets_bi_project_id").strip('"'),
#    "token_uri":os.environ.get("gsheets_bi_token_uri").strip('"'),
#    "bi_type":os.environ.get("gsheets_bi_type").strip('"')
#}
 
# Writing to sample.json

# Serializing json   
#json_object = json.dumps(gsheets, indent = 4)

sql_case_supply_inventory_type = """
(CASE 
    WHEN is_dooh=1 THEN 'DOOH'
    WHEN device_type in ('set-top','connected-tv') then 'CTV'
    WHEN is_ott=1 THEN 'OTT' 
    WHEN video_type='native' AND supply_inventory_type='video' THEN 'Native Video'
    WHEN video_type = 'rewarded' AND supply_inventory_type='video' THEN 'Rewarded Video'
    WHEN request_inventory_subtype = 'ingame' AND supply_inventory_type = 'video' THEN 'Ingame Video'
    WHEN request_inventory_subtype = 'ingame' AND supply_inventory_type = 'display' THEN 'Ingame Display'
    WHEN supply_inventory_type='image' THEN 'Native'
    ELSE supply_inventory_type 
    END) AS supply_inventory_type
"""

sql_case_format = """
(CASE 
    WHEN is_dooh=1 THEN 'DOOH'
    WHEN device_type in ('set-top','connected-tv') then 'CTV'
    WHEN is_ott=1 THEN 'OTT' 
    WHEN video_type='native' AND supply_inventory_type='video' THEN 'Native Video'
    WHEN video_type = 'rewarded' AND supply_inventory_type='video' THEN 'Rewarded Video'
    WHEN request_inventory_subtype = 'ingame' AND supply_inventory_type = 'video' THEN 'Ingame Video'
    WHEN request_inventory_subtype = 'ingame' AND supply_inventory_type = 'display' THEN 'Ingame Display'
    WHEN supply_inventory_type='image' THEN 'Native'
    ELSE supply_inventory_type 
    END) AS format,
"""

#----given a start and endate, return a list of days between-----
# currently support formats: datetime.date, datetime.datetime, pendulum.date, pendulum.datetime
def generate_list_days(start_date, end_date):
    if isinstance(start_date, pendulum.DateTime):
        print('current mode: pendulum.datetime')
        list_days = [
            start_date.date().add(days=i) for i in range((end_date.date() - start_date.date()).days + 1)
        ]
    elif isinstance(start_date, pendulum.Date):
        print('current mode: pendulum.date')
        list_days = [
            start_date.add(days=i) for i in range((end_date - start_date).days + 1)
        ]
    elif isinstance(start_date, datetime):
        print('current mode: datetime.datetime')
        delta = end_date.date() - start_date.date()
        list_days = [start_date.date() + timedelta(days=i) for i in range(delta.days + 1)]
    elif isinstance(start_date, date):
        print('current mode: pendulum.date')
        delta = end_date - start_date
        list_days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    else:
        print('unsupported date format, something might be wrong (X_x)')
    return list_days

#----Based on the current date, the date representing the next quarter is returned in the fillowing format 'Bidding_Guidance_Q{quarter}_{year}'
#from datetime import datetime, timedelta
def next_quarter():
    current_date = datetime.now()

    next_quarter_start = current_date + timedelta(days=92)

    year = next_quarter_start.year
    month = next_quarter_start.month

    if 1 <= month <= 3:
        quarter = "Q1"
    elif 4 <= month <= 6:
        quarter = "Q2"
    elif 7 <= month <= 9:
        quarter = "Q3"
    else:
        quarter = "Q4"

    output_string = f"Bidding_Guidance_{quarter}_{year}.csv"
    return output_string

def previous_quarter():
    current_date = datetime.now()

    year = current_date.year
    month = current_date.month

    # Mapping current quarter to the previous quarter
    if 1 <= month <= 3:  # Currently in Q1
        quarter = "Q4"
        year -= 1  # Adjust year for previous quarter
    elif 4 <= month <= 6:  # Currently in Q2
        quarter = "Q1"
    elif 7 <= month <= 9:  # Currently in Q3
        quarter = "Q2"
    else:  # Currently in Q4
        quarter = "Q3"

    output_string = f"Bidding_Guidance_{quarter}_{year}.csv"
    return output_string

def query_shard(query: str, host: str, port: int):
    """
    Query redshift shard and return results as pandas dataframe

    Args:
        query (str): SQL query to execute
        port (int): Redshift shard port
        host (str): Redshift shard host

    Returns:
        pandas.DataFrame: Query results
    """
    try:
        engine = create_engine(f"postgresql://{host}:{port}/")
        df = pd.read_sql(query, engine)
    except exc.OperationalError as e:
        raise SDMConnectionError(
            f"Error connecting to {host}:{port}. Please check your SDM configuration and try again."
        ) from e
    except TypeError as e:
        raise SQLParseError(
            "Error parsing query. Please check your query for special characters (e.g. %) and try again."
        ) from e
    return df

def query_shards_temp(query:str):
    shards = {
        "redshift_shard_0": {
            "host":"localhost",
            "port": 16450},
        "redshift_shard_1": {
            "host":"localhost",
            "port": 16451},
        "redshift_shard_2": {
            "host":"localhost",
            "port": 16452},
        "redshift_shard_3": {
            "host":"localhost",
            "port": 16453},
    }
    
    print("this is a TEMPORARY solution, we need a better long-term fix fast")
    df = pd.DataFrame()

    for i in shards:
        df = pd.concat([df, query_shard(query, shards[i]["host"], shards[i]["port"])])
        
    return df