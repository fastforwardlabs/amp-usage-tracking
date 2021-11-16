#####
#
# The following script is used to refresh data for the AMP Usage Tracking dashboard
#
#####

import os
import datetime
import pandas as pd
from utils import (
    get_usage_last_14_days,
    create_connection,
    create_table_from_df,
    delete_table,
)

TOKEN = os.environ["GH_TOKEN"]

AMP_NAMES = [
    "Churn_Prediction",
    "Image_Analysis",
    "Anomaly_Detection",
    "NeuralQA",
    "Structural_Time_Series",
    "SpaCy_Entity_Extraction",
    "Explainability_LIME_SHAP",
    "Question_Answering",
    "Active_Learning",
    "MLFlow_Tracking",
    "Few-Shot_Text_Classification",
    "Object_Detection_Inference",
    "Canceled_Flight_Prediction",
    "Streamlit_on_CML",
    "APIv2",
    "AutoML_with_TPOT",
    "Summarize",
    "Train_Gensim_W2V",
    "Tensorboard_on_CML"
]

AMP_REPOS = ["cloudera/CML_AMP_" + amp for amp in AMP_NAMES]
AMP_REPOS.append("cloudera/Applied-ML-Prototypes")

# 1. Get latest 14 day usage - both tracking and referring DFs
amp_tracking_df, amp_referring_df = get_usage_last_14_days(
    gh_token=TOKEN, amp_repos=AMP_REPOS
)

# 2. Pull just the second to last days stats from tracking DF (basically the final full count of yesterdays stats) and save to daily_archive
completed_day = amp_tracking_df.index[-2]
daily_tracking_df = amp_tracking_df.loc[completed_day]

today_str = datetime.datetime.today().strftime("%m-%d-%Y")
yesterday_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime(
    "%m-%d-%Y"
)

# uncomment next line and set to today-1 date if a day is missed (or last avaiable date).
#yesterday_str = '08-24-2021' 

os.makedirs(f"data/daily_archive/{today_str}", exist_ok=True)

daily_tracking_df.to_pickle(
    f"data/daily_archive/{today_str}/daily_tracking_{today_str}.pkl"
)

amp_referring_df.to_pickle(
    f"data/daily_archive/{today_str}/daily_referring_last14_{today_str}.pkl"
)
print('---------- SAVED DAILY ARCHIVE ARTIFACTS ------------')

# 3. Load yesterday's production tracking_df and append new daily_tracking_df
old_prod_tracking = pd.read_pickle(
    f"data/prod_archive/{yesterday_str}/cumulative_tracking_{yesterday_str}.pkl"
)

new_prod_tracking = pd.concat([old_prod_tracking, daily_tracking_df])

os.makedirs(f"data/prod_archive/{today_str}", exist_ok=True)
new_prod_tracking.to_pickle(
    f"data/prod_archive/{today_str}/cumulative_tracking_{today_str}.pkl"
)

print('---------- SAVED PRODUCTION ARCHIVE ARTIFACTS ------------')

# 4. Delete existing SQLite tables
conn = create_connection(f"{os.getcwd()}/db/pythonsqlite.db")

delete_table("amp_tracking", conn)
delete_table("amp_referring", conn)

# 5. Create new tables to refresh data

create_table_from_df("amp_tracking", conn, new_prod_tracking)
create_table_from_df("amp_referring", conn, amp_referring_df)

print('---------- UPDATED SQLITE DATABASE TABLES ------------')