#####
#
# Utility functions to support data refresh for AMP Usage Tracking dashboard 
#
#####

import os
import pickle
import datetime
import pandas as pd
import sqlite3
from sqlite3 import Error
from github import Github

def get_usage_last_14_days(gh_token, amp_repos):
    """
    Provided a GH API token and a list of qualified Github repository names that the
    API token has access to, this function pulls usage metrics (clones & views) for
    each repository for the last 14 days, and returns as a Pandas DataFrame. It also
    collects aggregate metrics on the sources sites driving traffic to each repo
    over the last 14 days.
    Args:
        gh_token (str)
        amp_repos List[str]
    Returns:
        amp_tracking_df (pd.DataFrame)
        amp_referring_df (pd_DataFrame)
    """

    gh = Github(gh_token)

    activity_dfs = []
    referring_dfs = []

    for repo in amp_repos:
        gh_repo = gh.get_repo(repo)

        # gather referring sites as DF
        try:
            refs = gh_repo.get_top_referrers()
            ref_data = []
            for ref in refs:
                data = {
                    "referrer": ref.referrer,
                    "refs_unique": ref.uniques,
                    "refs_total": ref.count,
                }
                ref_data.append(data)
            ref_df = pd.DataFrame(ref_data)
            ref_df["repo"] = repo[17:]
            referring_dfs.append(ref_df)
        except KeyError:
            print(f'No referrals in last 14 days for {repo}')
            pass

        # gather view activity as DF
        try:
            views = gh_repo.get_views_traffic(per="day")
            view_data = []
            for view in views["views"]:
                data = {
                    "timestamp": view.timestamp,
                    "views_unique": view.uniques,
                    "views_total": view.count,
                }
                view_data.append(data)
            view_df = pd.DataFrame(view_data).set_index("timestamp")
            idx = pd.date_range(
                end=pd.to_datetime("today").date().strftime("%m-%d-%Y"),
                start=(
                    pd.to_datetime("today").date() - datetime.timedelta(days=14)
                ).strftime("%m-%d-%Y"),
            )
            view_df = view_df.reindex(idx, fill_value=0)
        except KeyError:
            print(f'No views in last 14 days for {repo}')
            pass
            

        # gather clone activity as DF
        try:
            clones = gh_repo.get_clones_traffic(per="day")
            clone_data = []
            for clone in clones["clones"]:
                data = {
                    "timestamp": clone.timestamp,
                    "clones_unique": clone.uniques,
                    "clones_total": clone.count,
                }
                clone_data.append(data)

            clone_df = pd.DataFrame(clone_data).set_index("timestamp")
            clone_df = clone_df.reindex(idx, fill_value=0)

            # combine DFs
            activity_df = pd.concat([clone_df, view_df], axis=1)
            activity_df["repo"] = repo[17:]
        except KeyError:
            print(f'No clones in last 14 days for {repo}')
            pass

        activity_dfs.append(activity_df)

    amp_tracking_df = pd.concat(activity_dfs)
    amp_referring_df = pd.concat(referring_dfs).reset_index(drop=True)

    return amp_tracking_df, amp_referring_df

def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file

    Args:
        database file

    Returns:
        Connection object
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table_from_df(table_name, conn, df):
    """
    Create a SQLite table given the name, connection, and
    pandas dataframe

    """
    df.to_sql(name=table_name, con=conn)


def delete_table(table_name, conn):
    """
    Delete a table in a SQLite DB given the connection and table name

    """
    cur = conn.cursor()

    qry = f"DROP TABLE {table_name}"
    cur.execute(qry)


def select_all_from_table(table_name, conn):
    """
    Query a table in a SQLite DB given the connection and table name
    for all records and return as pandas dataframe

    """
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    return df