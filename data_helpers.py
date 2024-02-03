import numpy as np
import pandas as pd
import re
from constants import name_dict

DATA_PATH = "data/generated_data"


def get_weekly_game_info(week_num):
    """Get relevant game info for one week of games."""

    df = pd.read_excel("data/pickem_results_23_24.xlsx", sheet_name=f"week{week_num}")
    # first row of all nulls (where I'm going to cut off the table)
    first_row = df.index[df.isnull().all(1)].values[0]
    # row after last row of games (where totals are calculated)
    last_row = df.index[df["Timestamp"].str.contains("total") == True].values[0]
    df_T = df.iloc[first_row + 1 : last_row].reset_index()
    df_T.drop(["index"], axis=1, inplace=True)
    df_T.columns = df_T.iloc[0]
    df_T.columns = df_T.columns.str.lower()
    df_T.columns.values[0] = "game_id"
    df_T.drop([0, 1], inplace=True)
    df_T = df_T.loc[:, "game_id":"winner"]
    df_T = df_T.reindex(sorted(df_T.columns), axis=1)
    df_T["week_id"] = [week_num] * len(df_T)
    # drop all columns with email address
    df_clean = df_T.loc[:, ~df_T.columns.str.contains("com")]
    return df_clean


def get_away_line(game_str):
    away_line_str = game_str.split(" ")[1]
    if away_line_str == "(PK)":
        return 0
    return float(re.sub("[()]", "", away_line_str))


def get_favorite(row):
    if row["away_line"] < 0:
        # return favorite, underdog
        return (row["away_team"], row["home_team"])
    elif row["away_line"] > 0:
        return (row["home_team"], row["away_team"])
    return ("None", "None")


def create_game_info():
    """Creates the game_info table."""
    info_sheets = []
    for i in range(1, 19):
        info_sheets.append(get_weekly_game_info(i))
    game_info = pd.concat(info_sheets)
    game_info["away_team"] = [g.split(" ")[0] for g in game_info["game_id"]]
    game_info["home_team"] = [g.split(" ")[3] for g in game_info["game_id"]]
    game_info["away_line"] = game_info["game_id"].apply(lambda x: get_away_line(x))
    game_info["home_line"] = [line * -1 for line in game_info["away_line"]]
    game_info["favorite"], game_info["underdog"] = zip(
        *game_info.apply(get_favorite, axis=1)
    )
    game_info.to_excel(f"{DATA_PATH}/game_info.xlsx", index=False)


def get_weekly_picks(week_num):
    """Get picks for one week of games"""
    df = pd.read_excel("data/pickem_results_23_24.xlsx", sheet_name=f"week{week_num}")
    first_row = df.index[df.isnull().all(1)].values[0]
    df = df[:first_row]
    df.drop(["Timestamp", "Name", "Email"], axis=1, inplace=True, errors="ignore")
    df.rename(columns={"Email Address": "email", "ATS Bonus": "bonus"}, inplace=True)
    df = df.loc[:, "email":"bonus"]
    df.drop(["bonus"], axis=1, inplace=True)
    num_games = len(df.columns[1:])
    df["pick"] = df.iloc[:, 1 : num_games + 1].values.tolist()
    df["game_id"] = [df.iloc[:, 1 : num_games + 1].columns.values.tolist()] * len(df)
    df_explode = (
        df[["email", "pick", "game_id"]]
        .set_index(["email"])
        .apply(pd.Series.explode)
        .reset_index()
    )
    df_explode["week_id"] = [week_num] * len(df_explode)
    return df_explode


def create_all_picks():
    """Creates a table for all users picks for all weeks."""
    weekly_picks_lst = []
    for i in range(1, 19):
        weekly_picks_lst.append(get_weekly_picks(i))
    all_picks = pd.concat(weekly_picks_lst)
    all_picks["user_id"] = all_picks["email"].apply(lambda x: name_dict[x])
    all_picks.to_excel(f"{DATA_PATH}/all_picks.xlsx", index=False)


def create_all_picks_info():
    """Create a table that joins individual picks with relevant info about those games."""
    all_picks = pd.read_excel(f"{DATA_PATH}/all_picks.xlsx")
    game_info = pd.read_excel(f"{DATA_PATH}/game_info.xlsx")
    all_picks_info = pd.merge(
        all_picks, game_info, on=["week_id", "game_id"], how="inner"
    )
    all_picks_info["didnt_pick"] = np.where(
        all_picks_info["pick"] == all_picks_info["away_team"],
        all_picks_info["home_team"],
        all_picks_info["away_team"],
    )
    all_picks_info.to_excel(f"{DATA_PATH}/all_picks_info.xlsx", index=False)


# If I manually change the source data (sometimes I need to do some manual cleaning because it's
# messy), I like to just reprocess and write all of the tables. Since it's very small data, it
# only takes a few seconds. This command allows for me to do it in one fell swoop.
def recreate_data():
    create_game_info()
    create_all_picks()
    create_all_picks_info()


# Easier way to call already-created tables in my notebook.
def get_table(table_name):
    df = pd.read_excel(f"{DATA_PATH}/{table_name}.xlsx")
    exclude_col = "email"
    if exclude_col in df.columns:
        df = df.drop(columns=[exclude_col])
    return df
