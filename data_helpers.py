import numpy as np
import pandas as pd
import re


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


def make_game_info():
    """Makes the game_info table."""
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
    game_info.to_excel("data/generated_data/game_info.xlsx", index=False)


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


def make_all_picks():
    weekly_picks_lst = []
    for i in range(1, 19):
        weekly_picks_lst.append(get_weekly_picks(i))
    all_picks = pd.concat(weekly_picks_lst)
    all_picks["user_id"] = all_picks["email"].apply(lambda x: name_dict[x])
    all_picks.to_excel("data/generated_data/all_picks.xlsx", index=False)
