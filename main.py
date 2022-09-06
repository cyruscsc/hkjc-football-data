import math
import pandas
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter, Retry


URL = "https://bet.hkjc.com/football/getJSON.aspx"


def get_data():
    """Get match data including results and last odds as JSON from HKJC."""

    def get_results() -> list:
        """Get match results data as JSON from HKJC."""
        end_date = datetime.today()
        start_date = end_date - relativedelta(months=1)
        # get total number of matches first
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])  # dont get error please
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(url=URL)
        cookies = dict(response.cookies)
        params = {
            "jsontype": "search_result.aspx",
            "startdate": start_date.strftime("%Y%m%d"),
            "enddate": end_date.strftime("%Y%m%d"),
            "pageno": 1
        }
        response = session.post(url=URL, params=params, cookies=cookies)
        results = response.json()
        matches_count = int(results[0]["matchescount"])
        pages_count = math.ceil(matches_count/20)
        # really going to get all match results
        results_list = []
        print(f"total matches: {matches_count}")
        print(f"total pages: {pages_count}")
        for page_no in range(1, pages_count + 1):
            params = {
                "jsontype": "search_result.aspx",
                "startdate": start_date.strftime("%Y%m%d"),
                "enddate": end_date.strftime("%Y%m%d"),
                "pageno": page_no
            }
            response = session.post(url=URL, params=params, cookies=cookies)
            results_list.extend(response.json()[0]["matches"])
            print(f"page {page_no} done.")
            if page_no % 10 == 0:
                # pretend to be a human regularly
                session.close()
                session = requests.Session()
                session.mount("https://", HTTPAdapter(max_retries=retries))
            else:
                pass
        session.close()
        return results_list

    def get_last_odds(results_list: list) -> list:
        """Get last odds data of a selected match as JSON from HKJC."""
        ids = [match["matchID"] for match in results_list]
        last_odds_list = []
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])  # dont get error please
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(url=URL)
        cookies = dict(response.cookies)
        n = 0
        for id in ids:
            n += 1
            params = {
                "jsontype": "last_odds.aspx",
                "matchid": id
            }
            response = session.post(url=URL, params=params, cookies=cookies)
            print(f"match {n} - match id: {id} - {response}")
            last_odds = response.json()
            last_odds_list.append(last_odds)
            if n % 50 == 0:
                # pretend to be a human regularly
                session.close()
                session = requests.Session()
                session.mount("https://", HTTPAdapter(max_retries=retries))
            else:
                pass
        session.close()
        return last_odds_list

    match_results_list = get_results()
    match_last_odds_list = get_last_odds(results_list=match_results_list)
    match_data = {
        "Match ID": [match["matchID"] for match in match_results_list],
        "Match Date": [match["matchDate"] for match in match_results_list],
        "Tournament": [match["tournament"]["tournamentNameEN"] for match in match_results_list],
        "Home Team": [match["homeTeam"]["teamNameEN"] for match in match_results_list],
        "Away Team": [match["awayTeam"]["teamNameEN"] for match in match_results_list],
        "Half-time Scores": [f'{match_last_odds["accumulatedscore"][0]["home"]}-{match_last_odds["accumulatedscore"][0]["away"]}' if "accumulatedscore" in match_last_odds and len(match_last_odds["accumulatedscore"]) >= 2 else "None" for match_last_odds in match_last_odds_list],
        "Half-time Home Odds": [match_last_odds["fhaodds"]["H"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Half-time Draw Odds": [match_last_odds["fhaodds"]["D"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Half-time Away Odds": [match_last_odds["fhaodds"]["A"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Full-time Scores": [f'{match_last_odds["accumulatedscore"][1]["home"]}-{match_last_odds["accumulatedscore"][1]["away"]}' if "accumulatedscore" in match_last_odds and len(match_last_odds["accumulatedscore"]) >= 2 else "None" for match_last_odds in match_last_odds_list],
        "Full-time Home Odds": [match_last_odds["hadodds"]["H"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Full-time Draw Odds": [match_last_odds["hadodds"]["D"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Full-time Away Odds": [match_last_odds["hadodds"]["A"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Handicap Home": [match_last_odds["hdcodds"]["HG"] if "hdcodds" in match_last_odds else "None" for match_last_odds in match_last_odds_list],
        "Handicap Away": [match_last_odds["hdcodds"]["AG"] if "hdcodds" in match_last_odds else "None" for match_last_odds in match_last_odds_list],
        "Handicap Home Odds": [match_last_odds["hdcodds"]["H"].replace("100@", "").replace("000@", "") if "hdcodds" in match_last_odds else "None" for match_last_odds in match_last_odds_list],
        "Handicap Away Odds": [match_last_odds["hdcodds"]["A"].replace("100@", "").replace("000@", "") if "hdcodds" in match_last_odds else "None" for match_last_odds in match_last_odds_list],
        "Handicap HAD Home": [match_last_odds["hhaodds"]["HG"] for match_last_odds in match_last_odds_list],
        "Handicap HAD Away": [match_last_odds["hhaodds"]["AG"] for match_last_odds in match_last_odds_list],
        "Handicap HAD Home Odds": [match_last_odds["hhaodds"]["H"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Handicap HAD Draw Odds": [match_last_odds["hhaodds"]["D"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Handicap HAD Away Odds": [match_last_odds["hhaodds"]["A"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Odd Number Scores Odds": [match_last_odds["ooeodds"]["O"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "Even Number Scores Odds": [match_last_odds["ooeodds"]["E"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "First Team to Score Home Odds": [match_last_odds["ftsodds"]["H"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "First Team to Score None Odds": [match_last_odds["ftsodds"]["N"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list],
        "First Team to Score Away Odds": [match_last_odds["ftsodds"]["A"].replace("100@", "").replace("000@", "") for match_last_odds in match_last_odds_list]
    }
    return match_data


def make_sheet(data: dict):
    """Create new or update existing .csv file with provided data."""
    # create or append csv file with dataframe
    try:
        with open(file="hkjc_football.csv", mode="r"):
            pass
    except FileNotFoundError:
        df = pandas.DataFrame.from_dict(data=data, orient="columns").set_index("Match ID")
        df.to_csv(path_or_buf="hkjc_football.csv", mode="w")
    else:
        df = pandas.DataFrame.from_dict(data=data, orient="columns").set_index("Match ID")
        df.to_csv(path_or_buf="hkjc_football.csv", mode="a", header=False)
    # remove duplicated data in csv file
    df = pandas.read_csv(filepath_or_buffer="hkjc_football.csv").set_index("Match ID")
    df.drop_duplicates(inplace=True)
    df.to_csv(path_or_buf="hkjc_football.csv", mode="w")


make_sheet(get_data())
