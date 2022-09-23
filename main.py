import math
import pandas
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter, Retry


def talk_to_hkjc(params: dict):
    """Communicate with HKJC's website to get data."""
    url = "https://bet.hkjc.com/football/getJSON.aspx"
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])  # dont get error please
    session.mount("https://", HTTPAdapter(max_retries=retries))
    response = session.get(url=url)
    cookies = dict(response.cookies)
    response = session.post(url=url, params=params, cookies=cookies)
    session.close()
    return response.json()


def get_past_matches():
    """Get results and last odds of finished matches."""

    def get_match_ids() -> list:
        """Get match results of finished matches."""
        end_date = datetime.today()
        start_date = end_date - relativedelta(months=1)
        # get total number of matches first
        params = {
            "jsontype": "search_result.aspx",
            "startdate": start_date.strftime("%Y%m%d"),
            "enddate": end_date.strftime("%Y%m%d"),
            "pageno": 1
        }
        results = talk_to_hkjc(params=params)
        matches_count = int(results[0]["matchescount"])
        pages_count = math.ceil(matches_count/20)
        # really going to get all match results
        match_ids = []
        print(f"Total matches: {matches_count}")
        print(f"Total pages: {pages_count}")
        print(">> Start getting match IDs...")
        for page_no in range(1, pages_count + 1):
            params = {
                "jsontype": "search_result.aspx",
                "startdate": start_date.strftime("%Y%m%d"),
                "enddate": end_date.strftime("%Y%m%d"),
                "pageno": page_no
            }
            results = talk_to_hkjc(params=params)
            for match in results[0]["matches"]:
                match_ids.append(match["matchID"])
            print(f">>>> Page {page_no} done.")
        print(">> All match IDs retrieved.")
        return match_ids

    def get_last_odds(match_ids: list) -> list:
        """Get last odds of finished matches."""
        last_odds_list = []
        n = 0
        print(">> Start getting last odds of matches...")
        for match_id in match_ids:
            n += 1
            params = {
                "jsontype": "last_odds.aspx",
                "matchid": match_id
            }
            last_odds = talk_to_hkjc(params=params)
            last_odds_list.append(last_odds)
            print(f">>>> match {n} - match id: {match_id} - done")
        print(">> All last odds retrieved.")
        return last_odds_list

    return get_last_odds(match_ids=get_match_ids())


def get_upcoming_matches():
    """Get details and all odds of unfinished matches."""

    def get_match_ids() -> list:
        """Get match ids of unfinished matches."""
        params = {
            "jsontype": "fullmatchlist"
        }
        print(">> Start getting match IDs...")
        results = talk_to_hkjc(params=params)
        match_ids = [match["mID"] for match in results]
        print(">> All match IDs retrieved.")
        return match_ids

    def get_all_odds(match_ids: list) -> list:
        """Get all odds of unfinished matches."""
        all_odds_list = []
        n = 0
        print(">> Start getting all odds of matches...")
        for match_id in match_ids:
            n += 1
            params = {
                "jsontype": "odds_allodds.aspx",
                "matchid": match_id
            }
            results = talk_to_hkjc(params=params)
            for match in results["matches"]:
                if match["matchID"] == match_id:
                    all_odds_list.append(match)
                else:
                    pass
            print(f">>>> match {n} - match id: {match_id} - done")
        print(">> All odds retrieved.")
        return all_odds_list

    return get_all_odds(match_ids=get_match_ids())


def process_data(function) -> dict:
    """Turn the data received into a dictionary which can be used to build Pandas dataframe."""
    original_data = function()
    print(">> Start processing data...")
    processed_data = {
        "Match ID": [match["matchID"] for match in original_data],
        "Match Date": [match["matchDate"] for match in original_data],
        "Tournament": [match["tournament"]["tournamentNameEN"] for match in original_data],
        "Home Team": [match["homeTeam"]["teamNameEN"] for match in original_data],
        "Away Team": [match["awayTeam"]["teamNameEN"] for match in original_data],
        "Half-time Scores": [f'{match["accumulatedscore"][0]["home"]}-{match["accumulatedscore"][0]["away"]}' if "accumulatedscore" in match and len(match["accumulatedscore"]) >= 2 else "None" for match in original_data],
        "Half-time Home Odds": [match["fhaodds"]["H"].replace("100@", "").replace("000@", "") for match in original_data],
        "Half-time Draw Odds": [match["fhaodds"]["D"].replace("100@", "").replace("000@", "") for match in original_data],
        "Half-time Away Odds": [match["fhaodds"]["A"].replace("100@", "").replace("000@", "") for match in original_data],
        "Full-time Scores": [f'{match["accumulatedscore"][1]["home"]}-{match["accumulatedscore"][1]["away"]}' if "accumulatedscore" in match and len(match["accumulatedscore"]) >= 2 else "None" for match in original_data],
        "Full-time Home Odds": [match["hadodds"]["H"].replace("100@", "").replace("000@", "") for match in original_data],
        "Full-time Draw Odds": [match["hadodds"]["D"].replace("100@", "").replace("000@", "") for match in original_data],
        "Full-time Away Odds": [match["hadodds"]["A"].replace("100@", "").replace("000@", "") for match in original_data],
        "Handicap Home": [match["hdcodds"]["HG"] if "hdcodds" in match else "None" for match in original_data],
        "Handicap Away": [match["hdcodds"]["AG"] if "hdcodds" in match else "None" for match in original_data],
        "Handicap Home Odds": [match["hdcodds"]["H"].replace("100@", "").replace("000@", "") if "hdcodds" in match else "None" for match in original_data],
        "Handicap Away Odds": [match["hdcodds"]["A"].replace("100@", "").replace("000@", "") if "hdcodds" in match else "None" for match in original_data],
        "Handicap HAD Home": [match["hhaodds"]["HG"] for match in original_data],
        "Handicap HAD Away": [match["hhaodds"]["AG"] for match in original_data],
        "Handicap HAD Home Odds": [match["hhaodds"]["H"].replace("100@", "").replace("000@", "") for match in original_data],
        "Handicap HAD Draw Odds": [match["hhaodds"]["D"].replace("100@", "").replace("000@", "") for match in original_data],
        "Handicap HAD Away Odds": [match["hhaodds"]["A"].replace("100@", "").replace("000@", "") for match in original_data],
        "Odd Number Scores Odds": [match["ooeodds"]["O"].replace("100@", "").replace("000@", "") for match in original_data],
        "Even Number Scores Odds": [match["ooeodds"]["E"].replace("100@", "").replace("000@", "") for match in original_data],
        "First Team to Score Home Odds": [match["ftsodds"]["H"].replace("100@", "").replace("000@", "") for match in original_data],
        "First Team to Score None Odds": [match["ftsodds"]["N"].replace("100@", "").replace("000@", "") for match in original_data],
        "First Team to Score Away Odds": [match["ftsodds"]["A"].replace("100@", "").replace("000@", "") for match in original_data]
    }
    print(">> All data processed.")
    return processed_data


def generate_sheet(data: dict, file_name: str):
    """Create new or update existing .csv file with provided data."""
    # create or append csv file with dataframe
    print(">> Start generating CSV file...")
    try:
        with open(file=file_name, mode="r"):
            pass
    except FileNotFoundError:
        df = pandas.DataFrame.from_dict(data=data, orient="columns").set_index("Match ID")
        df.to_csv(path_or_buf=file_name, mode="w")
    else:
        df = pandas.DataFrame.from_dict(data=data, orient="columns").set_index("Match ID")
        df.to_csv(path_or_buf=file_name, mode="a", header=False)
    # remove duplicated data in csv file
    df = pandas.read_csv(filepath_or_buffer=file_name).set_index("Match ID")
    df.drop_duplicates(inplace=True)
    df.to_csv(path_or_buf=file_name, mode="w")
    print(">> CSV file saved.")


print("Start getting data of past matches...")
generate_sheet(data=process_data(function=get_past_matches), file_name="hkjc_football_past.csv")
print("Start getting data of upcoming matches...")
generate_sheet(data=process_data(function=get_upcoming_matches), file_name="hkjc_football_upcoming.csv")
print("Finished.")
