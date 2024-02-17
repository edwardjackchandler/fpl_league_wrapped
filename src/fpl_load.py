import pandas as pd
import requests
import abc


class FPLDataLoader:
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.base_url = "https://fantasy.premierleague.com/api/"
        self.url = None
        self.json = None

    # Convert the variables to attributes
    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    @property
    def json(self):
        return self._json

    @json.setter
    def json(self, json):
        self._json = json

    @abc.abstractmethod
    def request_data(self):
        self.json = requests.get(self.url).json()

    @abc.abstractmethod
    def format_request(self) -> str:
        self.data = pd.DataFrame(self.json)

    @abc.abstractmethod
    def format_data(self) -> pd.DataFrame:
        return pd.DataFrame(self.data)

    def get_data(self):
        self.request_data()
        self.format_request()
        return self.format_data()


def get_league_summary(self):
    managers_pdf = self.get_league_entries_pdf()
    entry_ids = managers_pdf["entry_id"].tolist()
    manager_pick_details = []

    for entry_id in entry_ids:
        manager_pick_details.append(self.get_pick_details(entry_id))

    manager_pick_details_pdf = pd.concat(manager_pick_details)

    return manager_pick_details_pdf.merge(managers_pdf, on="entry_id")


class StandingsLoader(FPLDataLoader):

    # Create a standings_schema_mapping attribute
    # Using ['id', 'event_total', 'player_name', 'rank', 'last_rank', 'rank_sort', 'total', 'entry', 'entry_name']

    standings_schema_mapping = {
        "id": "id",
        "event_total": "event_total",
        "player_name": "player_name",
        "rank": "league_rank",
        "last_rank": "last_rank",
        "rank_sort": "rank_sort",
        "total": "standings_total",
        "entry": "entry",
        "entry_name": "entry_name",
    }

    def __init__(self, league_id):
        super().__init__()
        self.league_id = league_id
        self.url = self.base_url + f"leagues-classic/{league_id}/standings/"

    def format_request(self):
        self.data = self.json["standings"]["results"]

    def format_data(self) -> pd.DataFrame:
        df = pd.DataFrame(self.data)
        df = df.rename(columns=self.standings_schema_mapping)
        return df


class HistoryLoader(FPLDataLoader):

    history_schema_mapping = {
        "event": "event",
        "points": "event_points",
        "total_points": "total_points",
        "rank": "fpl_event_rank",
        "rank_sort": "fpl_event_rank_sort",
        "overall_rank": "overall_rank",
        "bank": "bank",
        "value": "team_value",
        "event_transfers": "event_transfers",
        "event_transfers_cost": "event_transfers_cost",
        "points_on_bench": "points_on_bench",
        "entry": "entry",
    }

    def __init__(self, entry_id):
        super().__init__()
        self.entry_id = entry_id
        self.url = self.base_url + f"entry/{entry_id}/history/"

    def format_request(self):
        self.data = self.json["current"]

    def format_data(self):
        df = pd.DataFrame(self.data)
        df["entry"] = self.entry_id
        df = df.rename(columns=self.history_schema_mapping)
        return df


class LeagueHistoryLoader:
    league_history_schema_mapping = {
        "event": "event",
        "event_points": "event_points",
        "total_points": "cumulative_points",
        "fpl_event_rank": "fpl_event_rank",
        "fpl_event_rank_sort": "fpl_event_rank_sort",
        "overall_rank": "overall_rank",
        "bank": "bank",
        "team_value": "team_value",
        "event_transfers": "event_transfers",
        "event_transfers_cost": "event_transfers_cost",
        "points_on_bench": "points_on_bench",
        "entry": "entry",
        "player_name": "player_name",
        "entry_name": "entry_name",
    }


    def __init__(self, league_id):
        self.league_id = league_id
        self.standings = StandingsLoader(league_id)
        self.standings_df = self.standings.get_data()
        self.entry_ids = self.standings_df["entry"].tolist()
        self.histories = [HistoryLoader(entry_id) for entry_id in self.entry_ids]
        self.history_dfs = [history.get_data() for history in self.histories]

    def get_data(self):
        history_df = pd.concat(self.history_dfs)
        history_df = history_df.merge(self.standings_df, on="entry")
        # filter history_df to only include the keys in league_history_schema_mapping
        history_df = history_df.rename(columns=self.league_history_schema_mapping)
        return history_df


# standings = StandingsLoader(665568)
# print("standings")
# print(standings.get_data().columns)
# print(standings.get_data().head(2))

# history = HistoryLoader(3110982)
# print("history")
# print(history.get_data().columns)
# print(history.get_data().head(2))

# league_history = LeagueHistoryLoader(665568)
# print("league_history")
# print(league_history.get_df().columns)

# # Create a dataframe and group by the player_name and sum the bench points
# df = league_history.get_df()
# df = df.groupby("player_name").agg({"points_on_bench": "sum"}).sort_values(['points_on_bench'], ascending=False).reset_index()
# print(df)

