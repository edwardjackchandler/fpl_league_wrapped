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


class StandingsLoader(FPLDataLoader):
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
