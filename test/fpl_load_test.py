import unittest
import pandas as pd
from src.fpl_load import FPLDataLoader, StandingsLoader, HistoryLoader, LeagueHistoryLoader  # Assuming the classes are in fpl_load.py

class TestFPLDataLoader(unittest.TestCase):
    def setUp(self):
        self.fpl_data_loader = FPLDataLoader()

    def test_init(self):
        self.assertEqual(self.fpl_data_loader.base_url, "https://fantasy.premierleague.com/api/")
        self.assertIsNone(self.fpl_data_loader.url)
        self.assertIsNone(self.fpl_data_loader.json)


class TestStandingsLoader(unittest.TestCase):
    def setUp(self):
        self.standings_loader = StandingsLoader(123)  # Assuming 123 is a valid league_id

    def test_init(self):
        self.assertEqual(self.standings_loader.league_id, 123)
        self.assertEqual(self.standings_loader.url, self.standings_loader.base_url + "leagues-classic/123/standings/")


class TestHistoryLoader(unittest.TestCase):
    def setUp(self):
        self.history_loader = HistoryLoader(456)  # Assuming 456 is a valid entry_id

    def test_init(self):
        self.assertEqual(self.history_loader.entry_id, 456)
        self.assertEqual(self.history_loader.url, self.history_loader.base_url + "entry/456/history/")


class TestLeagueHistoryLoader(unittest.TestCase):
    def setUp(self):
        self.league_history_loader = LeagueHistoryLoader(789)  # Assuming 789 is a valid league_id

    def test_init(self):
        self.assertEqual(self.league_history_loader.league_id, 789)

if __name__ == '__main__':
    unittest.main()