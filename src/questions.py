import duckdb

# Import typing for dictionary
from typing import Dict


class AIQuestion:
    question_string: str
    sql_answer: str
    data_schema: Dict[str, type]


def get_total_points_left_on_bench(duckdb_df):
    """
    This function returns a DataFrame with the total points left on the bench for each player and entry,
    ordered by the total points left on the bench in descending order.
    """
    return duckdb.query(
        """
        SELECT 
            player_name, 
            entry_name, 
            SUM(points_on_bench) AS total_points_on_bench
        FROM 
            duckdb_df
        GROUP BY 
            player_name, 
            entry_name
        ORDER BY 
            total_points_on_bench DESC
    """
    ).to_df()


def get_most_points_left_on_bench_week(duckdb_df):
    """
    This function returns a DataFrame with the most points left on the bench in a week for each player and entry,
    along with the week (event) when this happened, ordered by the most points left on the bench in descending order.
    """
    return duckdb.query(
        """
        SELECT 
            d.player_name, 
            d.entry_name, 
            d.points_on_bench AS most_points_left_on_bench, 
            d.event
        FROM 
            duckdb_df d
        JOIN (
            SELECT 
                player_name, 
                entry_name, 
                MAX(points_on_bench) AS max_points
            FROM 
                duckdb_df
            GROUP BY 
                player_name, 
                entry_name
        ) m ON d.player_name = m.player_name AND d.entry_name = m.entry_name AND d.points_on_bench = m.max_points
        ORDER BY 
            most_points_left_on_bench DESC
    """
    ).to_df()


def get_biggest_difference(duckdb_df):
    """
    This function returns a DataFrame with the biggest difference in event points between two players in a single week,
    along with the week (event) when this happened, ordered by the difference in descending order.
    """
    return duckdb.query(
        """
        SELECT 
            a.event, 
            a.player_name AS player1, 
            a.entry_name AS entry1, 
            a.event_points AS points1,
            b.player_name AS player2, 
            b.entry_name AS entry2, 
            b.event_points AS points2,
            ABS(a.event_points - b.event_points) AS difference
        FROM 
            duckdb_df a, 
            duckdb_df b
        WHERE 
            a.event = b.event AND 
            a.player_name != b.player_name
        ORDER BY 
            difference DESC
        LIMIT 1
    """
    ).to_df()

def get_points_by_gameweek(duckdb_df):
    """
    This function returns a DataFrame with the points gained by a specific player and entry by gameweek,
    ordered by the gameweek in ascending order.
    """
    return duckdb.query(
        """
        SELECT
            player_name,
            entry_name,
            event AS gameweek, 
            SUM(event_points) OVER (PARTITION BY player_name, entry_name ORDER BY event) AS points
        FROM 
            duckdb_df
        ORDER BY 
            gameweek ASC
    """).to_df()

def get_most_frequent_last_rank(duckdb_df):
    """
    This function returns a DataFrame with the player who has been ranked last for each week throughout the season the most times.
    """
    return duckdb.query(
        """
        SELECT 
            player_name, 
            COUNT(*) AS times_last_rank
        FROM (
            SELECT 
                event AS gameweek, 
                player_name
            FROM 
                duckdb_df
            WHERE 
                rank = (SELECT MAX(rank) FROM duckdb_df d2 WHERE d2.event = duckdb_df.event)
        ) subquery
        GROUP BY 
            player_name
        ORDER BY 
            times_last_rank DESC
        LIMIT 1
    """
    ).to_df()