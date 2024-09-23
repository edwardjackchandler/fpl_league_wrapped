import duckdb

# Import typing for dictionary
from typing import Dict


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
            SUM(points_on_bench) AS bench_points
        FROM 
            duckdb_df
        GROUP BY 
            player_name, 
            entry_name
        ORDER BY 
            bench_points DESC
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
    """
    ).to_df()


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


def get_total_points_and_bench_points(duckdb_df):
    """
    This function returns a DataFrame with the total points and total points left on the bench for each player and team for the season.
    """
    return duckdb.query(
        """
        SELECT 
            player_name, 
            entry_name, 
            SUM(points_on_bench) AS bench_points, 
            SUM(event_points)AS total_points
        FROM 
            duckdb_df
        GROUP BY 
            player_name, 
            entry_name
    """
    ).to_df()


def get_player_best_rank_event(duckdb_df):
    """
    This function returns a DataFrame with the player_name, entry_name, best_rank, and the event on which that best_rank happened
    """
    return duckdb.query("""
        WITH total_points AS (
            SELECT 
                event, 
                event_points, 
                entry_name, 
                player_name,
                SUM(event_points) OVER (PARTITION BY player_name, entry_name ORDER BY event) AS total_points
            FROM
                duckdb_df
        ),
        ranks AS (
            SELECT 
                event, 
                event_points, 
                entry_name, 
                player_name,
                total_points,
                RANK() OVER (PARTITION BY event ORDER BY total_points DESC) as rank
            FROM
                total_points
        ),
        best_ranks AS (
            SELECT 
                player_name,
                entry_name,
                MIN(rank) as best_rank
            FROM 
                ranks
            GROUP BY
                player_name,
                entry_name
        )
        SELECT 
            br.player_name,
            br.entry_name,
            br.best_rank,
            STRING_AGG(CAST(r.event AS VARCHAR), ', ' ORDER BY event) AS event_list
        FROM
            best_ranks br
        JOIN 
            ranks r
        ON 
            br.player_name = r.player_name AND 
            br.entry_name = r.entry_name AND 
            br.best_rank = r.rank
        GROUP BY
            br.player_name,
            br.entry_name,
            br.best_rank
        ORDER BY 
            br.entry_name
    """).to_df()


def get_player_worst_rank_event(duckdb_df):
    """
    This function returns a DataFrame with the player_name, entry_name, worst_rank, and the event on which that worst_rank happened
    """
    return duckdb.query("""
        WITH total_points AS (
            SELECT 
                event, 
                event_points, 
                entry_name, 
                player_name,
                SUM(event_points) OVER (PARTITION BY player_name, entry_name ORDER BY event) AS total_points
            FROM
                duckdb_df
        ),
        ranks AS (
            SELECT 
                event, 
                event_points, 
                entry_name, 
                player_name,
                total_points,
                RANK() OVER (PARTITION BY event ORDER BY total_points DESC) as rank
            FROM
                total_points
        ),
        worst_ranks AS (
            SELECT 
                player_name,
                entry_name,
                MAX(rank) as worst_rank
            FROM 
                ranks
            GROUP BY
                player_name,
                entry_name
        )
        SELECT 
            wr.player_name,
            wr.entry_name,
            wr.worst_rank,
            STRING_AGG(CAST(r.event AS VARCHAR), ', ' ORDER BY event) AS event_list
        FROM
            worst_ranks wr
        JOIN 
            ranks r
        ON 
            wr.player_name = r.player_name AND 
            wr.entry_name = r.entry_name AND 
            wr.worst_rank = r.rank
        GROUP BY
            wr.player_name,
            wr.entry_name,
            wr.worst_rank
        ORDER BY 
            wr.player_name
    """).to_df()


def get_best_player_tally(duckdb_df):
    """
    This function looks at every game week, and calculates the best player tally.
    The best player is defined as the player who has the most points in that game week.
    Players should only exist in this if the number of times they are the best player is greater than 0.

    Schema: player_name, entry_name, game_weeks_won_total, game_weeks_won_list, game_weeks_won_dict
    e.g.
    | player_name | entry_name | game_weeks_won_total | game_weeks_won_list | game_weeks_won_dict       |
    |-------------|------------|----------------------|---------------------|---------------------------|
    | player1     | entry1     | 5                    | 1, 3, 5, 7, 9       | {1: 10, 3: 15, 5: 20, ...}|
    | player2     | entry2     | 3                    | 2, 4, 6             | {2: 12, 4: 18, 6: 22}     |
    """
    return duckdb.query("""
        WITH best_player AS (
            SELECT 
                event, 
                player_name, 
                entry_name, 
                event_points - event_transfers_cost AS net_points,
                RANK() OVER (PARTITION BY event ORDER BY net_points DESC) as rank
            FROM 
                duckdb_df
        )
        SELECT 
            player_name, 
            entry_name, 
            COUNT(*) AS game_weeks_won_total,
            STRING_AGG(CAST(event AS VARCHAR), ', ' ORDER BY event) AS game_weeks_won_list,
            MAP(LIST(event), LIST(net_points)) AS game_weeks_won_dict
        FROM 
            best_player
        WHERE 
            rank = 1
        GROUP BY 
            player_name, 
            entry_name
        HAVING 
            COUNT(*) > 0
        ORDER BY 
            game_weeks_won_total DESC
    """).to_df()


def get_worst_player_tally(duckdb_df):
    """
    This function looks at every game week, and calculates the worst player tally.
    The worst player is defined as the player who has the least points in that game week.
    Players should only exist in this if the number of times they are the worst player is greater than 0.

    Schema: player_name, entry_name, game_weeks_lost_total, game_weeks_lost_list, game_weeks_lost_dict
    e.g.
    | player_name | entry_name | game_weeks_lost_total | game_weeks_lost_list | game_weeks_lost_dict       |
    |-------------|------------|-----------------------|----------------------|----------------------------|
    | player1     | entry1     | 5                     | 1, 3, 5, 7, 9        | {1: 2, 3: 1, 5: 3, ...}    |
    | player2     | entry2     | 3                     | 2, 4, 6              | {2: 0, 4: 1, 6: 2}         |
    """
    return duckdb.query("""
        WITH worst_player AS (
            SELECT 
                event, 
                player_name, 
                entry_name, 
                event_points - event_transfers_cost AS net_points,
                RANK() OVER (PARTITION BY event ORDER BY net_points ASC) as rank
            FROM 
                duckdb_df
        )
        SELECT 
            player_name, 
            entry_name, 
            COUNT(*) AS game_weeks_lost_total,
            STRING_AGG(CAST(event AS VARCHAR), ', ' ORDER BY event) AS game_weeks_lost_list,
            MAP(LIST(event), LIST(net_points)) AS game_weeks_lost_dict
        FROM 
            worst_player
        WHERE 
            rank = 1
        GROUP BY 
            player_name, 
            entry_name
        HAVING 
            COUNT(*) > 0
        ORDER BY 
            game_weeks_lost_total DESC
    """).to_df()


def get_transfer_hits(duckdb_df):
    """
    This function returns a DataFrame for each game week showing which
    players have had event_transfer_costs greater than 0.
    """
    return duckdb.query("""
        SELECT 
            event, 
            player_name, 
            entry_name, 
            event_transfers_cost
        FROM 
            duckdb_df
        WHERE
            event_transfers_cost > 0
        ORDER BY
            event
    """).to_df()
