import base64

import bar_chart_race as bcr
import plotly.express as px
import streamlit as st

from src.fpl_load import LeagueHistoryLoader
from src.questions import (
    get_best_player_tally,
    get_biggest_difference,
    get_boring,
    get_most_points_left_on_bench_week,
    get_player_best_rank_event,
    get_player_worst_rank_event,
    get_points_by_gameweek,
    get_total_points_and_bench_points,
    get_total_points_left_on_bench,
    get_transfer_hits,
    get_worst_player_tally,
)


@st.cache_data
def plot_graph_race(df_data):
    """
    Create a League Title Race video using bar_chart_race.
    """
    #TODO - Customize the parameters for different league sizes
    df_values, df_ranks = bcr.prepare_long_data(df_data, 
                index='gameweek', 
                columns='entry_name', 
                values='points', 
                steps_per_period=1)

    return bcr.bar_chart_race(df_values,
                n_bars=16, 
                steps_per_period=30, 
                period_length=1500, 
                title = 'League Race', 
                period_template='{x:.0f}', 
                fixed_max=True, 
                filter_column_colors=True).data

def plot_total_points(df):
    fig = px.line(
        df,
        x="gameweek",
        y="points",
        color="entry_name",
        title="Total Points by Gameweek",
    )
    fig.update_xaxes(title_text="Gameweek")
    fig.update_yaxes(title_text="Total Points")
    fig.update_layout(autosize=True, height=800, width=800)  # Add this line
    st.plotly_chart(fig)


def plot_total_bench_points(df):
    df = df.sort_values(
        "bench_points", ascending=False
    )  # Order DataFrame from greatest to least points
    fig = px.bar(
        df, x="entry_name", y="bench_points", title="Most Points Left on Bench"
    )
    fig.update_xaxes(title_text="Entry Name")
    fig.update_yaxes(title_text="Points")
    st.plotly_chart(fig)


def plot_week_bench_points(df):
    df = df.sort_values(
        "most_points_left_on_bench", ascending=False
    )  # Order DataFrame from greatest to least points
    fig = px.bar(
        df,
        x="entry_name",
        y="most_points_left_on_bench",
        title="Most Points Left on Bench in a Week",
    )
    fig.update_xaxes(title_text="Entry Name")
    fig.update_yaxes(title_text="Points")
    st.plotly_chart(fig)


def plot_total_vs_bench_points(df):
    # Create a new column that is the sum of total_points and bench_points
    df["total_and_bench_points"] = df["total_points"] + df["bench_points"]

    # Sort DataFrame by total_and_bench_points from greatest to least
    df = df.sort_values("total_and_bench_points", ascending=False)

    fig = px.bar(
        df,
        x="entry_name",
        y=["total_points", "bench_points"],
        title="Total Points vs Points Left on Bench",
    )
    fig.update_xaxes(title_text="Entry Name")
    fig.update_yaxes(title_text="Points")
    st.plotly_chart(fig)


def main():
    st.title("FPL League Wrapped")

    default_league_id = 741068
    league_id = st.text_input("Enter league ID", value=default_league_id)
    load_button = st.button("Load Data")

    if load_button:
        if not league_id.isdigit():
            st.error("Please enter a valid league ID.")
        # Initialize LeagueHistoryLoader
        league_history_loader = LeagueHistoryLoader(int(league_id))

        game_week_points = get_points_by_gameweek(league_history_loader.get_data())
        display_data(league_history_loader)

        st.markdown("## Title Race Video")
        st.markdown("Video may take a minute to load.")
        html_str = plot_graph_race(game_week_points)
        start = html_str.find('base64,')+len('base64,')
        end = html_str.find('">')

        video = base64.b64decode(html_str[start:end])
        st.video(video)



def display_data(league_history_loader: LeagueHistoryLoader):
    # Load League Data
    df = league_history_loader.get_data()

    # Display the max game week
    st.markdown(f"## Data Refreshed for GW {df['event'].max()}")

    st.markdown("## Best and Worst Players")

    st.markdown("### Tickets To The Bottom Feeder Raffle")
    worst_players = get_worst_player_tally(df)
    st.write(worst_players)

    st.markdown("### Gameweeks Won")
    best_players = get_best_player_tally(df)
    st.write(best_players)

    st.markdown("### BORING")
    st.markdown("![Alt Text](https://media1.tenor.com/m/513CjqCC3_sAAAAd/boring-nigel-farage.gif)")

    boring_players = get_boring(df)
    st.write(boring_players)

    st.markdown("## Transfer Hits")
    st.markdown(
        "This section displays the total transfer hits taken by each player."
    )
    transfer_hits_df = get_transfer_hits(df)
    st.write(transfer_hits_df)

    st.markdown("## Points by Gameweek")
    st.markdown(
        "This section displays the points gained by each player for each gameweek."
    )
    points_by_gameweek_df = get_points_by_gameweek(df)
    plot_total_points(points_by_gameweek_df)

    st.markdown("## Player's Worst Rank")
    st.markdown(
        "This section displays a player's best rank across the whole season."
    )
    player_worst_rank = get_player_worst_rank_event(df)
    # todo - Make sure dataframe displays properly
    st.table(player_worst_rank)

    st.markdown("## Player's Best Rank")
    st.markdown(
        "This section displays a player's best rank across the whole season."
    )
    player_best_rank = get_player_best_rank_event(df)
    st.table(player_best_rank)

    st.markdown("## Total Points Left on Bench")
    st.markdown(
        "This section displays the total points left on the bench by each player."
    )
    total_bench_points_df = get_total_points_left_on_bench(df)
    # st.write(total_bench_points_df)
    plot_total_bench_points(total_bench_points_df)

    st.markdown("## Total Points vs Points Left on Bench")
    st.markdown(
        "This section displays the total points and points left on the bench by each player."
    )
    total_points_and_bench_points = get_total_points_and_bench_points(df)
    # st.write(total_points_and_bench_points)
    plot_total_vs_bench_points(total_points_and_bench_points)

    st.markdown("## Most Points Left on Bench in a Week")
    st.markdown(
        "This section displays the most points left on the bench in a week by each player."
    )
    week_bench_points_df = get_most_points_left_on_bench_week(df)
    # st.write(week_bench_points_df)
    plot_week_bench_points(week_bench_points_df)

    st.markdown("## Biggest Difference in Event Points")
    st.markdown(
        "This section displays the biggest difference in event points between any two players."
    )
    biggest_difference_df = get_biggest_difference(df)
    st.write(biggest_difference_df)


if __name__ == "__main__":
    main()
