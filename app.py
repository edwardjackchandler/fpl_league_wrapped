import streamlit as st
import plotly.express as px
from src.fpl_load import LeagueHistoryLoader
from src.questions import (
    get_biggest_difference,
    get_most_points_left_on_bench_week,
    get_total_points_left_on_bench,
    get_points_by_gameweek,
    get_most_frequent_last_rank,
)

def plot_total_points(df):
    fig = px.line(
        df,
        x="gameweek",
        y="points",
        color="entry_name",
        title="Total Points by Gameweek",
    )
    fig.update_xaxes(title_text='Gameweek')
    fig.update_yaxes(title_text='Total Points')
    fig.update_layout(autosize=True, height=800, width=800)  # Add this line
    st.plotly_chart(fig)


def plot_total_bench_points(df):
    df = df.sort_values(
        "total_points_on_bench", ascending=False
    )  # Order DataFrame from greatest to least points
    fig = px.bar(
        df, x="entry_name", y="total_points_on_bench", title="Most Points Left on Bench"
    )
    fig.update_xaxes(title_text='Entry Name')
    fig.update_yaxes(title_text='Points')
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
    fig.update_xaxes(title_text='Entry Name')
    fig.update_yaxes(title_text='Points')
    st.plotly_chart(fig)

def main():
    st.title("FPL League History Data")

    dev = False
    if dev:
        league_id = 665568
    else:
        if load_button and league_id.isdigit():
            st.markdown("temp_league_id = 665568")
            league_id = st.text_input("Enter league ID", "")
            load_button = st.button("Load Data")
            # Initialize LeagueHistoryLoader

        elif load_button:
            st.error("Please enter a valid league ID.")
    league_history_loader = LeagueHistoryLoader(int(league_id))

    # Load League Data
    df = league_history_loader.get_data()

    # st.markdown("# Player Ranked Last Most Frequently")
    # st.markdown("This section displays the player who has been ranked last for each week throughout the season the most times.")
    # most_frequent_last_rank_df = get_most_frequent_last_rank(df)
    # st.write(most_frequent_last_rank_df)

    # player_name = most_frequent_last_rank_df.iloc[0]['player_name']
    # st.markdown(f"The player who has been ranked last the most times is **{player_name}**.")

    st.markdown("# Points by Gameweek")
    st.markdown(
        "This section displays the points gained by each player for each gameweek."
    )
    points_by_gameweek_df = get_points_by_gameweek(df)
    plot_total_points(points_by_gameweek_df)

    st.markdown("# Total Points Left on Bench")
    st.markdown(
        "This section displays the total points left on the bench by each player."
    )
    total_bench_points_df = get_total_points_left_on_bench(df)
    st.write(total_bench_points_df)
    plot_total_bench_points(total_bench_points_df)  # Add this line

    st.markdown("# Most Points Left on Bench in a Week")
    st.markdown(
        "This section displays the most points left on the bench in a week by each player."
    )
    week_bench_points_df = get_most_points_left_on_bench_week(df)
    st.write(week_bench_points_df)
    plot_week_bench_points(week_bench_points_df)  # Add this line

    st.markdown("# Biggest Difference in Event Points")
    st.markdown(
        "This section displays the biggest difference in event points between any two players."
    )
    biggest_difference_df = get_biggest_difference(df)
    st.write(biggest_difference_df)


if __name__ == "__main__":
    main()
