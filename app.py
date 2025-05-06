# ---------------------------------------------------------------
# Movie Ratings Analysis with PySpark - Streamlit Application
# Made by: Aditya Singh and Harsh Joshi
# License: MIT License
# Date: 2025-05-06

# Yeh application PySpark aur Streamlit ka use karke 
# MovieLens dataset par analysis karta hai.
# ---------------------------------------------------------------

"""
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣦⣶⣾⣿⣷⣶⣶⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣶⣿⣟⠯⠓⣉⣩⣭⣝⣻⣿⣶⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣾⣿⠗⢡⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⠏⣰⣿⣿⣿⣿⣿⣿⣿⠋⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⣿⣿⣿⢠⣿⣿⣿⣿⣿⠿⢿⣿⣀⣿⣿⣿⣿⣷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣸⣿⣿⣿⠋⠀⠀⠀⣨⣩⠉⠀⢹⣿⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⠻⣿⣿⣿⣿⡏⣀⣀⣀⣀⢧⣿⠂⣀⠀⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣶⣿⣿⠗⡤⢤⣀⡉⠊⡱⢋⣉⣉⣷⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⡝⣿⣿⠀⠈⠙⠿⠃⠀⡇⠽⠛⢻⡏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⣿⣿⣿⣿⣿⡀⠀⣀⠤⠾⣄⡹⣄⠀⢸⣧⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣋⢹⣿⣿⣷⡾⢄⠀⠀⠀⠀⢈⣶⣿⠿⣿⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⣇⣸⣿⣿⣿⣿⣿⡏⢻⣿⣿⣇⠈⠡⢄⣀⠐⢉⣿⣿⣴⣿⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⣿⡿⠟⢻⣿⣿⣿⠀⠀⠻⣿⣿⣷⣤⣄⣠⣴⣿⣿⣿⣿⣿⣿⣇⡀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣠⠔⠒⠒⠉⠀⠀⠀⣿⣿⣿⡇⠀⠀⠀⠉⠛⢿⣿⡿⠛⠋⠘⣿⣿⠿⢯⠛⡂⠤⢄⡀⠀⠀⠀⠀
⠀⣠⠊⠁⠀⠀⠀⠀⠀⠀⠀⢻⠁⠀⠸⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠀⠀⠀⠀⠀⡇⠀
⣰⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢇⠀⠀⢣⠒⠲⠤⣀⡀⠀⡀⣀⠤⠒⠂⠸⡀⠀⢱⠀⠀⠀⠀⠀⠀⠙⣄
⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢆⡀⠀⢣⡀⠀⠈⠀⠈⠀⠃⠀⠀⠀⠰⠧⠀⠚⠀⠀⠀⠀⠀⠀⠀⠙
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠐⠛⠲⠻⠷⠒⠞⠂⠸⠣⠲⠖⠖⠀⠂⠻⠻⠿⠼⠟⠿⠧⠿⠃⠀⠀⠀⠀⠀⠀⠀

"""

import streamlit as st
from src.spark_utils import (
    get_spark, load_data,
    task_a_lowest_avg_movie, task_b_top_users,
    task_c_rating_distribution, task_d_highest_rated_with_min_votes
)

# Streamlit app ka basic config set kar rahe hain
st.set_page_config(page_title="Movie Ratings Analysis", layout="wide")
st.title("Movie Ratings Analysis with PySpark")
st.caption("Built on PySpark | Code designed to perform movie rating analysis efficiently")

# Spark session initialize kar rahe hain
spark = get_spark()

# Data load karne ka spinner - user ko batata hai ki loading process chal raha hai
with st.spinner("Spark initialize kar rahe hain aur MovieLens dataset download kar rahe hain. Kripya thoda intazaar karein..."):
    ratings_df, movies_df = load_data(spark)

# Sidebar mein user ko option dete hain task select karne ke liye
st.sidebar.header("Query Task Chune")
task = st.sidebar.selectbox(
    "Analysis type select karein:",
    options=[
        "(a) Lowest Avg Rating",
        "(b) Top Rating Users",
        "(c) Ratings Over Time",
        "(d) Top Rated Movies (min ratings)"
    ]
)

# Result panel jahan output show hoga
st.markdown("### Result Panel")

# Task (a): Sabse kam average rating wali movie
if task == "(a) Lowest Avg Rating":
    st.subheader("Movie with the Lowest Average Rating")
    with st.spinner("Movie ko search kar rahe hain... Kripya thoda intezaar karein."):
        result = task_a_lowest_avg_movie(ratings_df, movies_df)
        st.success(result)

# Task (b): Sabse zyada movies rate karne wale users
elif task == "(b) Top Rating Users":
    st.subheader("Users Who Rated the Most Movies")
    with st.spinner("Users ke ratings count kar rahe hain... Thoda intazaar karein."):
        top_users = task_b_top_users(ratings_df)
        st.dataframe(top_users, use_container_width=True)

# Task (c): Time ke saath ratings ka trend
elif task == "(c) Ratings Over Time":
    st.subheader("Ratings Trend Over Time")
    with st.spinner("Timestamp values process ho rahe hain... Kripya pratiksha karein."):
        rating_dist = task_c_rating_distribution(ratings_df)
        rating_dist["date"] = rating_dist["date"].astype("datetime64[ns]")
        st.line_chart(rating_dist.set_index("date"))

# Task (d): High-rated movies jinke pass minimum 100 ratings hain
elif task == "(d) Top Rated Movies (min ratings)":
    st.subheader("Top Rated Movies (Threshold: 100+ ratings)")
    with st.spinner("Top rated movies fetch kar rahe hain... Thoda intazaar karein."):
        top_movies = task_d_highest_rated_with_min_votes(ratings_df, movies_df)
        st.dataframe(top_movies, use_container_width=True)
