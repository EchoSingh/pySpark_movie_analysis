# pySPARK Movie Analysis

<img src="https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdXRpdG9keGF2czExMzRvc3N6YTZqY2MzaHZ6NG81dTE0czVheDFkdSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/KxscqRylVTBty6bUIS/giphy.gif" width="100" alt="Banner"/>

Movie Ratings Analysis using PySpark and Streamlit.  
This project analyzes the MovieLens 20M dataset using PySpark, with interactive visualizations provided by Streamlit. Additionally, a Kaggle notebook offers more insights into the analysis.

## Features
- **Lowest Rated Movie**: Identify the movie with the lowest average rating.
- **Top Users**: Determine the top users based on the number of ratings provided.
- **Rating Distribution**: Visualize how ratings are distributed over time.
- **Top Rated Movies**: List movies with high average ratings given a minimum number of votes.
- **Controversial Movies**: Highlight movies with high rating variance.
- **Average Rating by Genre**: Analyze average ratings across different movie genres.

## Installation

1. Clone the repository.
2. Install dependencies from [requirements.txt](requirements.txt):
    ```sh
    pip install -r requirements.txt
    ```
## File Structure

- `app.py` - Main Streamlit application.
- `src/spark_utils.py` - Utility functions for PySpark data processing (contains tasks a, b, c, d).
- `outputs/` - Contains images illustrating query results:
  - `query(a).jpg` - Lowest rated movie.
  - `query(b).jpg` - Top rating users.
  - `query(c).jpg` - Ratings distribution over time.
  - `query(d).jpg` - Top rated movies with minimum votes.
- `requirements.txt` - List of project dependencies.
- `LICENSE` - Project license.
- `README.md` - This file.

## Kaggle Notebook

For additional insights and analysis, check out my Kaggle notebook [here](https://www.kaggle.com/code/adi2606/spark-movie-analysis/notebook).

## Usage

To run the application, execute the following command in your terminal:
```sh
streamlit run app.py
```

## LICENSE
This project is licensed under the MIT License.
