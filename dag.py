import warnings
warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error


def descriptive_analysis():
    title_basics = pd.read_csv('data/title.basics.tsv', sep='\t')
    
    # Replace '\\N' with NaN
    title_basics['startYear'] = title_basics['startYear'].replace('\\N', np.nan)
    
    # Convert to integer, handling NaN values
    title_basics['startYear'] = title_basics['startYear'].dropna().astype(int)
    
    plt.figure(figsize=(10, 6))
    title_basics['titleType'].value_counts().plot(kind='bar')
    plt.title('Distribution of Title Types')
    plt.xlabel('Title Type')
    plt.ylabel('Count')
    plt.savefig('output/distribution_title_types.png')
    plt.close()

    genres_exploded = title_basics['genres'].str.split(',').explode()
    plt.figure(figsize=(12, 6))
    genres_exploded.value_counts().plot(kind='bar')
    plt.title('Distribution of Genres')
    plt.xlabel('Genres')
    plt.ylabel('Count')
    plt.savefig('output/distribution_genres.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    title_basics['startYear'].plot(kind='hist', bins=20)
    plt.title('Distribution of Start Years')
    plt.xlabel('Year')
    plt.ylabel('Frequency')
    plt.savefig('output/distribution_start_years.png')
    plt.close()


def relationship_analysis():
    title_basics = pd.read_csv('data/title.basics.tsv', sep='\t')
    title_ratings = pd.read_csv('data/title.ratings.tsv', sep='\t')
    merged_ratings = pd.merge(title_basics, title_ratings, on='tconst', how='inner')
    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=merged_ratings, x='runtimeMinutes', y='averageRating', hue='genres')
    plt.title('Runtime vs. Ratings by Genre')
    plt.xlabel('Runtime (Minutes)')
    plt.ylabel('Average Rating')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.savefig('output/runtime_vs_ratings.png')
    plt.close()

    title_crew = pd.read_csv('data/title.crew.tsv', sep='\t')
    crew_ratings = pd.merge(title_crew, title_ratings, on='tconst', how='inner')
    crew_ratings['numDirectors'] = crew_ratings['directors'].apply(lambda x: len(x.split(',')) if pd.notnull(x) else 0)
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=crew_ratings, x='numDirectors', y='averageRating')
    plt.title('Number of Directors vs. Ratings')
    plt.xlabel('Number of Directors')
    plt.ylabel('Average Rating')
    plt.savefig('output/directors_vs_ratings.png')
    plt.close()


    #def network_analysis():
    #title_crew = pd.read_csv('data/title.crew.tsv', sep='\t')
    #collaborations = []
    #for _, row in title_crew.iterrows():
    #    if pd.notnull(row['directors']) and pd.notnull(row['writers']):
    #        directors = row['directors'].split(',')
    #        writers = row['writers'].split(',')
    #        collaborations.extend([(d, w) for d in directors for w in writers])

    #G = nx.Graph()
    #G.add_edges_from(collaborations)
    #plt.figure(figsize=(12, 12))
    #nx.draw(G, node_size=10, alpha=0.7)
    #plt.title('Director-Writer Collaboration Network')
    #plt.savefig('output/collaboration_network.png')
    #plt.close()


def temporal_analysis():
    title_basics = pd.read_csv('data/title.basics.tsv', sep='\t')
    
    # Replace '\\N' with NaN
    title_basics['startYear'] = title_basics['startYear'].replace('\\N', np.nan)
    
    # Convert to integer, handling NaN values
    title_basics['startYear'] = title_basics['startYear'].dropna().astype(int)
    
    genres_by_year = title_basics.dropna(subset=['startYear']).copy()
    genres_by_year = genres_by_year.explode('genres').groupby(['startYear', 'genres']).size().unstack(fill_value=0)
    genres_by_year.plot.area(figsize=(12, 6), alpha=0.6)
    plt.title('Trends in Genres Over Time')
    plt.xlabel('Year')
    plt.ylabel('Frequency')
    plt.savefig('output/genres_trends.png')
    plt.close()

def cross_comparisons():
    title = pd.read_csv('data/title.akas.tsv', sep='\t')
    title_region_genres = title.groupby(['region', 'types']).size().unstack(fill_value=0)
    plt.figure(figsize=(12, 6))
    sns.heatmap(title_region_genres, cmap='viridis')
    plt.title('Region vs. Types Popularity')
    plt.xlabel('Types')
    plt.ylabel('Region')
    plt.savefig('output/region_vs_types.png')
    plt.close()

def anomaly_detection():
    title_ratings = pd.read_csv('data/title.ratings.tsv', sep='\t')
    ratings_outliers = title_ratings[title_ratings['numVotes'] < 100].sort_values(by='averageRating', ascending=False).head(10)
    print("Highly Rated but Under-Voted Titles:")
    print(ratings_outliers)
    ratings_outliers.to_csv('output/highly_rated_under_voted_titles.csv', index=False)

# Define functions for each analysis tas
# Define DAG
with DAG(
    dag_id='data_engineering_workshop_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 12, 13),
        'retries': 1,
    },
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='descriptive_analysis',
        python_callable=descriptive_analysis
    )

    task2 = PythonOperator(
        task_id='relationship_analysis',
        python_callable=relationship_analysis
    )

    #task3 = PythonOperator(
        #task_id='network_analysis',
        #python_callable=network_analysis
    #)

    task4 = PythonOperator(
        task_id='temporal_analysis',
        python_callable=temporal_analysis
    )

    task5 = PythonOperator(
        task_id='cross_comparisons',
        python_callable=cross_comparisons
    )

    task6 = PythonOperator(
        task_id='anomaly_detection',
        python_callable=anomaly_detection
    )

    #task7 = PythonOperator(
        #task_id='predictive_analysis',
        #python_callable=predictive_analysis
    #)

    # Set task dependencies
    task1 >> [task2, task4, task5, task6]

