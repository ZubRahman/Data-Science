# pandas
import pandas as pd
# parallel processing
import dask.dataframe as dd
from dask.distributed import Client, progress
# to see speed
import time
# histogram
import matplotlib.pyplot as plt
# scatterplot
import plotly.express as px
# because of Windows
from multiprocessing import freeze_support
#predict model
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
# freeze support
from multiprocessing import freeze_support
# cpu usage
import psutil
from psutil import *
import os

trips_by_distance_pandas = pd.read_csv("Trips_by_Distance.csv")
trips_full_data = pd.read_csv("Trips_Full Data.csv")



#pandas

def question1_pandas():
    # Question 1: How many people stay at home and the number of trips daily?

    # Summing the population staying at home
    population_staying_home = trips_by_distance_pandas['Population Staying at Home'].sum()

    # Summing the total number of trips (sum the appropriate columns)
    total_trips = trips_by_distance_pandas[['Number of Trips <1', 'Number of Trips 1-3', 'Number of Trips 3-5', 
                                    'Number of Trips 5-10', 'Number of Trips 10-25', 'Number of Trips 25-50', 
                                    'Number of Trips 50-100', 'Number of Trips 100-250', 'Number of Trips 250-500', 
                                    'Number of Trips >=500']].sum()
    total_trips = total_trips.sum()

    print(f"Population staying at home: {population_staying_home}")
    print(f"Total trips: {total_trips}")

def question2_pandas():
    # Question 2: How long and how many people travel daily?

    # Summing the number of trips for different distance categories in the `Trips_Full Data.csv`
    trip_distances = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Summing the number of people not staying at home
    people_not_staying_home = trips_full_data['People Not Staying at Home'].sum()

    print("\nTotal trips for each distance category:")
    print(trip_distances)
    print(f"\nTotal people not staying at home: {people_not_staying_home}")

def question3_pandas():
    # Question 3: Identify the dates that over 10 million people conduct 10-25 trips
    dates_10_25 = trips_by_distance_pandas[trips_by_distance_pandas['Number of Trips 10-25'] > 10_000_000]
    print("\nDates with more than 10 million trips for 10-25 miles:")
    print(dates_10_25[['Date', 'Number of Trips 10-25']])

def question4_pandas():
    # Question 4: if over 10 million conduct 50-100 trips
    dates_50_100 = trips_by_distance_pandas[trips_by_distance_pandas['Number of Trips 50-100'] > 10_000_000]
    print("\nDates with more than 10 million trips for 50-100 miles:")
    print(dates_50_100[['Date', 'Number of Trips 50-100']])

def question1_2_visual():
    trip_distances = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Summing the number of people not staying at home
    people_not_staying_home = trips_full_data['People Not Staying at Home'].sum()

    # creating the bar plot i.e.
    # Histogram 1: People staying at home vs Week
    plt.figure(figsize=(10, 5))
    plt.hist(trips_by_distance_pandas['Week'], 
            weights=trips_by_distance_pandas['Population Staying at Home'],
            bins=trips_by_distance_pandas['Week'].nunique(), 
            color='skyblue', edgecolor='black')
    plt.xlabel("Week")
    plt.ylabel("Number of People Staying at Home")
    plt.title("Histogram: People Staying at Home vs Week")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # barplot: People travelling vs Distance
    plt.figure(figsize=(10, 5))
    plt.bar(trip_distances.index, trip_distances.values, color='orange', edgecolor='black')
    plt.xlabel("Trip Distance Category")
    plt.ylabel("Number of People Traveling")
    plt.title("Bar Plot: People Traveling vs Distance")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def question3_4_visual():
    dates_10_25 = trips_by_distance_pandas[trips_by_distance_pandas['Number of Trips 10-25'] > 10_000_000]
    dates_50_100 = trips_by_distance_pandas[trips_by_distance_pandas['Number of Trips 50-100'] > 10_000_000]

    #Visualisation
    fig = px.scatter(
        x=dates_10_25['Date'], 
        y=dates_10_25['Number of Trips 10-25'],
        labels={'x': 'Date', 'y': 'Number of Trips (10-25 miles)'},
        title='Trips of 10-25 Miles Over Time'
    )
    fig.show()

    #Visualisation
    fig = px.scatter(
        x=dates_50_100['Date'], 
        y=dates_50_100['Number of Trips 50-100'],
        labels={'x': 'Date', 'y': 'Number of Trips (50-100 miles)'},
        title='Trips of 50-100 Miles Over Time'
    )
    fig.show()

dtype={'County Name': 'object',
       'Number of Trips': 'float64',
       'Number of Trips 1-3': 'float64',
       'Number of Trips 10-25': 'float64',
       'Number of Trips 100-250': 'float64',
       'Number of Trips 25-50': 'float64',
       'Number of Trips 250-500': 'float64',
       'Number of Trips 3-5': 'float64',
       'Number of Trips 5-10': 'float64',
       'Number of Trips 50-100': 'float64',
       'Number of Trips <1': 'float64',
       'Number of Trips >=500': 'float64',
       'Population Not Staying at Home': 'float64',
       'Population Staying at Home': 'float64',
       'State Postal Code': 'object'}

# Load data using Dask
trips_by_distance_dask = dd.read_csv("Trips_by_Distance.csv",dtype = dtype, blocksize="25MB",
    sample_rows=1000)

dtype1 = {
    'County Name': 'object',
    'Number of Trips': 'float64',
    'Number of Trips 1-3': 'float64',
    'Number of Trips 10-25': 'float64',
    'Number of Trips 100-250': 'float64',
    'Number of Trips 25-50': 'float64',
    'Number of Trips 250-500': 'float64',
    'Number of Trips 3-5': 'float64',
    'Number of Trips 5-10': 'float64',
    'Number of Trips 50-100': 'float64',
    'Number of Trips <1': 'float64',
    'Number of Trips >=500': 'float64',
    'Population Not Staying at Home': 'float64',
    'Population Staying at Home': 'float64',
    'State Postal Code': 'object'
}


trips_full_data_dask = dd.read_csv("Trips_Full Data.csv", dtype = dtype1)

def question1_dask(df):
    population_staying_home = df['Population Staying at Home'].sum().compute()
    
    trip_cols = ['Number of Trips <1', 'Number of Trips 1-3', 'Number of Trips 3-5', 
                 'Number of Trips 5-10', 'Number of Trips 10-25', 'Number of Trips 25-50', 
                 'Number of Trips 50-100', 'Number of Trips 100-250', 'Number of Trips 250-500', 
                 'Number of Trips >=500']
    total_trips = df[trip_cols].sum().sum().compute()

    print(f"[DASK] Population staying at home: {population_staying_home}")
    print(f"[DASK] Total trips: {total_trips}")

def question2_dask(df):
    trip_distances = df[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                         'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum().compute()
    people_not_staying_home = df['People Not Staying at Home'].sum().compute()

    print("\n[DASK] Total trips by distance:")
    print(trip_distances)
    print(f"[DASK] People not staying at home: {people_not_staying_home}")

def question3_dask(df):
    result = df[df['Number of Trips 10-25'] > 10_000_000][['Date', 'Number of Trips 10-25']].compute()
    print("\n[DASK] Dates with >10 million trips (10-25):")
    print(result)

def question4_dask(df):
    result = df[df['Number of Trips 50-100'] > 10_000_000][['Date', 'Number of Trips 50-100']].compute()
    print("\n[DASK] Dates with >10 million trips (50-100):")
    print(result)

def dask_parallel_processing():
    processors = [1,10, 20]
    processing_times = {}

    for n in processors:
        print(f"\n>>> Running with {n} processors")
        client = Client(memory_limit = '1GB', n_workers=n, threads_per_worker=1)
        #client = Client(n_workers=n, threads_per_worker = 1)
        #client = Client(memory_limit='1GB', n_workers=10, threads_per_worker=1)

        start = time.time()
        question1_dask(trips_by_distance_dask)
        question2_dask(trips_full_data_dask)
        question3_dask(trips_by_distance_dask)
        question4_dask(trips_by_distance_dask)
        end = time.time()

        duration = end - start
        processing_times[n] = duration
        print(f"[DASK] Time with {n} processors: {duration:.2f} seconds")

        client.close()

    print("\n[DASK] Summary of processing times:", processing_times)

def predict_model():
    # Aggregate total trips across distances
    trip_data = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles',
                                 'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Convert distance ranges into approximate midpoint values (in miles)
    distance_midpoints = np.array([13, 37.5, 75, 175, 375, 600]).reshape(-1, 1)  # X
    trip_counts = np.array(trip_data.values).reshape(-1, 1)  # y

    # Create and fit a polynomial regression model
    model = make_pipeline(PolynomialFeatures(degree=2), LinearRegression())
    model.fit(distance_midpoints, trip_counts)

    # Predict on a range of distances for simulation
    distance_range = np.linspace(0, 700, 100).reshape(-1, 1)
    predicted_trips = model.predict(distance_range)

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.scatter(distance_midpoints, trip_counts, color='red', label='Actual Data')
    plt.plot(distance_range, predicted_trips, label='Predicted Model', color='blue')
    plt.xlabel("Trip Distance (miles)")
    plt.ylabel("Number of Trips")
    plt.title("Simulated Travel Frequency vs Trip Distance")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def print_resource_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    cpu_percent = psutil.cpu_percent(interval=1)  # wait 1 second to get accurate value
    ram_used_mb = mem_info.rss / (1024 * 1024)

    print(f"CPU Usage: {cpu_percent:.2f}%")
    print(f"RAM Usage: {ram_used_mb:.2f} MB")

if __name__ == "__main__":
    freeze_support()
    
    # serial processing
    start_time = time.time()
    question1_pandas()
    question2_pandas()
    question3_pandas()
    question4_pandas()
    end_time = time.time()
    print(f"{end_time - start_time:.2f}seconds")
    print_resource_usage()

    dask_parallel_processing()
    print_resource_usage()

    #question1_2_visual()
    #question3_4_visual()

    #predict_model()
