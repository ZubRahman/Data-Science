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

trips_by_distance = pd.read_csv("Trips_by_Distance.csv")
trips_full_data = pd.read_csv("Trips_Full Data.csv")

def question1():
    # Question 1: How many people stay at home and the number of trips daily?

    # Summing the population staying at home
    population_staying_home = trips_by_distance['Population Staying at Home'].sum()

    # Summing the total number of trips (sum the appropriate columns)
    total_trips = trips_by_distance[['Number of Trips <1', 'Number of Trips 1-3', 'Number of Trips 3-5', 
                                    'Number of Trips 5-10', 'Number of Trips 10-25', 'Number of Trips 25-50', 
                                    'Number of Trips 50-100', 'Number of Trips 100-250', 'Number of Trips 250-500', 
                                    'Number of Trips >=500']].sum()
    total_trips = total_trips.sum()

    print(f"Population staying at home: {population_staying_home}")
    print(f"Total trips: {total_trips}")

def question2():
    # Question 2: How long and how many people travel daily?

    # Summing the number of trips for different distance categories in the `Trips_Full Data.csv`
    trip_distances = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Summing the number of people not staying at home
    people_not_staying_home = trips_full_data['People Not Staying at Home'].sum()

    print("\nTotal trips for each distance category:")
    print(trip_distances)
    print(f"\nTotal people not staying at home: {people_not_staying_home}")

def question1_2_visual():
    trip_distances = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Summing the number of people not staying at home
    people_not_staying_home = trips_full_data['People Not Staying at Home'].sum()

    # creating the bar plot i.e.
    # Histogram 1: People staying at home vs Week
    plt.figure(figsize=(10, 5))
    plt.hist(trips_by_distance['Week'], 
            weights=trips_by_distance['Population Staying at Home'],
            bins=trips_by_distance['Week'].nunique(), 
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

def question3():
    # Question 3: Identify the dates that over 10 million people conduct 10-25 trips
    dates_10_25 = trips_by_distance[trips_by_distance['Number of Trips 10-25'] > 10_000_000]
    print("\nDates with more than 10 million trips for 10-25 miles:")
    print(dates_10_25[['Date', 'Number of Trips 10-25']])

def question4():
    # Question 4: if over 10 million conduct 50-100 trips
    dates_50_100 = trips_by_distance[trips_by_distance['Number of Trips 50-100'] > 10_000_000]
    print("\nDates with more than 10 million trips for 50-100 miles:")
    print(dates_50_100[['Date', 'Number of Trips 50-100']])

def question3_4_visual():
    dates_10_25 = trips_by_distance[trips_by_distance['Number of Trips 10-25'] > 10_000_000]
    dates_50_100 = trips_by_distance[trips_by_distance['Number of Trips 50-100'] > 10_000_000]

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
        y=dates_50_100['Number of Trips 10-25'],
        labels={'x': 'Date', 'y': 'Number of Trips (50-100 miles)'},
        title='Trips of 50-100 Miles Over Time'
    )
    fig.show()

def parallel_processing():
    freeze_support()
    n_processors = [10, 20]
    n_processors_time = {}

    for processor in n_processors:
        print(f"\n\n\nStarting computation with {processor} processors...\n\n\n")
        client = Client(n_workers = processor)
        start = time.time()
        
        question1()
        question2()
        question3()
        question4()
        
        endtime = time.time()

        dask_time = endtime - start
        n_processors_time[processor] = dask_time
        print(f"\n\n\nTime with {processor} processors: {dask_time} seconds\n\n\n")
        # Close the client after computation
        client.close()
        print("\n\n\n", n_processors_time, "\n\n\n")

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


if __name__ == "__main__":

    # serial processing
    """start_time = time.time()
    question1()
    question2()
    question3()
    question4()
    end_time = time.time()
    print(f"{end_time - start_time:.2f}seconds")
    
    """

    #question1_2_visual()
    #question3_4_visual()

    #parallel_processing()
    predict_model()
