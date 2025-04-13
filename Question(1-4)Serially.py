import pandas as pd
import time

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

    # Question 2: How long and how many people travel daily?

    # Summing the number of trips for different distance categories in the `Trips_Full Data.csv`
    trip_distances = trips_full_data[['Trips 1-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles', 
                                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']].sum()

    # Summing the number of people not staying at home
    people_not_staying_home = trips_full_data['People Not Staying at Home'].sum()

    print("\nTotal trips for each distance category:")
    print(trip_distances)
    print(f"\nTotal people not staying at home: {people_not_staying_home}")

    # creating the bar plot i.e.
    import matplotlib.pyplot as plt
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
    # Filter dates where the number of trips is greater than 10 million for 10-25 and 50-100 trips
    # Filter out rows where the total number of trips exceeds 10 million
    dates_10_25 = trips_by_distance[trips_by_distance['Number of Trips 10-25'] > 10_000_000]
    print("\nDates with more than 10 million trips for 10-25 miles:")
    print(dates_10_25[['Date', 'Number of Trips 10-25']])

    import plotly.express as px
    fig = px.scatter(x= df_new["Date"], y= df_new[' Number of Trips 10-25'])
    fig.show()
    # x and y given as array_like objects
    import plotly.express as px
    fig = px.scatter(x= df_new["Date"], y= df_new[' Number of Trips 50-100'])
    fig.show()
    


def question4():
    # Question 4: if over 10 million conduct 50-100 trips
    dates_50_100 = trips_by_distance[trips_by_distance['Number of Trips 50-100'] > 10_000_000]

    print("\nDates with more than 10 million trips for 50-100 miles:")
    print(dates_50_100[['Date', 'Number of Trips 50-100']])
    



start_time = time.time()
question1()
#question2()
#question3()
#question4()
end_time = time.time()
print(f"{end_time - start_time:.2f}seconds")
