📁 Files Used
Trips_by_Distance.csv:
Trips_Full Data.csv: 

## Features
Serial and parallel data processing for performance comparison

CPU and RAM usage tracking

Data visualization with Matplotlib and Plotly

Predictive modeling of travel frequency using polynomial regression

CSV export of Dask processing times

Dask dashboard for monitoring parallel execution

## Visualizations
Histogram: People staying at home vs week
Bar Plot: Trip counts by distance
Scatter Plots: 10–25 and 50–100 mile trip counts over time
Processing time comparisons: Serial vs Dask (1, 10, 20 processors)
Polynomial Regression: Predicting travel frequency from distance

## Libraries used
Pandas: Serial data processing

Dask: Parallel computing

Matplotlib / Plotly: Data visualization

Scikit-learn: Regression modeling

Psutil: System resource monitoring

Multiprocessing: Windows compatibility

## How to Run
Install Dependencies:

pip install pandas dask[complete] matplotlib plotly scikit-learn psutil

## Run the script:
python3 code.py

This will:

Process data using Pandas (serial)

Launch Dask clusters and process in parallel

Plot processing time comparisons

Visualize patterns and predictions

## Output
Dask processing times saved in dask_processing_times.csv

Multiple visual charts shown during execution

Terminal outputs of analytical results

## Notes
The script opens the Dask dashboard on localhost:8790 for real-time monitoring.

Ensure both CSV files are in the same directory as the script.

🧠 Author
Project for Coventry University - Data Science Module (5004CMD)
Mohammad Zubayer Rahman
