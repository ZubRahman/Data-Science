import dask.dataframe as dd
import time

# Function to process the data (example: count trips per day)
def process_data(df):
    return df.groupby('Date')['Trips'].sum().compute()  # Dask performs lazy computation; .compute() forces evaluation

# Serial Processing function
def serial_processing(file_path):
    start_time = time.time()
    df = dd.read_csv(file_path)  # Dask reads CSV in chunks, useful for large files
    result = process_data(df)
    end_time = time.time()
    print(f"Serial Processing Time: {end_time - start_time:.2f} seconds")
    return result



if __name__ == "__main__":
    file_path = "Trips_Full Data.csv"
    print("Running Serial Processing...")
    serial_result = serial_processing(file_path)
    print("Result:\n", serial_result)
