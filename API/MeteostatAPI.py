import pandas as pd
import json
from datetime import datetime
from meteostat import Hourly, Daily

class MeteostatAPI:
    def __init__(self, start_date, end_date):
        # Converting the string dates to datetime objects
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.path_to_save = './data/raw/base_3/'

    def process_hourly_data(self, station):
        # Fetching hourly data for the station
        data = Hourly(station, self.start_date, self.end_date)
        data = data.fetch()
        return data

    def process_daily_data(self, station):
        # Fetching daily data for the station
        data = Daily(station, self.start_date, self.end_date)
        data = data.fetch()
        return data

    def fetch_weather_data(self, city, station):
        print(f'===================={city}====================')
        
        # Process and display hourly data
        hourly_df = pd.DataFrame(self.process_hourly_data(station))
        print("Hourly Data:")
        print(hourly_df.head(5))

        # Process and display daily data
        daily_df = pd.DataFrame(self.process_daily_data(station))
        print("Daily Data:")
        print(daily_df.head(5))
        
        return daily_df, hourly_df

    def get_weather_data_for_all_cities(self, locations):
        # Loop through all cities and fetch the data
        for city, station in locations.items():
            print(f'City: {city} Station: {station}')
            daily_df, hourly_df = self.fetch_weather_data(city, station)
            # Save the data to CSV files
            self.save_data_frame_to_csv(daily_df, self.path_to_save, city, 'daily')
            self.save_data_frame_to_csv(hourly_df, self.path_to_save, city, 'hourly')

    def save_data_frame_to_csv(self, data_frame, file_path, city_name, data_type):
        """Save a pandas DataFrame to a CSV file with a personalized name based on the city."""
        name_file = f"{file_path}{city_name}_{data_type}_1973_2024.csv"
        # Save the DataFrame to the file
        data_frame.to_csv(name_file, index=True)

        
def load_stations_from_file(file_path):
    """Load cities and coordinates from a JSON file."""
    with open(file_path, 'r') as file:
        data = json.load(file)
        # Create a dictionary with the city as key and the station as value
        locations = {entry['city']: entry['sation'] for entry in data}  # Corrected 'sation' to 'station'
    return locations

if __name__ == '__main__':
    # Load stations from the JSON file
    file_path = './configs/stations.json'  # Path to your JSON file with city data
    locations = load_stations_from_file(file_path)
    print(locations)
    
    # Initialize the weather data fetcher
    weather_fetcher = MeteostatAPI(start_date="1973-01-01", end_date="2024-12-31")
    # Fetch and process weather data for all cities
    weather_fetcher.get_weather_data_for_all_cities(locations)
    #print(datetime(2018, 1, 1))
