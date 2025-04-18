import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import json

class OpenMeteoAPI:
    def __init__(self, start_date, end_date, timezone="America/Sao_Paulo"):
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = timezone
        self.path_to_save = './data/raw/base_1/'

        # Setup the Open-Meteo API client with cache and retry on error
        self.cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)

        self.url = "https://archive-api.open-meteo.com/v1/archive"
        self.params = {
            "hourly": ["relative_humidity_2m", "pressure_msl", "surface_pressure"],
            "daily": [
                "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "rain_sum", "showers_sum", 
                "snowfall_sum", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant"
            ],
            "timezone": self.timezone,
            "start_date": self.start_date,
            "end_date": self.end_date
        }

    def fetch_weather_data(self, latitude, longitude):
        """Fetch weather data from Open-Meteo API."""
        self.params["latitude"] = latitude
        self.params["longitude"] = longitude
        responses = self.openmeteo.weather_api(self.url, params=self.params)
        return responses[0]  # Assuming we have only one response for the given coordinates

    def process_hourly_data(self, response):
        """Process hourly weather data."""
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }
        hourly_data["relative_humidity_2m"] = hourly.Variables(0).ValuesAsNumpy()
        hourly_data["pressure_msl"] = hourly.Variables(1).ValuesAsNumpy()
        hourly_data["surface_pressure"] = hourly.Variables(2).ValuesAsNumpy()

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        return hourly_dataframe

    def process_daily_data(self, response):
        """Process daily weather data."""
        daily = response.Daily()
        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )
        }
        daily_data["temperature_2m_max"] = daily.Variables(0).ValuesAsNumpy()
        daily_data["temperature_2m_min"] = daily.Variables(1).ValuesAsNumpy()
        
        daily_data["temperature_2m_mean"] = daily.Variables(2).ValuesAsNumpy()
        daily_data["rain_sum"] = daily.Variables(3).ValuesAsNumpy()
        daily_data["showers_sum"] = daily.Variables(4).ValuesAsNumpy()
        daily_data["snowfall_sum"] = daily.Variables(5).ValuesAsNumpy()
        daily_data["wind_speed_10m_max"] = daily.Variables(6).ValuesAsNumpy()
        daily_data["wind_gusts_10m_max"] = daily.Variables(7).ValuesAsNumpy()
        daily_data["wind_direction_10m_dominant"] = daily.Variables(8).ValuesAsNumpy()

        daily_dataframe = pd.DataFrame(data=daily_data)
        return daily_dataframe

    def display_location_info(self, response, city):
        """Display basic information about the location."""
        print(f"Weather data for {city}:")
        print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation: {response.Elevation()} m asl")
        print(f"Timezone: {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()} s")

    def get_weather_data_for_city(self, city, latitude, longitude):
        """Fetch, process, and display the weather data for a single city."""
        response = self.fetch_weather_data(latitude, longitude)

        # Display location info
        self.display_location_info(response, city)

        # Process and display hourly data
        hourly_df = pd.DataFrame(self.process_hourly_data(response))
        print("Hourly Data:")
        print(hourly_df.head(5))

        # Process and display daily data
        daily_df = pd.DataFrame(self.process_daily_data(response))
        print("Daily Data:")
        print(daily_df.head(5))
        return daily_df,hourly_df

    def get_weather_data_for_all_cities(self, locations):
        """Fetch and display weather data for all cities in the provided dictionary."""
        for city, (latitude, longitude) in locations.items():
            daily_df,hourly_df=self.get_weather_data_for_city(city, latitude, longitude)
            self.save_data_frame_to_csv(daily_df,self.path_to_save,city,data_type="daily")
            self.save_data_frame_to_csv(hourly_df,self.path_to_save,city,data_type="hourly")
    def save_data_frame_to_csv(self,data_frame, file_path, city_name,data_type):
        """Save a pandas DataFrame to a CSV file with a personalized name based on city."""
        # Criação do nome do arquivo com base no city_name
        name_file = f"{file_path}{city_name}_{data_type}_1973_2024.csv"
    
        # Salva o DataFrame com o nome do arquivo personalizado
        data_frame.to_csv(name_file, index=False)



def load_locations_from_file(file_path):
    """Load cities and coordinates from a JSON file."""
    with open(file_path, 'r') as file:
        data = json.load(file)
        locations = {entry['city']: tuple(entry['coordinates']) for entry in data}
    return locations



# Example usage:
if __name__ == "__main__":
    # Load locations from the file
    file_path = 'citys.json'  # Path to your JSON file with city data
    locations = load_locations_from_file(file_path)
    print(locations)

    # Initialize the weather data fetcher
    weather_fetcher = OpenMeteoAPI(start_date="1973-01-01", end_date="2024-12-31")

    # Fetch and display weather data for all cities in the list
    weather_fetcher.get_weather_data_for_all_cities(locations)
