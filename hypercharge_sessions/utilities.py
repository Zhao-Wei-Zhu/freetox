from pyspark.sql.functions import *
from pyspark.sql.types import *
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Function to calculate percentage of null values
def null_percentage(df):
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    total_count = df.count()
    return {k: v/total_count*100 for k, v in null_counts.asDict().items()}

def display_null_percentages(null_counts_dict):
  for col, percentage in null_counts_dict.items():
    print(f"{col:30s}{percentage:.2f}%")

def detect_outliers(df):
    outlier_dfs = []
    for col_name in df.schema.names:
        col_type = df.schema[col_name].dataType
        if col_type == IntegerType() or col_type == LongType() or col_type == FloatType() or col_type == DoubleType():
            quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
            IQR = quantiles[1] - quantiles[0]
            lower_bound = quantiles[0] - 1.5 * IQR
            upper_bound = quantiles[1] + 1.5 * IQR
            outlier_df = df.filter((F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound))
            outlier_dfs.append(outlier_df)
    return outlier_dfs

geolocator = Nominatim(user_agent="hypercharge_sessions", timeout=10)
# Function to get the address details from latitude and longitude
# It calls the geolocator once per row and returns all relevant fields
def get_address_from_coords(lat, long, retries=3, delay=1):
    try:
        for i in range(retries):
            try:
                location = geolocator.reverse(f"{lat}, {long}")
                if location and location.raw:
                    address = location.raw['address']
                    
                    # Check if 'town' or 'village' exists in the address
                    town = address.get('town', None)
                    village = address.get('village', None)
                    
                    # Priority goes to 'town', but if not present, use 'village'
                    town_or_village = town if town else village
                    
                    return [
                        address.get('road', None),          # locationStreet
                        address.get('postcode', None),      # locationZipCode
                        town_or_village,                    # locationTown
                        address.get('state', None)          # locationProvince (it's really the regione tho)
                    ]
            except (GeocoderTimedOut, GeocoderServiceError):
                time.sleep(delay)  # Wait before retrying
                continue
        # Return None if retries fail
        return [None, None, None, None]
            
    except Exception as e:
        # Log or print the exception for debugging purposes
        print(f"Error: {e}")
        return [None, None, None, None]

# Define UDF to return a list of address fields (street, zip code, town, province)
@udf(returnType=ArrayType(StringType()))
def get_address(lat, long):
    return get_address_from_coords(lat, long)