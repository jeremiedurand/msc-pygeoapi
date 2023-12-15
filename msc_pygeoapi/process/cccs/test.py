from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json

from msc_pygeoapi.env import (
    MSC_PYGEOAPI_ES_USERNAME,
    MSC_PYGEOAPI_ES_PASSWORD,
)

es = Elasticsearch('https://geomet-dev-22.cmc.ec.gc.ca:443/elasticsearch', basic_auth=("elasticSearch", "Es4tkag"))
es.ping()

################################################
### FUNCTIONS                    
################################################
def build_query(mapping, bbox=None, station_ids=None, years=None, months=None, days=None, hours=None):
    try:
        # Initialize an empty query
        query = {
            #"size": size if size else 10000,
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        # Add conditions based on input values
        if bbox:  # bbox filter
            bbox_query = {
                "geo_bounding_box": {
                    "geometry": {
                        "top_left": {
                            "lat": bbox["top_left"]["lat"],
                            "lon": bbox["top_left"]["lon"]
                        },
                        "bottom_right": {
                            "lat": bbox["bottom_right"]["lat"],
                            "lon": bbox["bottom_right"]["lon"]
                        }
                    }
                }
            }
            query["query"]["bool"]["filter"] = bbox_query

        if station_ids:  # station ID filter
            query["query"]["bool"]["must"].append({"terms": {"properties." + mapping['STN_IDName']: station_ids}})

        if years:  # year filter
            query["query"]["bool"]["must"].append({"terms": {"properties." + mapping['yearName']: years}})

        if months:  # month filter
            query["query"]["bool"]["must"].append({"terms": {"properties." + mapping['monthName']: months}})

        if 'dayName' in mapping and days:  # day filter
            query["query"]["bool"]["must"].append({"terms": {"properties." + mapping['dayName']: days}})

        if 'hourName' in mapping and hours: # hour filter
            query["query"]["bool"]["must"].append({"terms": {"properties." + mapping['hourName']: hours}})

        return query
    
    except Exception as e:
        print(f"An error occurred in build_query: {e}")
        raise

def calculate_missing_data(es_hits, data_field):
    try:
        missing_data_count = 0
        # Check if the data field is missing in the document
        for hit in es_hits:
            # Access the document source
            document = hit['_source']
            field_value = document['properties'][data_field]  # Field to perform the stats calculations
            if field_value is None:
                missing_data_count += 1

        return missing_data_count
    except Exception as e:
        print(f"An error occurred in calculate_missing_data: {e}")
        raise

def calculate_missing_days(data):
    try:
        missing_days = 0

        for value in data:
            if value is None:
                missing_days += 1

        return missing_days
    except Exception as e:
        print(f"An error occurred in calculate_missing_days: {e}")
        raise
    
def calculate_consecutive_missing_days(data):
    try:
        consecutive_missing_days = 0
        max_consecutive_missing_days = 0

        for value in data:
            if value is None:
                consecutive_missing_days += 1
                max_consecutive_missing_days = max(max_consecutive_missing_days, consecutive_missing_days)
            else:
                consecutive_missing_days = 0

        return max_consecutive_missing_days
    
    except Exception as e:
        print(f"An error occurred in calculate_consecutive_missing_days: {e}")
        raise

def check_missing_data_options(input_missing_data_option, percentage_missing_data, missing_days, consecutive_missing_days):
    try:
        if input_missing_data_option == None:
            missing_data_option_status = "None"

        elif input_missing_data_option == 5:
            if percentage_missing_data > input_missing_data_option:
                missing_data_option_status = (f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
                #raise Exception(f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
            else:
                missing_data_option_status = "OK"

        elif input_missing_data_option == 10:
            if percentage_missing_data > input_missing_data_option:
                missing_data_option_status = (f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
                #raise Exception(f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
            else:
                missing_data_option_status = "OK"

        elif input_missing_data_option == 15:
            if percentage_missing_data > input_missing_data_option:
                missing_data_option_status = (f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
                #raise Exception(f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
            else:
                missing_data_option_status = "OK"

        elif input_missing_data_option == "WMO":
            # Check WMO criteria for missing or consecutive missing days
            if missing_days == 0 or consecutive_missing_days == 0:
                missing_data_option_status = "OK"
            else:
                missing_days_threshold = 11
                consecutive_missing_days_threshold = 5

                if missing_days >= missing_days_threshold or consecutive_missing_days >= consecutive_missing_days_threshold:
                    #raise Exception("WMO criteria met: Missing or invalid month")
                    missing_data_option_status = "WMO criteria met: Missing or invalid month"
                else:
                    missing_data_option_status = "OK"

        return missing_data_option_status
    except Exception as e:
        print(f"An error occurred, {input_missing_data_option} doesn't provide an acceptable option: {e}")
        raise


def calculate_statistics(calculations, values_results_list, manual_value_threshold):
    """
    calculate statistics from user inputs

    :param calculations: list of calculation given by user input
    :param values_results_list: list of values to calculate genereted by main function climate_stats()
    :param manual_value_threshold: float threshold given by user input
    
    :returns: dict of statistics results
    """
    mean_value = None
    highest_value = None
    lowest_value = None
    count_higher = None
    count_lower = None
    count_equal = None

    # Assuming values_results_list is a list of numeric values or None
    values_results_list = [value for value in values_results_list if value is not None] #Skip the No data (missing data) values
    if values_results_list:
        if "mean" in calculations:
            mean_value = round(sum(values_results_list) / len(values_results_list), 2)
        if "max" in calculations:
            highest_value = max(values_results_list)
        if "min" in calculations:
            lowest_value = min(values_results_list)
        if "count above threshold" in calculations:
            count_higher = sum(1 for value in values_results_list if value > manual_value_threshold)
        if "count below threshold" in calculations:
            count_lower = sum(1 for value in values_results_list if value < manual_value_threshold)
        if "count equal threshold" in calculations:
            count_equal = sum(1 for value in values_results_list if value == manual_value_threshold)
    
    else: # Handle the case when values_results_list is empty
        mean_value = highest_value = lowest_value = count_higher = count_lower = count_equal = None

    statistics = {
        "total": len(values_results_list),
        "mean": mean_value,
        "max": highest_value,
        "min": lowest_value,
        "threshold": manual_value_threshold,
        "count_above": count_higher,
        "count_below": count_lower,
        "count_equal": count_equal
    }

    return statistics

def create_geojson_with_stats(station_data, global_stats, output_info):
    try:
        geojson = {
            "type": "FeatureCollection",
            "info": output_info,
            "global_stats": global_stats,
            "features": []
        }

        for station_info in station_data:
            feature = {
                "type": "Feature",
                "properties": {
                    "name": station_info[0],
                    "station_id": station_info[1]
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": station_info[2]
                }
            }

            geojson["features"].append(feature)

        return geojson
    
    except Exception as e:
        print(f"An error occurred in create_geojson_with_stats: {e}")
        raise

#IN PROGRESS - FOR DEBUGGING
def process_station_hits(station_ids, result_test):
    query_info = []
    for station_id in station_ids:
        station_hits = [hit for hit in result_test['hits']['hits'] if hit['_source']['properties']['STN_ID'] == station_id]
        
        if station_hits:
            query_info.append(f"Station ID {station_id} has hits!")
            print(f"Station ID {station_id} has hits!")
        else:
            query_info.append(f"Station ID {station_id} has no hits.")
            print(f"Station ID {station_id} has no hits.")

    return query_info

################################################
### INPUTS                    
################################################
'''
#Working
bbox = {
    "top_left": {
        "lat": 80,  # Max latitude (north)
        "lon": -141  # Min longitude (west)
    },
    "bottom_right": {
        "lat": 25,  # Min latitude (south)
        "lon": 100  # Max longitude (east)
    }
}
'''
'''
#Working
bbox = {
    "top_left": {
        "lat": 80,  # Max latitude (north)
        "lon": -141  # Min longitude (west)
    },
    "bottom_right": {
        "lat": 25,  # Min latitude (south)
        "lon": -138  # Max longitude (east)
    }
}
'''
bbox = None
station_ids = [5680, 5682] # Exemple : 1535, 1789 # Replace with your station IDs or an empty list for all
#normal_ids = [] # Fonctionne seulement pour climate_normals_data
years = [1980, 1981, 1982, 1983, 1984] # Replace with desired years or an empty list for all
months = [11, 12, 1, 2, 3, 4]  # Replace with desired months or an empty list for all, 5 pour bbox
days = [] # Replace with desired days or an empty list for all, 11 pour bbox
hours = [] # Replace with desired hours or an empty list for all
calculations = ["max"]
property = 'TOTAL_PRECIPITATION'
manualValueThreshold = 0
input_missing_data_option = 5 #5, 10, 15, WMO, None
indexSearch = 'climate_public_daily_data'

################################################
### MAPPING                    
################################################
STN_IDName = None
yearName= None
monthName = None
dayName = None
hourName = None

#TODO add indexes : #indexSearch = "climate_station_information",...
# Mapping of station point data
data_mapping = {
    'climate_normals_data': { #TODO Making option working
        'STN_IDName': 'STN_ID',
        'property': 'VALUE',
        'monthName': 'MONTH',
        # 'property': 'FIRST_YEAR'  # Uncomment if needed
        #FIRST_YEAR, LAST_YEAR, MONTH, NORMAL_CODE/NORMAL_ID
    },
    'climate_public_hourly_data': { #Working
        'STN_IDName': 'STN_ID',
        'property': 'TEMP', #TODO Add properties option
        'yearName': 'LOCAL_YEAR',
        'monthName': 'LOCAL_MONTH',
        'dayName': 'LOCAL_DAY',
        'hourName': 'LOCAL_HOUR',
    },
    'climate_public_daily_data': { #Working
        'STN_IDName': 'STN_ID',
        'property': ['TOTAL_PRECIPITATION', 'TOTAL_SNOW', 'TOTAL_RAIN'],#TODO Add properties option
        'yearName': 'LOCAL_YEAR',
        'monthName': 'LOCAL_MONTH',
        'dayName': 'LOCAL_DAY',
    },
        'climate_public_climate_summary': { #Working
        'STN_IDName': 'STN_ID',
        'property': ['TOTAL_PRECIPITATION', 'TOTAL_SNOWFALL', 'NORMAL_SNOWFALL', 'NORMAL_SUNSHINE', 'NORMAL_PRECIPITATION', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'MEAN_TEMPERATURE'], #TODO Add properties option
        'fieldName_totalDays': ['COOLING_DEGREE_DAYS', 'DAYS_WITH_PRECIP_GE_1MM', 'HEATING_DEGREE_DAYS'],
        'yearName': 'LOCAL_YEAR',
        'monthName': 'LOCAL_MONTH',
        #CLIMATE_IDENTIFIER
        #SNOW_ON_GROUND_LAST_DAY
        #DAYS_WITH_PRECIP_GE_1MM': {'type': 'integer'}, 'DAYS_WITH_VALID_MAX_TEMP': {'type': 'integer'}, 'DAYS_WITH_VALID_MEAN_TEMP': {'type': 'integer'}, 'DAYS_WITH_VALID_MIN_TEMP': {'type': 'integer'}, 'DAYS_WITH_VALID_PRECIP': {'type': 'integer'}, 'DAYS_WITH_VALID_SNOWFALL': {'type': 'integer'}, 'DAYS_WITH_VALID_SUNSHINE'
    },
}

mapping = data_mapping.get(indexSearch, {}) #Search in mapping for input index

################################################
### ELASTICSEARCH PROCESSING                   
################################################
query = None
es_result = None
es_hits = None
es_count = None
missing_data = None
missing_data_percentage = None
consecutive_missing_days = 0
missingParameters = None
unique_stations = None

# Query build
query = build_query(mapping=mapping, station_ids=station_ids, bbox=bbox, years=years, months=months, days=days, hours=hours)

# Perform the search
es_result = es.search(index=indexSearch, body=query, size=10000) #TODO test : match_all

# Get results
es_hits = es_result['hits']['hits']
es_count = es_result['hits']['total']['value']

# Create list of values that match the query (This list will be use for the stats calculations)
valuesResultsList = []
for hit in es_hits:
    document = hit['_source']
    field_value = document['properties'][property] # property to perfom the stats calculations
    #if field_value is not None:
    valuesResultsList.append(field_value)

################################################
### MISSING DATA   
# input_missing_data_option :                  
# None -> Not handling missing data
# 5 -> Checking for 5% missing data
# 10 -> Checking for 10% missing data
# 15 -> Checking for 15% missing data
# "WMO" -> Checking for WMO parameters missing data
################################################
# Calculate missing data from results 
missing_data = calculate_missing_data(es_hits, property)
missing_data_percentage = round(((missing_data / es_count) * 100), 2)

# Calculate total missing days from list of values that match the query #TODO make it work for all the indexes
missing_days = calculate_missing_days(valuesResultsList)

# Calculate consecutive missing days from list total missing days
consecutive_missing_days = calculate_consecutive_missing_days(missing_days)

# Check missing data using input_missing_data_option
missingParameters = check_missing_data_options(
    input_missing_data_option, missing_data_percentage, missing_days, consecutive_missing_days
    )

################################################
### STATS CALCULATIONS                   
################################################
try:
    if valuesResultsList:  # Check if valuesResultsList is empty
        unique_stations = [] # Keep track of unique stations info
        for hit in es_hits:
            station_info = ( 
                hit["_source"]["properties"]["STATION_NAME"],
                hit["_source"]["properties"]["STN_ID"],
                hit["_source"]["geometry"]["coordinates"],
            )           
            # Check if the station name is not already in the set
            if station_info not in unique_stations:
                unique_stations.append(station_info)

        # Calculate results stats
        statistics_result = calculate_statistics(calculations, valuesResultsList, manualValueThreshold)

except Exception as e:
    print(f"An error occurred: {e}")
    raise

################################################
### OUTPUTS                    
################################################
output_info = {
#"es_query" : query,
"INDEX" : indexSearch,
"PROPERTY" : property,
"MISSING_DATA_OPTION": input_missing_data_option,
"MISSING_DATA_TOTAL" : missing_data,
"MISSIND_DATA_PERCENTAGE" : missing_data_percentage,
"MISSING_DATA_STATUS" : missingParameters, # This is the important one, as it give the status form the missing data hanlding
"UNIQUE_STATIONS_COUNT" : len(unique_stations), # This is nice info when using bbox
}

# Generating outputs #TODO make it work for CSV
output_geojson = create_geojson_with_stats(unique_stations, statistics_result, output_info)
json_output = str(output_geojson).replace("'", '"')
print(json_output)

################################################
### DEBUGGING                    
################################################
# Query
#print(json.dumps(query, indent=2))

# Index
#print(es.indices.get_alias().keys())
#print(es.indices.get(index=indexSearch))
#climate_normals_data, climate_pub lic_daily_data

#print(es.indices.get(index='*'))
#print(es.indices.get_alias(index="*"))