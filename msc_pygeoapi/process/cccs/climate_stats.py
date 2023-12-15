# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>
#
# Copyright (c) 2019 Louis-Philippe Rousseau-Lambert
# Copyright (c) 2023 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import click
import csv
import io
import json
import logging
import os
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


from osgeo import gdal, osr
from pyproj import Proj, transform
import yaml
from yaml import CLoader
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
es = ElasticsearchConnector()

from msc_pygeoapi.env import (
    MSC_PYGEOAPI_ES_URL,
    MSC_PYGEOAPI_ES_USERNAME,
    MSC_PYGEOAPI_ES_PASSWORD,
)

LOGGER = logging.getLogger(__name__)

UNITS = {
    'CDD': {'ABS': 'Cooling degree days / ' +
            'Degrés-jours de climatisation'},
    'GSC': {'ABS': 'Days / Jours'},
    'GSO': {'ABS': 'Days / Jours'},
    'GSW': {'ABS': 'Days / Jours'},
    'HDD': {'ABS': 'Heating degree days / Degrés-jours de chauffe'},
    'PR': {'ANO': '%', 'ABS': 'mm'},
    'PREP1': {'ABS': 'Days / Jours'},
    'SFCWIND': {'ANO': '%', 'ABS': 'm s-1'},
    'SIC': {'ANO': '%', 'ABS': '%'},
    'SIT': {'ANO': '%', 'ABS': 'm'},
    'SND': {'ANO': '%', 'ABS': 'm'},
    'SPEI': {'ABS': 'Standardized Precipitation Evapotranspiration Index / ' +
             'Indice de précipitations et d’évapotranspiration normalisé'},
    'TM': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TN': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TN20': {'ABS': 'Nights / Nuits'},
    'TT': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TX': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TX30': {'ABS': 'Days / Jours'}
    }

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'climate-stats',
    'title': 'GeoMet-Climate climate stats process',
    'description': 'GeoMet-Climate climate stats process',
    'keywords': ['climate stats'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'http://canada.ca/climate-services',
        'hreflang': 'en-CA'
    }, {
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'http://canada.ca/services-climatiques',
        'hreflang': 'fr-CA'
    }],
    'inputs': {
        'index': {
            'title': 'index name',
            'description': 'GeoMet-Climate index to request',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'calculations': {
            'title': 'calculations',
            'description': 'Calculations to process',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 10
        },
        'property': {
            'title': 'property',
            'description': 'Property to calculate',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 10
        },
        'bbox': {
            'title': 'bbox coordinate',
            'description': 'Bbox coordinates to request stations inside',
            'schema': {
                'type': 'bbox string format',
            },
            'minOccurs': 1,
            'maxOccurs': 10
        },
        'stations_ids': {
            'title': 'Stations ids',
            'description': 'Stations ids to request',
            'schema': {
                'type': 'list of int',
            },
            'minOccurs': 1,
            'maxOccurs': 1000
        },
        'threshold': {
            'title': 'Threshold input',
            'description': 'Threshold input to calculate stats of stations : under, over, equal to thresold',
            'schema': {
                'type': 'float',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'missing_data_option': {
            'title': 'Missing data option inputs',
            'description': 'Missing data option to request and filtered',
            'schema': {
                'type': 'int or string',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'years': {
            'title': 'Years',
            'description': 'Year(s) to request stats',
            'schema': {
                'type': 'list of number',
            },
            'minOccurs': 1,
            'maxOccurs': 100
        },
        'months': {
            'title': 'Months',
            'description': 'Month(s) to request stats',
            'schema': {
                'type': 'list of number',
            },
            'minOccurs': 1,
            'maxOccurs': 12
        },
        'days': {
            'title': 'Days',
            'description': 'Day(s) to request stats',
            'schema': {
                'type': 'list of number',
            },
            'minOccurs': 1,
            'maxOccurs': 31
        },
        'hours': {
            'title': 'Hours',
            'description': 'Hour(s) to request stats',
            'schema': {
                'type': 'list of number',
            },
            'minOccurs': 1,
            'maxOccurs': 24
        },
    },
    'outputs': {
        'climate-stats-response': {
            'title': 'output climate stats',
            'description': 'GeoJSON or CSV of climate data for specific ' +
                           'locations retrieved from a GeoMet-Climate ' +
                           'layer for all time steps',
            'schema': {
                'oneOf': [{
                    'contentMediaType': 'application/json'
                }, {
                    'contentMediaType': 'application/json'
                }]
            }
        }
    }, 
    'example': {
        "inputs": {
            "index": "climate_public_daily_data",
            "calculations": ["mean"],
            "property": "TOTAL_PRECIPITATION",
            "bbox": None,
            "stations_ids": [1535, 1789, 5137, 5555],
            "threshold": 0,
            "missing_data_option": 5,
            "years": [1989, 1990],
            "months": [],
            "days": [1],
            "hours": []
        }
    }
}

''' # serialize() use in raster_drill 
def serialize(values_dict, cfg, output_format, x, y):
    """
    Writes the information in the format provided by the user

    :param values_dict: result of the get_location_info function
    :param cfg: yaml information
    :param output_format: output format (GeoJSON or CSV)
    :param x: x coordinate
    :param y: y coordinate

    :returns: GeoJSON or CSV output
    """

    time_begin = values_dict['dates'][0]
    time_end = values_dict['dates'][-1]
    time_step = values_dict['time_step']

    data = None

    LOGGER.debug('Creating the output file')
    if len(values_dict['dates']) == len(values_dict['values']):

        if 'CANGRD' not in cfg['label_en']:

            split_en = cfg['label_en'].split('/')
            split_fr = cfg['label_fr'].split('/')

            if 'SPEI' in cfg['label_en']:
                var_en, sce_en, seas_en, label_en = split_en
                var_fr, sce_fr, seas_fr, label_fr = split_fr
                type_en = type_fr = ''
            elif 'Index' in cfg['label_en']:
                var_en, sce_en, label_en = split_en
                var_fr, sce_fr, label_fr = split_fr
                type_en = type_fr = seas_en = seas_fr = ''
            else:
                var_en, sce_en, seas_en, type_en, label_en = split_en
                var_fr, sce_fr, seas_fr, type_fr, label_fr = split_fr

            pctl_en = re.findall(r' \((.*?)\)', label_en)[-1]
            pctl_fr = re.findall(r' \((.*?)\)', label_fr)[-1]
        else:
            type_en, var_en, label_en = cfg['label_en'].split('/')
            type_fr, var_fr, label_fr = cfg['label_fr'].split('/')
            seas_en = re.findall(r' \((.*?)\)', label_en)[0]
            seas_fr = re.findall(r' \((.*?)\)', label_fr)[0]
            sce_en = 'Historical'
            sce_fr = 'Historique'
            pctl_en = pctl_fr = ''

        if output_format == 'CSV':
            time_ = 'time_{time_begin}/{time_end}/{time_step}'
            row = [time_,
                   'values',
                   'longitude',
                   'latitude',
                   'scenario_en',
                   'scenario_fr',
                   'time_res_en',
                   'time_res_fr',
                   'value_type_en',
                   'value_type_fr',
                   'percentile_en',
                   'percentile_fr',
                   'variable_en',
                   'variable_fr',
                   'uom']

            try:
                data = io.BytesIO()
                writer = csv.writer(data)
                writer.writerow(row)
            except TypeError:
                data = io.StringIO()
                writer = csv.writer(data)
                writer.writerow(row)

            for i in range(0, len(values_dict['dates'])):
                writer.writerow([values_dict['dates'][i],
                                 values_dict['values'][i],
                                 x,
                                 y,
                                 sce_en,
                                 sce_fr,
                                 seas_en,
                                 seas_fr,
                                 type_en,
                                 type_fr,
                                 pctl_en,
                                 pctl_fr,
                                 var_en,
                                 var_fr,
                                 values_dict['uom']])

        elif output_format == 'GeoJSON':

            values = []
            for k in values_dict['values']:
                values.append(k)

            data = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [x, y]
                },
                'properties': {
                    'time_begin': time_begin,
                    'time_end': time_end,
                    'time_step': time_step,
                    'variable_en': var_en,
                    'variable_fr': var_fr,
                    'uom': values_dict['uom'],
                    'value_type_en': type_en,
                    'value_type_fr': type_fr,
                    'scenario_en': sce_en,
                    'scenario_fr': sce_fr,
                    'period_en': seas_en,
                    'period_fr': seas_fr,
                    'percentile_en': pctl_en,
                    'percertile_fr': pctl_fr,
                    'label_en': label_en,
                    'label_fr': label_fr,
                    'values': values
                }
            }

    return data
'''
def serialize(output_info, output_geojson):
    """
    Writes the information in the format provided by the user

    :param output_info: Information about the Elasticsearch query and missing data
    :param output_geojson: GeoJSON data with statistics

    :return: Serialized output (e.g., JSON or CSV)
    """

    output_format = "GeoJSON"  # You can modify this based on the desired output format

    if output_format == 'CSV':
        data = io.StringIO()
        writer = csv.writer(data)

        # Write header row
        writer.writerow(['es_query_index', 'es_value_field_name', 'missing_option_input',
                         'missing_data', 'missing_data_percentage', 'missing_parameters',
                         'unique_stations_count'])

        # Write data row
        writer.writerow([output_info['es_query_index'],
                         output_info['es_value_field_name'],
                         output_info['missing_option_input'],
                         output_info['missing_data'],
                         output_info['missing_data_percentage'],
                         output_info['missing_parameters'],
                         output_info['unique_stations_count']])

    elif output_format == "GeoJSON":
        data = {
            'output_info': output_info,
            'output_geojson': output_geojson
        }

        # Convert the dictionary to a JSON string
        data = json.dumps(data)

    return data

################################################
### FUNCTIONS                    
################################################
def build_query(mapping, bbox=None, stations_ids=None, years=None, months=None, days=None, hours=None):
    """
    #TODO
    generate Elasticsearch query from user inputs

    :param mapping:  
    :param bbox: 
    :param stations_ids: 
    :param years: 
    :param months: 
    :param days: 
    :param hours: 

    :returns: Elasticsearch query
    """
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

        #for debug
        print(f"Mapping: {mapping}")

        if stations_ids and 'STN_IDName' in mapping:
            stn_id_name = mapping['STN_IDName']
            query["query"]["bool"]["must"].append({"terms": {"properties." + stn_id_name: stations_ids}})

        if years and 'yearName' in mapping:
            year_name = mapping['yearName']
            query["query"]["bool"]["must"].append({"terms": {"properties." + year_name: years}})

        if months and 'monthName' in mapping:
            month_name = mapping['monthName']
            query["query"]["bool"]["must"].append({"terms": {"properties." + month_name: months}})

        if 'dayName' in mapping and days:
            day_name = mapping['dayName']
            query["query"]["bool"]["must"].append({"terms": {"properties." + day_name: days}})

        if 'hourName' in mapping and hours:
            hour_name = mapping['hourName']
            query["query"]["bool"]["must"].append({"terms": {"properties." + hour_name: hours}})

        return query

    except Exception as e:
        print(f"An error occurred in build_query: {e}")
        raise

def calculate_missing_data(es_hits, data_field):
    """
    calculate missing data from Elasticsearch query results

    :param es_hits: Elasticsearch query results 
    :param data_field: input property from the user

    :returns: int of total missing data
    """
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
    """
    calculate missing days using total count of missing days

    :param data: int of total missing days
    
    :returns: int of total missing days
    """
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
    """
    calculate consecutive missing days using total count of missing days

    :param data: int of total missing days
    
    :returns: higher value of consecutive missing days found
    """
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

def check_missing_data_options(input_missing_data_option, missing_data_percentage, missing_days, consecutive_missing_days):
    """
    check/validate missing data parameter using user input : (None, 5, 10, 15, "WMO")

    :param input_missing_data_option: int or str form user input for which missing data option to check
    :param missing_data_percentage: float of missing data percentage   
    :param missing_days: int of missing days form calculate_missing_days()
    :param consecutive_missing_days: int of consecutive missing days form calculate_consecutive_missing_days()
    
    :returns: str missing data status : ("None", "OK", "The input missing data option filtered X")
    """
    
    try:
        if input_missing_data_option == None:
            missing_data_option_status = "None"

        elif input_missing_data_option == 5:
            if missing_data_percentage > input_missing_data_option:
                missing_data_option_status = (f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
                #raise Exception(f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
            else:
                missing_data_option_status = "OK"

        elif input_missing_data_option == 10:
            if missing_data_percentage > input_missing_data_option:
                missing_data_option_status = (f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
                #raise Exception(f"The input missing data option filtered this result: percentage missing data > input missing data option: {percentage_missing_data} > {input_missing_data_option}")
            else:
                missing_data_option_status = "OK"

        elif input_missing_data_option == 15:
            if missing_data_percentage > input_missing_data_option:
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
    """
    generate geojson file with output stats form calculate_statistics()

    :param station_data: list of unique stations info genereted by main function climate_stats()
    :param global_stats: dict stats genereted by calculate_statistics()
    :param output_info: dict of info genereted by main function climate_stats()
    
    :returns: geojson file
    """
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
def climate_stats(index, calculations, property, bbox, stations_ids, threshold, missing_data_option, years=None, months=None, days=None, hours=None):
    """ 
    #TODO
    Writes the information in the format provided by the user
    and reads some information from the geomet-climate yaml

    :param layer: layer name
    :param x: x coordinate
    :param y: y coordinate
    :param format_: output format (GeoJSON or CSV)

    :return: return the final file fo a given location
    """

    from msc_pygeoapi.process.cccs import (GEOMET_CLIMATE_CONFIG,
                                           GEOMET_CLIMATE_BASEPATH,
                                           GEOMET_CLIMATE_BASEPATH_VRT)

    LOGGER.info('start climate stats')

    ################################################
    ### MAPPING                   
    ################################################
    LOGGER.info('start mapping point data')
    LOGGER.info('Input - INDEX value:', index)
    LOGGER.info('Input - property value:', property)
    
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

    mapping = data_mapping.get(str(index), {}) # Search in mapping for input index

    # To add the possibility for property in a list
    #if isinstance(property, list) and property:
        #property = property[0]
        #LOGGER.info('property, property:', property)  # Use the first element of the list directly
    #else:
        #LOGGER.info('property, property:', property)  # Use the string directly

    LOGGER.info('Data mapping completed', data_mapping)
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

    LOGGER.info('start es processing')

    # TODO remove this and replace with env variable
    es = Elasticsearch('https://geomet-dev-22.cmc.ec.gc.ca:443/elasticsearch', basic_auth=("elasticSearch", "Es4tkag"))
    es.ping()
    
    LOGGER.info('es query building')
    # Query build
    query = build_query(mapping, stations_ids, bbox, years, months, days, hours)

    LOGGER.info('es search')
    # Perform the search
    es_result = es.search(index=index, body=query, size=10000) #TODO test : match_all #TODO test for big quantity of data

    LOGGER.info('es results processing')
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

    LOGGER.info('es processing completed')

    ################################################
    ### MISSING DATA   
    # input_missing_data_option :                  
    # None -> Not handling missing data
    # 5 -> Checking for 5% missing data
    # 10 -> Checking for 10% missing data
    # 15 -> Checking for 15% missing data
    # WMO -> Checking for WMO parameters missing data
    #################################################
    LOGGER.info('start missing data handling')
    # Calculate missing data from results 
    missing_data = calculate_missing_data(es_hits, property)
    missing_data_percentage = round(((missing_data / es_count) * 100), 2)

    # Calculate total missing days from list of values that match the query #TODO make it work for all the indexes
    missing_days = calculate_missing_days(valuesResultsList)

    # Calculate consecutive missing days from list total missing days
    consecutive_missing_days = calculate_consecutive_missing_days(missing_days)

    # Check missing data using input_missing_data_option
    missingParameters = check_missing_data_options(
        missing_data_option, missing_data_percentage, missing_days, consecutive_missing_days
        )
    
    LOGGER.info('missing data handling completed')

    ################################################
    ### STATS CALCULATIONS                   
    ################################################
    LOGGER.info('start stats calculations')
    try:
        if valuesResultsList: # Check if valuesResultsList is empty
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
            statistics_result = calculate_statistics(calculations, valuesResultsList, threshold)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    LOGGER.info('stats calculations completed')

    ################################################
    ### OUTPUTS                    
    ################################################
    LOGGER.info('start generating outputs')
    
    output_info = {
    #"es_query" : query,
    "INDEX" : index,
    "PROPERTY" : property,
    "MISSING_DATA_OPTION": missing_data_option,
    "MISSING_DATA_TOTAL" : missing_data,
    "MISSIND_DATA_PERCENTAGE" : missing_data_percentage,
    "MISSING_DATA_STATUS" : missingParameters, # This is the important one, as it give the status form the missing data hanlding
    "UNIQUE_STATIONS_COUNT" : len(unique_stations), # This is nice info when using bbox
    }

    #output_geojson = create_geojson_with_stats(unique_stations, statistics_result, missing_data, missing_data_percentage, missingParameters) #TODO add output_results
    #output_geojson = create_geojson_with_stats(unique_stations, statistics_result)
    #output_geojson = str(output_geojson).replace("'", '"')
    #print(output_geojson)
    #print(output_geojson)
    #output = serialize(output_info, output_geojson)

    # Generating outputs #TODO make it work for CSV
    output_geojson = create_geojson_with_stats(unique_stations, statistics_result, output_info)
    output_geojson = str(output_geojson).replace("'", '"')

    LOGGER.info('generating outputs completed')
    LOGGER.info('climate stats completed')

    return output_geojson

@click.group('execute')
def climate_stats_execute():
    pass

@click.command('climate-stats')
@click.pass_context

@click.option('--index', help='Index name to process', required=True)
@click.option('--calculations', help='calculations', required=True)
@click.option('--property', help='property', required=True)
@click.option('--bbox', help='Bbox coordinates to process', required=False)
@click.option('--stations_ids', help='Stations ids to process', required=False, is_flag=False)
@click.option('--threshold', help='Treshold input for stats calculation', required=False, default=0)
@click.option('--missing_data_option', 'missing_data_option', type=click.Choice(['None', 5, 10, 15, 'WMO']),
            default='None', help='Missing data option to chose for the process')
@click.option('--years', help='Query', required=False, default=[])
@click.option('--months', help='Query', required=False, default=[])
@click.option('--days', help='Query', required=False, default=[])
@click.option('--hours', help='Query', required=False, default=[])
#@click.option('--format', 'format_', type=click.Choice(['GeoJSON', 'CSV']),
            #default='GeoJSON', help='output format')

def climate_stats_cli(ctx, index, calculations, property, bbox, stations_ids, threshold, missing_data_option, years=None, months=None, days=None, hours=None):
    """Process climate statistics based on provided parameters.
        Args:
            ctx (click.Context): Click context.
            index (str): Index name to process.
            calculations: 
            property: 
            bbox (string): Bbox coordinates flag to process.
            stations_ids (str): Stations ids to process.
            threshold (float): Threshold input for stats calculation.
            missing_data_option (str): Missing data option.
            years (list): Query for years.
            months (list): Query for months.
            days (list): Query for days.
            hours (list): Query for hours.
            format_ (str): Output format (GeoJSON or CSV).
        
        Returns:
            dict: Processed climate statistics.

        """
    try:
        #if validate_parameters(bbox, stations_ids, index, calculations, threshold, missing_data_option, years, months, days, hours):
        output = climate_stats(str(index), calculations, str(property), bbox, stations_ids, float(threshold), missing_data_option, years, months, days, hours)
        #if format_ == 'GeoJSON':
        click.echo(json.dumps(output, ensure_ascii=False))
        #elif format_ == 'CSV':
            #click.echo(output.getvalue())
        logging.info("Climate statistics processing successful.")
    except Exception as e:
        logging.error(f"Error processing climate statistics: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        raise

climate_stats_execute.add_command(climate_stats_cli)

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


    class ParameterValidationError(Exception):
        def __init__(self, parameter_name, expected_type):
            self.parameter_name = parameter_name
            self.expected_type = expected_type
            super().__init__(f'Invalid type for parameter "{parameter_name}". Expected type: {expected_type}')
    
    class ClimateStatsProcessor(BaseProcessor):
        """Climate Stats Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.cccs.climate_stats.ClimateStatsProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            mimetype = 'application/json'

            index = data.get('index')
            calculations = data.get('calculations')
            property = data.get('property')           
            bbox = data.get('bbox')
            stations_ids = data.get('stations_ids')
            threshold = data.get('threshold')
            missing_data_option = data.get('missing_data_option')
            years = data.get('years')
            months =data.get('months')
            days = data.get('days')
            hours = data.get('hours')
            #format_ = self.validate_parameter('format_', data.get('format'), str)

            try:
                output = climate_stats(index, calculations, property, bbox, stations_ids, threshold, missing_data_option, years, months, days, hours)

            except ValueError as err:
                # Handle other errors
                msg = f'Process execution error: {err}'
                LOGGER.error(msg)
                # Return a response with a specific error code
                return {"error": msg, "error_code": "PROCESS_EXECUTION_ERROR"}, 500

            #if format_ == 'GeoJSON':
                #dict_ = output
            #elif format_ == 'CSV':
                #mimetype = 'text/csv'
                #dict_ = output.getvalue()
            dict_ = output
            
            #else:
                #msg = 'Invalid format'
                #LOGGER.error(msg)
                # Return a response with a specific error code
                #return {"error": msg, "error_code": "INVALID_FORMAT_ERROR"}, 400

            return mimetype, dict_

        def __repr__(self):
            return f'<ClimateStatsProcessor> {self.name}'

except (ImportError, RuntimeError):
    pass