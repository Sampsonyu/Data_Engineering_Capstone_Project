import configparser
import datetime
import os
import re

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, round, substring, upper
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, quarter, date_format
from pyspark.sql.types import DateType, IntegerType, StringType
# from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
import psycopg2


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session
    :return: spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """
    Description: This function processes the songs data files.
    1. Read and load immigration_data from S3
    2. Transform them to create immigration
    3. Write them to partioned parquet files in table directories on S3

    :param spark: spark session
    :param input_data: input file path
    :param output_data: output file path
    :return: dim_immigrant, dim_time, dim_immigration
    """
    print('Processing US immigration dataset...')

    # paths of input datasets
    months = ['jun']
    # months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    paths = [month.join(['18-83510-I94-Data-2016/i94_', '16_sub.sas7bdat']) for month in months]
    # immigration_data_paths = [os.path.join(input_data, path) for path in paths]
    immigration_data_paths = [input_data + path for path in paths]

    # Defining User Defined Function (UDF) to convert log timestamp (seconds since epoch) to \
    # actual Datetime Type timestamp
    # create timestamp column from original timestamp column

    # convert_sas_date = udf(lambda days: (datetime.date(1960, 1, 1) + datetime.timedelta(days=days)), T.DateType())
    # df = df.withColumn('timestamp', get_timestamp(df.ts))

    convert_i94mode_udf = udf(convert_i94mode, StringType())
    convert_sas_date_udf = udf(convert_sas_date, DateType())
    convert_visa_udf = udf(convert_visa, StringType())
    # get_sas_day_udf = udf(get_sas_day, IntegerType())

    # immigration_data_paths = os.path.join(input_data, '18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat')
    for immigration_data_paths in immigration_data_paths:
        # get filepath to song data file
        print("Processing fact_immigration spark dataframe")
        df_immigration_spark = spark.read\
            .format('com.github.saurfang.sas.spark')\
            .option("header", "true") \
            .option("inferSchema", "true")\
            .load(immigration_data_paths)

    #    .schema(schema['immigration_schema'])

        df_immigration_spark = df_immigration_spark \
            .withColumn('arrival_date', convert_sas_date_udf(df_immigration_spark['arrdate'])) \
            .withColumn('departure_date', convert_sas_date_udf(df_immigration_spark['depdate'])) \
            .withColumn('arrival_year', df_immigration_spark['i94yr'].cast(IntegerType())) \
            .withColumn('arrival_month', df_immigration_spark['i94mon'].cast(IntegerType())) \
            .withColumn('age', df_immigration_spark['i94bir'].cast(IntegerType())) \
            .withColumn('country_of_bir', df_immigration_spark['i94cit'].cast(IntegerType())) \
            .withColumn('country_of_res', df_immigration_spark['i94res'].cast(IntegerType())) \
            .withColumn('port_of_admission', df_immigration_spark['i94port'].cast(StringType())) \
            .withColumn('birth_year', df_immigration_spark['biryear'].cast(IntegerType())) \
            .withColumn('mode', convert_i94mode_udf(df_immigration_spark['i94mode'])) \
            .withColumn('visa_category', convert_visa_udf(df_immigration_spark['i94visa']))

    #    .withColumn('arrival_day', get_sas_day_udf(df_immigration_spark['arrdate'])) \

        dim_immigration = df_immigration_spark.select(
            col('cicid').alias('id'),
            col('arrdate').alias('arr_ts'),
            'arrival_year',
            'arrival_month',
            'country_of_bir',
            'country_of_res',
            'port_of_admission',
            'mode',
            'visa_category',
            'visatype',
            col('depdate').alias('dep_ts')) \
            .dropDuplicates()

        # immigration_out_path = os.path.join(output_data, 'fact_immigration/')
        immigration_out_path = output_data + 'fact_immigration/'

        dim_immigration.write.parquet(immigration_out_path, mode='append', partitionBy=('arrival_year',
                                                                                        'arrival_month'))

        df_time_spark = df_immigration_spark.select(col("arrdate").alias("time")).union(
            df_immigration_spark.select(col("depdate").alias("time"))).distinct().na.drop()

        # Create time dimension spark dateframe
        print("Processing time dimension spark dataframe")
        dim_time = df_time_spark.withColumn('date', convert_sas_date_udf(df_time_spark['time']))

        dim_time = dim_time\
            .select(
                col('time').alias('time_id')
                , col('date').alias('date')
                , year('date').alias('year')
                , quarter('date').alias('quarter')
                , month('date').alias('month')
                , dayofmonth('date').alias('day')
                , dayofweek('date').alias('weekday')
                , weekofyear('date').alias('week'))
        # time_out_path = os.path.join(output_data, 'dim_time/')
        time_out_path = output_data + 'dim_time/'
        dim_time.write.parquet(time_out_path, mode='append', partitionBy='year')

        # Create immigrant dimension spark dateframe
        print("Processing immigrant dimension spark dataframe")
        dim_immigrant = df_immigration_spark \
            .filter(col('birth_year') >= 1900) \
            .filter(col('birth_year') <= 2020) \
            .select(
                col('cicid').alias('immigrant_id'),
                'gender',
                'age',
                'birth_year') \
            .dropDuplicates()
        # immigrant_out_path = os.path.join(output_data, 'dim_immigrant/')
        immigrant_out_path = output_data + 'dim_immigrant/'
        dim_immigrant.write.parquet(immigrant_out_path, mode='append', partitionBy='birth_year')

    print('Finished processing us immigration data.')
    # return dim_immigrant, dim_time, dim_immigration


def process_demographics_data(spark, input_data, output_data):
    """
    Description: Process the demographics data files, transform it to demographics table
    :param spark: spark session
    :param input_data: input file path
    :param output_data: output file path
    """
    # dim_country, dim_port
    dim_port_dict = process_label_data(spark, output_data)
    print('Processing us cities demographics dataset...')
    # get filepath to demographics data file
    # demographics_data_path = os.path.join(input_data, 'us-cities-demographics.csv')
    demographics_data_path = input_data + 'us-cities-demographics.csv'
    df_demographics_spark = spark.read \
        .format('csv') \
        .option("header", "true") \
        .option("delimiter", ";")\
        .load(demographics_data_path)

    # Calculate percentages of each numeric column and create new columns.
    df_demographics_spark = df_demographics_spark \
        .withColumn("Median Age", col("Median Age").cast("float")) \
        .withColumn("pct_male_pop",
                    df_demographics_spark["Male Population"] / df_demographics_spark["Total Population"] * 100) \
        .withColumn("pct_female_pop",
                    df_demographics_spark["Female Population"] / df_demographics_spark["Total Population"] * 100) \
        .withColumn("pct_veterans",
                    df_demographics_spark["Number of Veterans"] / df_demographics_spark["Total Population"] * 100) \
        .withColumn("pct_foreign_born",
                    df_demographics_spark["Foreign-born"] / df_demographics_spark["Total Population"] * 100) \
        .withColumn("pct_race", df_demographics_spark["Count"] / df_demographics_spark["Total Population"] * 100) \
        .orderBy("State")

    # Select columns with new calculated percentages.
    df_demographics_spark_select = df_demographics_spark \
        .select(col("City").alias("city"),
                col("State").alias("state"),
                col("Median Age").alias("median_age"),
                "pct_male_pop",
                "pct_female_pop",
                "pct_veterans",
                "pct_foreign_born",
                "Race",
                "pct_race",
                "Total Population")

    # pivot the Race column
    df_demographics_spark_pivot = df_demographics_spark_select\
        .groupBy("city", "state", "median_age", "pct_male_pop", "pct_female_pop", "pct_veterans",
                "pct_foreign_born", "Total Population")\
        .pivot("Race")\
        .avg("pct_race")

    # change the header name of the race fields for spark compatibility and round the percentage.
    df_demographics_spark_pivot = df_demographics_spark_pivot \
        .select(upper(col("city")).alias("city"),
                upper(col("state")).alias("state"),
                round(col("median_age")).alias("median_age"),
                round(col("pct_male_pop"), 1).alias("perc_of_male_pop"),
                round(col("pct_female_pop"), 1).alias("perc_of_female_pop"),
                round(col("pct_veterans"), 1).alias("perc_of_veterans"),
                round(col("pct_foreign_born"), 1).alias("perc_of_foreign_born"),
                round(col("American Indian and Alaska Native"), 1).alias("perc_of_native_american"),
                round(col("Asian"), 1).alias("perc_of_asian"),
                round(col("Black or African-American"), 1).alias("perc_of_black"),
                round(col("Hispanic or Latino"), 1).alias("perc_of_latino"),
                round(col("White"), 1).alias("perc_of_white"),
                col("Total Population").alias("total_pop"))\
        .dropDuplicates(['city'])

    # dim_demographics = df_demographics_spark_pivot.withColumn("city_id", monotonically_increasing_id())
    dim_demographics = dim_port_dict\
        .join(df_demographics_spark_pivot,
              (dim_port_dict['city_name'] == df_demographics_spark_pivot['city']) & (
                          dim_port_dict['state_name'] == df_demographics_spark_pivot['state']), "inner") \
        .drop("city_name", "state_name")

    # demographics_out_path = os.path.join(output_data, 'dim_demographics/')
    demographics_out_path = output_data + 'dim_demographics/'
    dim_demographics.write.parquet(demographics_out_path, mode='overwrite', partitionBy='state')
    # dim_demographics.write.format("csv").save("data/demographics_table/demo.csv")
    print('Finished processing us cities demographics data.')
    # return dim_demographics
    # port_code/state_code/city/state/median_age/perc_of_male_pop/perc_of_femal......


def process_airport_data(spark, input_data, output_data):
    """
    Description: Process the airport data files, transform it to airport table
    :param spark: spark session
    :param input_data: input file path
    :param output_data: output file path
   """
    print('Processing airport codes dataset...')

    # get filepath to airport data file
    # airport_data_path = os.path.join(input_data, 'airport-codes_csv.csv')
    airport_data_path = input_data + 'airport-codes_csv.csv'
    df_airport_spark = spark.read\
        .format('csv')\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .load(airport_data_path)\
        .drop("coordinates", "gps_code", "continent", "elevation_ft")

    dim_airport = df_airport_spark\
        .filter((col("type") == "small_airport") |
                (col("type") == "large_airport") |
                (col("type") == "medium_airport"))\
        .filter(df_airport_spark["iso_country"] == "US")\
        .filter(df_airport_spark['local_code'].isNotNull())\
        .withColumn("state", substring(df_airport_spark["iso_region"], 4, 2))
#         .filter(df_airport_spark['iata_code'].isNotNull()) \

    # airport_out_path = os.path.join(output_data, 'dim_airport/')
    airport_out_path = output_data + 'dim_airport/'
    dim_airport.write.parquet(airport_out_path)

    print('Finished processing airport codes data.')
    # return dim_airport


def process_temperature_data(spark, input_data, output_data):
    """
    Description: Process the temperatureData data files, transform it to temperature table
    :param spark: spark session
    :param input_data: input file path
    :param output_data: output file path
    """
    print('Processing cities temperature dataset...')
    # get filepath to temperatureData data file
    # temperature_data_path = os.path.join(input_data, 'GlobalLandTemperaturesByCity.csv')
    temperature_data_path = input_data + 'GlobalLandTemperaturesByCity.csv'
    df_temperature_spark = spark.read \
        .format('csv') \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(temperature_data_path)

    df_temperature_spark = df_temperature_spark \
        .filter(df_temperature_spark["country"] == "United States") \
        .filter(df_temperature_spark.AverageTemperature.isNotNull())\
        .withColumn("year", year(df_temperature_spark["dt"])) \
        .withColumn("month", month(df_temperature_spark["dt"]))

    dim_temperature = df_temperature_spark.select(
        "year",
        "month",
        "AverageTemperature",
        "City",
        "Country")\
        .dropDuplicates()

    # temperature_out_path = os.path.join(output_data, 'dim_temperature/')
    temperature_out_path = output_data + 'dim_temperature/'
    dim_temperature.write.parquet(temperature_out_path, mode='overwrite', partitionBy='year')

    print('Finished processing cities temperature data.')
    # return dim_temperature


def process_label_data(spark, output_data):
    """
    Description: Process the country data files, transform it to country table
    :param spark: spark session
    :param labels: i94cntyl/i94addr/i94prtl
    :param output_data: output file path
    :return: dim_port_dict
    """
    print('Processing i94 SAS dictionary...')
    labels = ["i94cntyl", "i94addr", "i94prtl"]
    for label in labels:

        if label == "i94cntyl":

            i94cntyl = code_mapper(label)  # '101': 'ALBANIA',
            i94cntyl = {k: (v if not re.match('^INVALID:|^Collapsed|^No Country Code', v) else 'INVALID ENTRY')
                        for k, v in i94cntyl.items()}

            df_i94cntyl = pd.DataFrame(i94cntyl.items(), columns=['country_code', 'country'])

            # country_out_path = os.path.join(output_data, 'i94cntyl.csv')
            country_out_path = output_data + 'i94cntyl.csv'
            print(country_out_path)
            df_i94cntyl.to_csv(country_out_path, index=False, sep=',')

            dim_country = spark.createDataFrame(df_i94cntyl)
            # country_out_path = os.path.join(output_data, 'dim_country/')
            country_out_path = output_data + 'dim_country/'
            dim_country.write.parquet(country_out_path)

        elif label == "i94addr":
            i94addr = code_mapper(label)   # 'AK': 'ALASKA'
            i94addr = {k: format_state(v) for k, v in i94addr.items()}

            df_i94addr = pd.DataFrame(i94addr.items(), columns=['state_codes', 'state'])

            # state_out_path = os.path.join(output_data, 'i94addr.csv')
            state_out_path = output_data + 'i94addr.csv'
            print(state_out_path)
            df_i94addr.to_csv(state_out_path, index=False, sep=',')

        elif label == "i94prtl":
            # 'ORL': 'ORLANDO, FL'  to  'ORL', 'ORLANDO',  'FL'
            i94port = code_mapper(label)
            i94port_split = covert_i94port(i94port, i94addr)

            df_i94port = pd.DataFrame(i94port_split.values(), columns=['port_code', 'city', 'state_code'])
            # port_out_path = os.path.join(output_data, 'i94port.csv')
            port_out_path = output_data + 'i94port.csv'
            print(port_out_path)
            df_i94port.to_csv(port_out_path, index=False, sep=',')

    # print(pd.merge(df_i94port, df_i94addr, left_on='state_code', right_on='state_codes', how='left')
    #      .drop('state_codes', axis=1))
    df_i94port_spark = spark.createDataFrame(df_i94port)
    df_i94addr_spark = spark.createDataFrame(df_i94addr)
    df_i94port_dict = df_i94port_spark \
        .join(df_i94addr_spark, df_i94port_spark['state_code'] == df_i94addr_spark['state_codes'], 'left')
    dim_port_dict = df_i94port_dict\
        .select("port_code", upper(col("city")).alias("city_name"),
                "state_code", upper(col("state")).alias("state_name"))\
        .filter(df_i94port_dict["state"].isNotNull())
    print('Finished processing i94 SAS dictionary')
    return dim_port_dict
    # +---------+------------+----------+--------------+
    # |port_code|   city_name|state_code|    state_name|
    # +---------+------------+----------+--------------+
    # |      DOU|     DOUGLAS|        AZ|       ARIZONA|
    # |      LUK|   LUKEVILLE|        AZ|       ARIZONA|


# Create UDF for process date, mode, visa
def convert_sas_date(days):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :param days: Days since 1/1/1960
    :return: datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)


def get_sas_day(days):
    """
    Converts SAS date stored as days since 1/1/1960 to day of month
    :param days: Days since 1/1/1960
    :return: Day of month value as integer
    """
    if days is None:
        return None
    return (datetime.date(1960, 1, 1) + datetime.timedelta(days=days)).day


def convert_i94mode(mode):
    """
    Converts i94 travel mode code to a description
    :param mode: int i94 mode as integer
    :return: i94 mode description
    """
    if mode == 1:
        return "Air"
    elif mode == 2:
        return "Sea"
    elif mode == 3:
        return "Land"
    else:
        return "Not Reported"


def convert_visa(visa):
    """
    Converts visa numeric code to description
    :param visa: str
    :return: Visa description: str
    """
    if visa is None:
        return "Not Reported"
    elif visa == 1:
        return "Business"
    elif visa == 2:
        return "Pleasure"
    elif visa == 3:
        return "Student"
    else:
        return "Not Reported"


def format_state(s):
    """
    Format state column
    :param s:
    :return:
    """
    s = s.replace('DIST. OF', 'District of') \
         .replace('S.', 'South') \
         .replace('N.', 'North') \
         .replace('W.', 'West')
    return ' '.join([w.capitalize() if w != 'of' else w for w in s.split()])


def covert_i94port(i94port, i94addr):
    """
    Process i94port dictionary
    :param i94port:
    :param i94addr:
    :return: i94port_split
    """
    i94port_split = {}
    index = 0
    for k, v in i94port.items():
        if not re.match('^Collapsed|^No PORT Code', v):
            try:
                # extract state part from i94port
                # the state part contains the state and also other words
                state_part = v.rsplit(',', 1)[1]
                city_part = v.rsplit(',', 1)[0]
                # create a set of all words in state part
                state_part_set = set(state_part.split())
                # if the state is valid (is in the set(i94addr.keys()), then retrieve state
                state = list(set(i94addr.keys()).intersection(state_part_set))[0]
                # add state to dict
                i94port_split[index] = [k, city_part, state]
            except IndexError:
                # no state is specified for Washington DC in labels so it is added here
                # 'MARIPOSA AZ' is not split by ","
                if v == 'WASHINGTON DC':
                    i94port_split[index] = [k, 'WASHINGTON DC', 'DC']
                elif v == 'MARIPOSA AZ':
                    i94port_split[index] = [k, 'MARIPOSA', 'AZ']
                else:
                    i94port_split[index] = [k, 'NULL', 'NULL']

        else:
            i94port_split[index] = [k, 'NULL', 'NULL']
        index += 1
    return i94port_split


def code_mapper(label):
    """
    Split I94 SAS label dictionary
    Parameters:
    :param label:
    :return label_dict:
    """
    print("mapper function")

    i94_sas_label_descriptions_path = "data/I94_SAS_Labels_Descriptions.SAS"

    with open(i94_sas_label_descriptions_path) as f:
        sas_labels_content = f.read()
    sas_labels_content = sas_labels_content.replace('\t', '')
    label_content = sas_labels_content[sas_labels_content.index(label):]
    label_content = label_content[:label_content.index(';')].split('\n')
    label_content = [i.replace("'", "") for i in label_content]

    label_dict = [i.split('=') for i in label_content[1:]]
    label_dict = dict([i[0].strip(), i[1].strip()] for i in label_dict if len(i) == 2)

    return label_dict


def check_data_quality(spark, output_data):
    """
    Count checks on fact and dimension table to ensure completeness of data.
    Parameters:
    :spark: Spark Session
    :output_data: folder to get the parquet files
    """
    print('Checking data quality...')

    tables = {
        "fact_immigration": output_data + "fact_immigration/arrival_year=2016/*/*.parquet",
        "dim_time": output_data + "dim_time/*/*.parquet",
        "dim_demographics": output_data + "dim_demographics/*/*.parquet",
        "dim_airport": output_data + "dim_airport/*.parquet",
        "dim_temperature": output_data + "dim_temperature/*/*.parquet",
        "dim_immigrant": output_data + "dim_immigrant/*/*.parquet",
        "dim_country": output_data + "dim_country/*.parquet"
    }

    for table_name, path in tables.items():
        # quality check for table
        df = spark.read.parquet(path)
        total_count = df.count()
        print(f"Checking {table_name} records...")
        if total_count == 0:
            print(f"Data quality check failed for {table_name} with zero records!")
        else:
            print(f"Data quality check passed for {table_name} with {total_count:,} records.")

    print('Finished checking data quality.')
    return 0


def main():
    """
    Description: main function of etl pipeline
        create_spark_session,
        process fact and dimension functions,
        check_data_quality
    """
    print('ETL process begins!')
    spark = create_spark_session()

    # specify input data folder # input_data = 's3a://udacity-dend/'
    input_data = 'data/'
    # specify output data folder # output_data = 's3://sparkifydatasource/spark_output/'
    output_data = 'data/output/'

    # dim_demographics
    # process_demographics_data(spark, input_data, output_data)
    # dim_airport
    # process_airport_data(spark, input_data, output_data)
    # dim_immigrant, dim_time, fact_immigration
    # process_immigration_data(spark, input_data, output_data)
    # dim_temperature
    # process_temperature_data(spark, input_data, output_data)

    print('ETL process finished!')
    check_data_quality(spark, output_data)


if __name__ == "__main__":
    main()
