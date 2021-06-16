# Data_Engineering_Capstone_Project


## Project Summary

This is the capstone project for the Udacity Data Engineering Nanodegree program. The idea is to take multiple disparate data sources, clean the data, and process it through an ETL pipeline to produce a usable data set for analytics.

We will be looking at the immigration data for the U.S. ports (I94 immigration data). The plan is to enrich this data using the other data sources suggested (e.g. and U.S. City Demographic Data), build an ETL pipeline to process the raw data. 

We will be gathering, assessing, cleaning, creating data models, setup and push data to a data lake where it will be transformed into fact and dimension spark DataFrames that form a star schema. These DataFrames are then stored as ".parquet" files on Amazon S3 Bucket.

At a high level:

- Data is extracted from the immigration SAS data, partitioned by year, month and stored in a data lake on Amazon S3 as Parquet files.
- The partitioned data is loaded into data lake (Amazon EMR cluster)
- Design fact and dimension tables
- Write the processed data for each table back to S3


Example questions we could explore with the final data set:

+ For a given port city, how many immigrants enter from which countries?
+ Is there any relationship between the average temperature of the country of origin and average temperature of the port city of entry?
+ Is there any relationship between the connection between the volume of travel and the number of entry ports (ie airports)
+ The effects of temperature on the volume of travellers
+ What time of year or month sees more immigration for certain areas?


### Technical Overview
Project uses following technologies:

+ AWS S3 Storage : To store inputs & outputs
+ AWS EMR
+ Juypter Notebooks
+ Python
+ Spark & Hadoop
+ Libraries : Pandas & Pyspark


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Modeli94port
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


### Project Datasets

**I94 Immigration Data (immigration)** 

The i94 data contains information about visitors to the US via an i94 form that all visitors must complete. It comes from the US National Tourism and Trade Office and includes details on incoming immigrants and their ports of entry. - [source](https://www.trade.gov/national-travel-and-tourism-office). 

It is provided in SAS7BDAT format which is a binary database storage format. It is created by Statistical Analysis System (SAS) software to store data.

The immigration data is partitioned into monthly SAS files. Each file is around 300 to 700 MB. The data provided represents 12 months of data for the year 2016, ranging in size from 391 MB to 716 MB, for a total size of 6.51 GB. This is the bulk of the data used in the project.

A data dictionary ```I94_SAS_Labels_Descriptions.SAS``` is provided for the immigration data. In addition to descriptions of the various fields, the port and country codes used were listed in table format. Two csv files are extracted from the dictionary: 

* ```i94_countries.csv```: Table containing country codes used in the dataset.
* ```i94_ports.csv```: Table containing city codes used in the dataset.

These files will be used as a lookup when extracting the immigration data.

**World Temparature data (temperature)** 

This dataset comes from Kaggle. - [source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

It contains average temperature data for countries and cities around the world between 1743-11-01 and 2013-09-01.

The data is stored in ```GlobalLandTemperaturesByCity.csv``` (508 MB).

**U.S. City Demographic Data (demographics)**

This data comes from OpenSoft and contains demographic data for U.S. cities and states, such as age, population, veteran status and race. - [source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

This data placed in the data lake as a single CSV file (246 KB). This data will be combined with port city data to provide ancillary demographic info for port cities.

**Airport Code Table (airports)**

This is a simple table of airport codes and corresponding cities. It comes from datahub.io. - [source](https://datahub.io/core/airport-codes#data)

This data is a single CSV file (5.8 KB). It provides additional information for airports and can be combined with the immigration port city info.


Below is a table of datasets used in the project:
<table>
<thead>
<tr>
<th>Source name</th>
<th>Filename</th>
<th>Format</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>I94 Immigration Sample Data</td>
<td>immigration_data_sample.csv</td>
<td>csv</td>
<td>This is a sample data which is from the US National Tourism and Trade Office.</td>
</tr>
<tr>
<td><a href="https://travel.trade.gov/research/reports/i94/historical/2016.html">I94 Immigration Data</a></td>
<td>data/18-83510-I94-Data-2016/i94_***16_sub.sas7bdat</td>
<td>SAS</td>
<td>This data comes from the US National Tourism and Trade Office.</td>
</tr>
<tr>
<td><a href="https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data">World Temperature Data</a></td>
<td>world_temperature.csv</td>
<td>csv</td>
<td>This dataset contains temperature data of various cities from 1700&#39;s - 2013. This dataset came from Kaggle.</td>
</tr>
<tr>
<td><a href="https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/">U.S. City Demographic Data</a></td>
<td>us-cities-demographics.csv</td>
<td>csv</td>
<td>This dataset contains population details of all US Cities and census-designated places includes gender &amp; race informatoin. This data came from OpenSoft.</td>
</tr>
<tr>
<td><a href="https://datahub.io/core/airport-codes#data">Airport Codes</a></td>
<td>airport-codes_csv.csv</td>
<td>csv</td>
<td>This is a simple table of airport codes and corresponding cities.</td>
</tr>
<tr>
<td>I94_country</td>
<td>i94_countries.csv</td>
<td>csv</td>
<td>Shows corresponding i94 Country of Citizenship &amp; Country of Residence codes. Source : I94_SAS_Labels_Descriptions.SAS</td>
</tr>
<tr>
<td>I94_port</td>
<td>i94_ports.csv</td>
<td>csv</td>
<td>Shows US Port of Entry city names and their corresponding codes. Source : I94_SAS_Labels_Descriptions.SAS</td>
</tr>
</tbody>
</table>
