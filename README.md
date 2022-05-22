## Data Enginerring Project - Data Lake

Consider a scenario given as below. A music streaming startup, Sparkify, has very strong annual growth on their user base. The database they built in the beginning is not appropricate for their bussiness demand anymore. As their data engineer, we plan to migrate all our data to cloud because of its cost-effective, and scalability. In this project, we will practice what we have learnt about Data Lake to build our analytics tables. 


To deal with large amounts of input data, we could use AWS EMR cluster plus parallel processing tool - spark. The procedure can be divided into the following steps: 

1. Loading data from S3 using Spark
2. Process the data into analytics tables using Spark
- we can either process our data in workspace or in our own EMR cluster. 
3. Save these tables in columnar form back into S3

## Usage

Execute the following command in your command line. It might takes 15 mins for running etl.py.

```python
~ % python etl.py
```

## Database schema design and ETL pipeline

1. staging tables 
We basically follow all the attributes in JSON file to build our staging tables. It's straightforward so I'll skip this part.

2. final analytics tables
We use star-schema for this work. The fact table is used to record bussiness event. In our case, the event is being recorded whenever a user plays a song.
-- Fact Table (songplays)
-- Dimension Table (users, songs, times, artists)

![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/star-scheme.png)  

Here, we attach some scheme format, and part of its query result for reference.  

Fact table - songplays
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/songplays-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/songplays-query-result.png)  

Dimension table - users  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/users-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/users-query-result.png)  

Dimension table - times  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/times-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/times-query-result.png)  

## File Descriptions

1. etl.py - pyspark script for ETL process.  
2. dwg.cfg - The config file contains AWS login information.
3. README.md - The description File including usage, purpose and summary.
