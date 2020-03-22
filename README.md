<p align="center">
  <img width="730" height="370" src="https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/project_logo.jpg">
</p>

# Automatic DataPipeline between Cloud Data Lake and Cloud Data Warehouse
---
### PROJECT BACKGROUND AND DESCRIPTION
###### *BACKGROUND*
A startup company recently moves its data storage and data processing server to AWS cloud data lake (AWS S3) and AWS cloud data processing cluster (AWS spark cluster). The introduction of AWS cloud services improves this company's data storage and data processing ability. Now, this company wants more. There are two things this company wants to achieve in this project.

First, **use AWS Redshift to build a cloud data warehouse.** In the past, it is hard for other departments to access the user data or the song data which is collected by this company. If the data can be shared by other departments, it may benefit this company's business. Thus, this company decides to set up a cloud data warehouse that stores clean and structured data. By having this cloud data warehouse, other departments can easily access the data and use the data to improve this company's business.

Second, **use AWS serverless services, AWS Glue and AWS Athena, to build an immediate data searching and analysis service for data analysis team.** As in the early days, the data analysis team only can check the data or conduct data analysis tasks when data arrives in the database or data warehouse. It is hard for them to check the data or conduct the data analysis when data just arrives at the data lake. If the data analysis team can check the data or conduct the data analysis when data just arrives, it may find some valuable points and these points help data engineers to improve the data pipeline. Therefore, this company considers introducing AWS Glue and AWS Athena to create an immediate data searching and analysis service which allows the data analysis team to use SQL queries to check or analyze the data in the data lake.

###### *PROJECT DESCRIPTION*
This project has **two goals**. 

1. **Launch a cloud data warehouse by using AWS Redshift :** 

    Considering the query efficiency in the cloud data warehouse, we decide to use star schema to store our data. And, we build an automatic data pipeline between cloud data lake (AWS S3) and cloud data warehouse (AWS Redshift) by using Apache Airflow. This data pipeline automatically extracts data from the cloud data lake, clean the data, dump data in a stage table in Redshift, execute the data quality check, and store data in the final table in Cloud data lake with a star schema. This automatic data pipeline helps engineers to reduce the burden of executing and maintaining the ETL data pipeline manually.

2. **Build an immediate data searching and analysis service :** 

    It allows the data analysis team to use SQL query to check or analyze the data in the data lake.


### SYSTEM ARCHITECTURE
<p align="center">
  <img width="850" height="550" src="https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/system_architecture_new.jpeg">
</p>

##### *DATA PIPELINE DETAILS*
- **DAG for this project**
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/DAG.jpg)
- **Explanation for each task in DAG**  
    1. **Begin_execution**: the start of the data pipeline.  
    2. **Stage_events**: Create Stage_events table in Redshift, load events json data from S3 and save the data into Stage_event table  
    3. **stage_songs**: Create Stage_songts table in Redshift, load songs json data from S3 and save the data into Stage_song table  
    4. **Load_songplays_fact_table**: Create songplay table in Redshift, Load data from Stage_event and Stage_song table, join these two tables, and save the joined result in songplay table  
    5. **Load_user_dim_table**: Create user table in Redshift, Load user table relating data from songplay table, and save the data in user table  
    6. **Load_song_dim_table**: Create song table in Redshift, Load song table relating data from songplay table, and save the data in song table  
    7. **Load_time_dim_table**: Create time table in Redshift, Load time table relating data from songplay table, and save the data in time table  
    8. **Load_artist_dim_table**: Create artist table in Redshift, Load artist table relating data from songplay table, and save the data in artist table  
    9. **Data_quality_checks_users**: Check how many rows in user table, if it is less than 1, raise an error.  
    10. **Data_quality_checks_songs**: Check how many rows in song table, if it is less than 1, raise an error.  
    11. **Data_quality_checks_time**: Check how many rows in time table, if it is less than 1, raise an error.  
    12. **Data_quality_checks_artists**: Check how many rows in artist table, if it is less than 1, raise an error.  


- **Other requirements for this DAG**  
    1. The DAG does not have dependencies on past runs
    2. On failure, the task are retried 3 times
    3. Retries happen every 5 minutes
    4. Catchup is turned off
    5. don't email on retry
#
##### *DATA MODELING*
- **Data modeling for this project**
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/datamodel.jpg)
- **Explanation for each table**  
    1. **songplays table (fact table)**: contain data associated with song plays. The column in this table are songplay_id, start_time, user_id, song_id, artist_id, session_id, location, user_agent
    2. **users Tables (dimensional table)**: contain data associated with users. The column in this table are user_id, first_name, last_name, gender, level
    3. **songs Tables (dimensional table)**: contain data associated with songs. The column in this table are song_id, title, artist_id, year, duration
    4. **artists Tables (dimensional table)**: contain data associated with artists. The column in this table are artist_id, name, location, lattitude, longitude
    5. **time Tables (dimensional table)**: contain data associated with time. The column in this table are start_time, hour, day, week, month, year, weekday

------------
#### FILES IN THE REPOSITORY
**dags folder**: It contains files which relates to DAG.
1. **Sparkify_datapipeline_dag.py**: a python script which is used for defining the DAG

**plugins folder**: It contains two folders, helpers and operators.  
----**helpers folder**: 
1. **sql_queries.py**: a python script which defines the SQL command that will be used in this project. The operators folder has four

----**operators folder**: 
1. **data_quality.py**: a python script which defines DataQualityOperator
2. **load_dimension.py**: a python script which defines LoadDimensionOperator
3. **load_fact.py**: a python script which defines LoadFactOperator
4. **stage_redshift.py**: a python script which defines StageToRedshiftOperator


------------
#### HOW TO RUN THE PROJECT
**The step of running the project is listed below.**
1. change directory to the folder you install your airflow. Then, find start.sh file. In this project, the start.sh file is located on **/opt/airflow/start.sh**. Thus, we type `/opt/airflow/start.sh` in our terminal to launch the Airflow server.

2. access to Airflow webUI. Generally speaking, you can access to the webUI through this address **http://localhost:8080**

3. configure AWS credentials (it allows us to access to AWS account)
3-1. Admin > Connections
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/1.jpg)
3-2. Click Create
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/2.jpg)
3-3. type **AWS access key id** and **secret access key**
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/3.jpg)  

4. configure Redshift connection
4-1. Admin > Connections
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/1.jpg)
4-2. Click Create
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/2.jpg)
4-3. type Redshift information in these columns
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/4.jpg)  

5. start the data pipeline
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/5.jpg)

6. monitor the data pipeline
![](https://github.com/ChunYen-Chang/Automatic-DataPipeline-between-Cloud-datalake-and-Cloud-datawarehouse/blob/master/images/6.jpg)
