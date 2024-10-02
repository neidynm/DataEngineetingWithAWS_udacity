 # Project: Data Pipelines with Airflow

 ## Introduction
 A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.



## Project Instructions

### Datasets
For this project, there aretwo datasets. Here are the s3 links for each:
* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song-data

### Copy S3 Bucket
The data for the project is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we will copy the data to your bucket in the same AWS Region where you created the Redshift workgroup so that Redshift can access the bucket.

If you haven't already, create your own S3 bucket using the AWS Cloudshell (this is just an example - buckets need to be unique across all AWS accounts): `aws s3 mb s3://songs-bucket/`

Copy the data from the udacity bucket to the home cloudshell directory:
```
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/
```
Copy the data from the home cloudshell directory to your own bucket -- this is only an example:
```
aws s3 cp ~/log-data/ s3://sean-murdock/log-data/ --recursive
aws s3 cp ~/song-data/ s3://sean-murdock/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://sean-murdock/
```
List the data in your own bucket to be sure it copied over -- this is only an example:
```
aws s3 ls s3://sean-murdock/log-data/
aws s3 ls s3://sean-murdock/song-data/
aws s3 ls s3://sean-murdock/log_json_path.json
```

### Project Init Dag
![Project INIT DAG workflow](/Data%20Pipelines%20with%20Airflow/airflow/dags/project/assets/project_init_dag.png)
Allows the initial tables to be created. Should the tables already exist then those will be dropped.

### Final Project Dag
![Final Project DAG workflow](/Data%20Pipelines%20with%20Airflow/airflow/dags/project/assets/final_project_dag.png)
**Completed DAG Dependencies Image Description**
The Begin_execution task should be followed by both Stage_events and Stage_songs. These staging tasks should both be followed by the task Load_songplays_fact_table. Completing the Load_songplays_fact_table should trigger four tasks at the same time: Load_artist_dim_table, Load_song_dim_table, Load_time_dim_table, and Load_user_dim_table. After completing all of these four tasks, the task Run_dadta_quality_checks_should_run. And, finally, run the Stop_execution task.


### Airflow configuration
After logging in, add the needed connections through the Admin > Connections menu, namely the aws_credentials and redshift connections.
Do not forget to start your Redshift cluster from the AWS console.
After completing all the steps above, run the project_init to instatiate the project and create the inital tables needed. Once this is completed run the final_project DAG.
