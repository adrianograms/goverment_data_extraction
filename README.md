# Brazilian Goverment Data Extraction

This project was developed to extract data from a investment API available by the Brazilian government, to provide valuable information about the projects being funded by the national government.

The technologies used on this project were: Python, Pyspark, Apache Airflow.

## Pipeline
The pipeline for this project consist of a extraction from a investment government project api (previously it had a extraction for a financial execution api as well, but, because of problems with the api itself, i decided to remove it from the project, but you still can find the tasks and functions related to this part for history purposes). After the extraction, the data was stored on json files that were extracted and transformed to a relational format to store in a postgres database. \
For last, the data was converted in a dimensional format, with the creation of dimensions and facts based on the data extracted.

You can see the entire pipeline on the picture bellow.

![alt text](images/image.png)


## Test
For testing, we have a ipynb notebook with some tests configured to run manually and test the pipeline without the need to executing the entire dag, being possible to run a single task or group tasks (wrapped on test dags). The path for this notebook is source/test

## Database DDL
For the database DDL, you can find it on scrips/ddl, the user used to create all the tables was called devops, so if you want to only restore, remember to create a user with this name. 