# Human Resources Analytics

This project aims to find an explanation for the high employee turnover in the given dataset. It collects JSON and XML data files, an SQL database and loads them into a bucket. After that, it processes the data, uploads it to another bucket and creates a machine learning model to predict whether an employee will leave or stay with the company. The data lake is built using minio, the data processed using pandas and the machine learning model is generated and optimized with pyCaret.

## Installation

Make sure you have Docker and Git installed on your machine.

1. Create a folder for the project and clone the repository inside it

   ```bash
   git clone https://github.com/juniormach96/human-resources-analytics .
   ```
2. Open the terminal from inside the project folder. This is important because we are going to use PWD variable to map the docker volumes, so make sure you type the following commands from the right place.
3. Install a MySql Server

   ```bash
   docker run --name mysqldb1 -v "$PWD/database:/database" -e MYSQL_ROOT_PASSWORD=admin -p "3307:3306" -d mysql
   ```
4. Install minio Data Lake

   ```bash
   docker run -d -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" quay.io/minio/minio server /data --console-address ":9001"
   ```
5. Install Airflow

   ```bash
   docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow2 apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password admin --firstname FirstName --lastname LasName --role Admin --email admin@example.org); airflow webserver & airflow scheduler'
   ```
6. Go to http://localhost:8080 to access Airflow UI, then login with the user 'admin' and the password 'admin'
7. Go to http://localhost:8080/variable/list/, then click on choose file and select the airflow_variables.json to add the variables to the environment.
8. Now, let's import the database from the .sql file. For this, we need to access the container to start typing commands from within it.

   ```bash
   docker exec -it mysqldb1 bash
   ```
9. Access the database folder

   ```bash
   cd database
   ```
10. create the employees db

    ```bash
    mysql --user="root" --password="admin" < "employees_db.sql"
    ```
11. Now everything is set, you can access http://localhost:8080/ to trigger the DAGs and http://127.0.0.1:9001/ to enter the data lake, using 'minioadmin' for login and password
