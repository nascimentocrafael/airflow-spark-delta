# Airflow Spark Delta

**THIS PROJECT IS A FORK FROM [airflow-spark](https://github.com/cordon-thiago/airflow-spark) WITH UPDATED VERSIONS OF PYTHON, POSTGRES, AIRFLOW, SPARK AND THE ADDITION OF DELTA (delta-spark==2.3.0).**

PREREQ TO RUN ON WINDOWS:

	* [WSL 2.0](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)
	* [Docker](https://docs.docker.com/desktop/install/windows-install/)
	* You might need to change line endings from CRLF to LF of Dockerfiles inside docker/\*/, docker/docker-airflow/entrypoint.sh, docker/docker-compose.yml and docker/docker-airflow/pg-init-scripts/init-user-db.sh. 

This project contains the following containers:

* postgres: Postgres database for Airflow metadata and a Test database to test whatever you want.
    * Image: postgres:13.10-alpine
    * Database Port: 5432
    * References: https://hub.docker.com/_/postgres

* airflow-webserver: Airflow webserver and Scheduler.
    * Image: docker-airflow-spark:2.5.3_3.3.2
    * Port: 8282
	* Based on: PYTHON:3.9 using AIRFLOW 2.5.3, SPARK 3.3.2 and HADOOP 3

* spark: Spark Master.
    * Image: bitnami/spark:3.3.2
    * Port: 8181
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.
    * Image: bitnami/spark:3.3.2
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

* jupyter-spark: Jupyter notebook with pyspark for interactive development.
  * Image: jupyter/pyspark-notebook:spark-3.3.2
  * Port: 8888
  * References: 
    * https://hub.docker.com/layers/jupyter/pyspark-notebook/spark-3.1.2/images/sha256-37398efc9e51f868e0e1fde8e93df67bae0f9c77d3d3ce7fe3830faeb47afe4d?context=explore
    * https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook
    * https://hub.docker.com/r/jupyter/pyspark-notebook/tags/

## Architecture components

![](./doc/architecture.png "Architecture")

## Setup

### Clone project

    $ git clone https://github.com/nascimentocrafael/airflow-spark-delta

### Build airflow Docker

Inside the airflow-spark/docker/docker-airflow

    $ docker build --rm --force-rm -t docker-airflow-spark:2.5.3_3.3.2 .

### Start containers

Inside the airflow-spark/docker create airflow user:

	$ docker-compose run airflow-webserver airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin


Start the containers:

    $ docker-compose up

If you want to run in background:

    $ docker-compose up -d

Note: when running the docker-compose for the first time, the images of `postgres`, `bitnami/spark` and `jupyter/pyspark-notebook` will be downloaded before the containers started.

### Check if you can access

Airflow: http://localhost:8282

Spark Master: http://localhost:8181

PostgreSql - Database Test:

* Server: localhost:5432
* Database: test
* User: test
* Password: postgres

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow

Jupyter Notebook: http://127.0.0.1:8888
  * For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:
  
        $ docker logs -f docker_jupyter-spark_1

## How to run a DAG to test

1. Configure spark connection acessing airflow web UI http://localhost:8282 and going to Connections
   ![](./doc/airflow_connections_menu.png "Airflow Connections")

2. Edit the spark_default connection inserting `spark://spark` in Host field and Port `7077`
    ![](./doc/airflow_spark_connection.png "Airflow Spark connection")

3. Run the spark-test DAG
   
4. Check the DAG log for the task spark_job. You will see the result printed in the log
   ![](./doc/airflow_dag_log.png "Airflow log")

5. Check the spark application in the Spark Master web UI (http://localhost:8181)
   ![](./doc/spark_master_app.png "Spark Master UI")

## How to run the Spark Apps via spark-submit
After started your docker containers, run the command below in your terminal:
```
$ docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 <spark_app_path> [optional]<list_of_app_args>
```

Example running the hellop-world.py application:
```
$ docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 /usr/local/spark/app/hello-world.py /usr/local/spark/resources/data/airflow.cfg
```

## Increasing the number of Spark Workers

You can increase the number of Spark workers just adding new services based on `bitnami/spark:3.1.2` image to the `docker-compose.yml` file like following:

```
spark-worker-n:
        image: bitnami/spark:3.3.2
        user: root
        networks:
            - default_net
        environment:
            - SPARK_MODE=worker
            - SPARK_MASTER_URL=spark://spark:7077
            - SPARK_WORKER_MEMORY=1G
            - SPARK_WORKER_CORES=1
            - SPARK_RPC_AUTHENTICATION_ENABLED=no
            - SPARK_RPC_ENCRYPTION_ENABLED=no
            - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
            - SPARK_SSL_ENABLED=no
			- PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
            - PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3
        volumes:
            - ../spark/app:/usr/local/spark/app # Spark scripts folder (Must be the same path in airflow and Spark Cluster)
            - ../spark/resources/data:/usr/local/spark/resources/data #Data folder (Must be the same path in airflow and Spark Cluster)

```

## Adding Airflow Extra packages

Rebuild Dockerfile (in this example, adding GCP extra):

    $ docker build --rm --build-arg AIRFLOW_DEPS="gcp" -t docker-airflow-spark:2.5.3_3.3.2 .

After successfully built, run docker-compose to start container:

    $ docker-compose up

More info at: https://github.com/puckel/docker-airflow#build

## Useful docker commands

    List Images:
    $ docker images <repository_name>

    List Containers:
    $ docker container ls

    Check container logs:
    $ docker logs -f <container_name>

    To build a Dockerfile after changing sth (run inside directoty containing Dockerfile):
    $ docker build --rm -t <tag_name> .

    Access container bash:
    $ docker exec -i -t <container_name> /bin/bash

## Useful docker-compose commands

    Start Containers:
    $ docker-compose -f <compose-file.yml> up -d

    Stop Containers:
    $ docker-compose -f <compose-file.yml> down --remove-orphans
    
# Extras
## Spark + Delta sample

* The DAG [spark-delta-test.py](dags/spark-delta-test.py) loads [movies.csv](spark/resources/data/movies.csv)  data into delta tables.
  * This DAG runs the load-delta.py application.
  * To read the delta table you can use the [read-delta-notebook](notebooks/read-delta-notebook.ipynb)