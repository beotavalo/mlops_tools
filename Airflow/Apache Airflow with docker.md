1. Install Docker
Make sure you have Docker installed on your machine. If you don't have it, you can download and install it from here: https://docs.docker.com/engine/install/.


2. Set Up Docker Compose for Airflow
You can use the official docker-compose setup provided by Airflow. It’s a convenient way to get all necessary services (like the webserver, scheduler, and database) up and running.

3. Create Necessary Folders
Airflow needs folders for DAGs, logs, and plugins. Create those in the same directory as the docker-compose.yml file:

```
mkdir dags logs plugins
```

4. Initialize the Airflow Database
Go to the airflow-docker folder

```
cd Module-1/Lab\ Notebooks/airflow-docker

```

And then run the following command to initialize the Airflow metadata database:

```
docker-compose up airflow-init

```

This will initialize the database in the PostgreSQL container.

5. Start Airflow Containers
After the database is initialized, you can start Airflow services (webserver, scheduler, etc.) with:
```
docker-compose up

```
This command will spin up the containers, including the webserver and scheduler.

6. Access the Airflow Web UI
Once the containers are up, you can access the Airflow user interface by navigating to:

```
http://localhost:8080

```
You should now see the Airflow UI where you can monitor and manage your DAGs.

7. Set Up an Admin User (Optional)
By default, Airflow does not set up authentication for the web UI. If you want to enable user authentication, you'll need to create an admin user:

```
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

```

8. Stop Containers
To stop the running containers, press Ctrl+C or run:

```
docker-compose down

```

9. Adding DAGs
You can add your DAGs by placing them in the dags/ folder. The DAGs will automatically be loaded into Airflow's UI.

With these steps, you'll have Apache Airflow running with a web-based interface using Docker. You can now monitor and manage your pipelines through the UI.