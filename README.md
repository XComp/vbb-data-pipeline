# GTFS Pipeline Project

This small project is about playing around with the Airflow framework. The initial goal was to get the GTFS data 
regularly provided by Verkehrverbund Berlin/Brandenburg (VBB) and storing the data in a PostgreSQL database. This scope 
is going to change in a way that other sources should be added as well.

## Helpful resources

* [Curated list of Airflow documentation](https://github.com/jghoman/awesome-apache-airflow): I found a few nice articles and examples about Airflow on this list (like the ones listed below). 
* [puckel's Airflow Docker image](https://github.com/puckel/docker-airflow): Docker image `puckel/docker-airflow` for easy setup of an Airflow infrastructure.
* [ETL Best Practices with airflow](https://gtoonstra.github.io/etl-with-airflow/index.html): Good collection of example project for Airflow.
* [Tips & Tricks by Kaxil Naik](https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f): Some small tips on how to improve the code.
* [astronomer.io](https://www.astronomer.io/guides/using-airflow-plugins/): Interesting insight on how to organize the plugins.
* [sfeir.com](https://lemag.sfeir.com/installing-and-using-apache-airflow-on-the-google-cloud-platform/): Interesting walk-through of installing Airflow on GCP instances using CloudSQL

## Setup

Starting the project is done by running `docker-compose up -d` from the project's base folder. The Airflow webserver can be 
accessed via [localhost:8080](http://localhost:8080/).

The project can be stopped calling `docker-compose down` from the project's base folder.
