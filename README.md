# Producer HR Data

The goal of our consumer is to consume data from topic in Kafka and then load valid data in SQLITE and reject data in another topic for
reprocessing. I use a Bath approach but it will be totally possible to switch quickly and easily to streaming technologie like
Apache Spark (Streaming). If you want to use it you have to register in Kesque and gorest then change only configuration file - consumer.conf et crt.

## The Architecutre

```
├───configuration : config files ()
├───db : the sqlite db file is used to store data from pulsar, when you run using docker the data is inside the container, if you want you can link you local to your container
├───ddl : SQL script(s) for creating the database tables
├───logs : the logs by python scrpits
├───src : python scripts
└───tests : test scripts
└───Dockerfile : My Docker File to create a environment and run my consumer
└───requirements.txt : Librairies used in this project
```

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Run the Producer

To run this project you can use Dockerfile or use Venv in your local environment
 
 Using Docker : 
    - Create your image based on my docker File : ``` docker build -t consumers . ``` then run ``` docker run consumers ```  
 
 Using Venv : 
    - Create a virtual environment using pipenv and then install librairies using ```pip3 install -r requirements.txt```
    - Run hr_producer located in src/producers/hr_producer
    

### For the unit test

Run the code below to test the hr producer ( Not Available for now )

```
pytest -q test.py
```

## Deployment


## Authors

* **Youssef HISSOU**