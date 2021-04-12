import json
import requests
import sqlalchemy
from ratelimit import limits, sleep_and_retry
import datetime
import time
import copy
import logging
import src.utils.tools as T
import pulsar
import time
from datetime import datetime
from jsonschema import validate
import json
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd

# Load configuration
config = T.get_configuration("/consumers/conf/consumer.conf")

print(config.sections())

# API Configuration
sixty_seconds = int(config["API"]["sixty_seconds"])
nb_calls = int(config["API"]["nb_calls"])
api_url_base = config["API"]["api_url_base"]
api_token = config["API"]["api_token"]

# PULSAR Configuration
service_url = config.get('PULSAR','service_url')
pulsar_token = config["PULSAR"]["token"]
trust_certs = config["PULSAR"]["certs"]

# Topic
topic_name = config["TOPIC.HR"]["topic_users_data"]

# Msg Schema Path
schema_file_path = config["TOPIC.HR"]["topic_schema_path"]

# SQLite Config
db_file_path = config["SQLITE"]["db_file_path"]

# File & Directory configuration
app_name = 'hr_users_consumer'

def read_json_schema(schema_file_path):
    """
    This function is used to read json schema stored locally
    :param schema_file_path:
    :return: schema
    """
    with open(schema_file_path) as f:
        schema = json.load(f)
    return schema

def validateJSON(jsonData):
    """
    This function is used to validate json received by our consumer, next step could be to use Avro and Schema Registry
    :param jsonData:
    :return: Boolean
    """
    try:
        json.loads(jsonData)
        validate(instance=json.loads(jsonData), schema=read_json_schema(schema_file_path))
    except Exception as err:
        logging.error(err)
        logging.info(" Message received is not correct ")
        logging.info(" Message sent to Pulsar Rejection Topic for reprocessing")
        # IF a message is not correct, I prefer to stop the consumer and fix the problem. Another way will be to
        # Send message to another to topic if the message is not valid and change raise below by pass.
        raise
        return False

    return True

def transform(msg):
    """
    Enrich data to know if the message is an update or not, this function could be used to enrich the entire message
    :param msg:
    :return:
    """
    try:
        msg_json = json.loads(msg)
        df = pd.json_normalize(msg_json['data'])
        df['updated_at'] = pd.to_datetime(
            df['updated_at'].str[:-10].str.replace("T", "").str.replace("-", "").str.replace(":", ""),
            format="%Y%m%d%H%M%S")

        df['created_at'] = pd.to_datetime(
            df['created_at'].str[:-10].str.replace("T", "").str.replace("-", "").str.replace(":", ""),
            format="%Y%m%d%H%M%S")

        df["diff_date"] = (df['updated_at'] - df['created_at']).dt.days.astype('int16')

        #New column to know if data is created or updated
        df['CDC'] = df['diff_date'].apply(lambda x: 'CREATED' if x <= 0 else 'UPDATED')

    except Exception as e:
        logging.error("There is an issue with transformation : {0}".format(str(e)))
    return df

if __name__ == "__main__":

    start_time = time.time()

    T.init_logger('a', app_name)

    logging.info("Start Consumer at : {}".format(datetime.now()))

    try:
        # Init the client
        client = T.init_client(service_url, trust_certs, pulsar_token)

        # Init the producer without schema to send max of data, I'll control data in consumer
        # I enable batching, the producer will accumulate and send a batch of messages in a single request.
        consumer = client.subscribe(topic_name, \
                                    app_name,\
                                    initial_position=pulsar.InitialPosition.Latest)

        # We run the process all of the time, it will crash if there is a problem. By Using Retention time, we'll have time to retablish and
        # don't lost any data.
        while True:
            try:
                msg = consumer.receive(3000)
                logging.info("Received message '{}' id='{}', partition_key='{}'".format(msg.data(),
                                                                                        msg.message_id(),
                                                                                        msg.partition_key()))

                logging.info( " Check if message is valid based on our Contract Schema (Can use Avro...)")

                if validateJSON(msg.value()): # If not the process will be stopped
                    logging.info(" Message received is correct ")
                    logging.info(" Message transformed")
                    df = transform(msg.value())
                    logging.info(" Message sent to SQLITE DB")
                    T.save_df_to_sqlite(db_file_path, df, "users")

                # Acknowledging the message to remove from message backlog
                consumer.acknowledge(msg)

                waitingForMsg = False
            except Exception as e:
                print("Still waiting for a message...")
            #time.sleep(1)
        logging.info(" Time Duration {} seconds for {} messages".format(str(time.time() - start_time),len(list_ids)))

    except Exception as e:
        logging.error("there is a problem in consumer : {0}".format(str(e)))
        logging.info(" Time Duration for {} messages".format(str(time.time() - start_time),0))
        # If there is an issue I stop the producer
        raise
