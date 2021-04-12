import logging
import coloredlogs
import sys
import configparser
import pulsar
import sqlite3

import sqlalchemy


def init_logger(mode, app_name):
    """
    Init logger used for each script
    :param logs_file_path:
    :param mode: mode used for storage (a -> append)
    :param app_name: script name
    :return:
    """
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='/consumers/logs/collect.log',
                        filemode=mode)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    # Now, we can log to the root logger, or any other logger. First the root...
    # application:

    logger1 = logging.getLogger(app_name)

    # If you don't want to see log messages from libraries, you can pass a
    # specific logger object to the install() function. In this case only log
    # messages originating from that logger will show up on the terminal.
    coloredlogs.install(level='INFO', logger=logger1, fmt='%(asctime)s : %(name)s : %(levelname)s %(message)s')

def get_configuration(file_path):
    """
    This function is used to retrieve configuration from yaml file located in conf directory
    :param file_path: 
    :return: 
    """
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def init_client(service_url, trust_certs, token):
    """
    Init client for Pulsar hosted in Kesque
    :param service_url:
    :param trust_certs:
    :param token:
    :return:
    """
    client = pulsar.Client(service_url,
                           authentication=pulsar.AuthenticationToken(token),
                           tls_trust_certs_file_path=trust_certs)
    return client

def get_users_id_from_db(db_file_path):
    """
    This method is used for connecting to sql lite database and get all user ID to get from API
    It simulate operationnal sysem to know which data to keep. Best way will be to ask Rest API to get latest Data
    But I did not find a way this simple gorest API.
    :param db_file_path: Database Path Location
    :param script_to_excute: STR Script to execute
    :return: returns nothing
    """
    logging.info('Connect to the database')
    try:
        sqliteConnection = sqlite3.connect(db_file_path)

        cursor = sqliteConnection.cursor()
        cursor.execute("select USER_ID from USER_TO_GET")
        cursor = cursor.fetchall()
        list_ids = [value[0] for value in cursor]
        sqliteConnection.close()
        return list_ids

    except sqlite3.Error as error:
        raise Exception('Error while connecting to sqlite: {}'.format(error))
    finally:
        if (sqliteConnection):
            sqliteConnection.close()


def save_df_to_sqlite(db_file_path, df, table_name):
    """
    Function used to save dataframe in sqlite
    :param db_file_path:
    :param df:
    :param table_name:
    :return:
    """
    try:
        engine = sqlalchemy.create_engine('sqlite:///'+db_file_path, echo=False)
        # It can be very slow with huge data, better to use another approach
        df.to_sql(table_name, con=engine, if_exists='append')
    except Exception as e:
        logging.error(" There is an issue with sqlite loading : {0}".format(str(e)))
        raise

    return True
