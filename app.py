import datetime
import json

import connexion
from connexion import NoContent
import swagger_ui_bundle

import sqlite3
import yaml
import logging
import logging.config

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell

import pykafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

import threading
from threading import Thread


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Set up logging
#logging.config.fileConfig('log_conf.conf')
#logger = logging.getLogger('basic')

DB_ENGINE = create_engine(f"sqlite:///events.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
     # Set up Kafka client and topic
    client = KafkaClient(hosts=app_config['events']['hostname'] + ':' + str(app_config['events']['port']))
    topic = client.topics[str.encode(app_config['events']['topic'])]

    # Set up Kafka consumer to read latest events
    messages = topic.get_simple_consumer(
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    # Process incoming events
    for msg in messages:
        # Decode message value to string and parse as JSON object
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)

        # Extract payload and event type from message
        payload = msg['payload']
        msg_type = msg['type']

        # Create new database session
        session = DB_SESSION()

        # Log event storage
        logger.info("CONSUMER::storing %s event", msg_type)
        logger.info(msg)

        # Store event in database
        if msg_type == 'buy':
            event = Buy(**payload)
        elif msg_type == 'sell':
            event = Sell(**payload)

        session.add(event)
        session.commit()
        session.close()

    # Store new read position for consumer
    messages.commit_offsets()

    
    
    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    
    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic

    # Notes:
    #
    # An 'offset' in Kafka is a number indicating the last record a consumer has read,
    # so that it does not re-read events in the topic
    #
    # When creating a consumer object,
    # reset_offset_on_start = False ensures that for any *existing* topics we will read the latest events
    # auto_offset_reset = OffsetType.LATEST ensures that for any *new* topic we will also only read the latest events
    
        # This blocks, waiting for any new events to arrive

        # TODO: decode (utf-8) the value property of the message, store in a variable named msg_str
        
        # TODO: convert the json string (msg_str) to an object, store in a variable named msg

        # TODO: extract the payload property from the msg object, store in a variable named payload

        # TODO: extract the type property from the msg object, store in a variable named msg_type

        # TODO: create a database session

        # TODO: log "CONSUMER::storing buy event"
        # TODO: log the msg object

        # TODO: if msg_type equals 'buy', create a Buy object and pass the properties in payload to the constructor
        # if msg_type equals sell, create a Sell object and pass the properties in payload to the constructor
        
        # TODO: session.add the object you created in the previous step
        
        # TODO: commit the session

    # TODO: call messages.commit_offsets() to store the new read position

# Endpoints
def buy(body):
    # TODO: copy over code from previous version of storage
    session = DB_SESSION()
    buy= Buy(**body)

    session.add(buy)

    session.commit()
    session.close()

    logger.info("Stored buy event: {}".format(body))

    return NoContent, 201

# 

def get_buys(timestamp):
    # TODO: copy over code from previous version of storage
    session = DB_SESSION()
    #fix the error 
  
    # if not timestamp:
        # timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = session.query(Buy).filter(Buy.date_created >= timestamp)

    data = []

    for row in rows:
        row = row.to_dict()
        data.append(row)

    session.close()

    print("Request to get_buys with timestamp: {timestamp}, returned {len[data]} results")

    return data, 200


def sell(body):
    # TODO: copy over code from previous version of storage
    session = DB_SESSION()

    sell = Sell(**body)
    session.add(sell)
    session.commit()
    session.close()

    return NoContent, 201
# end

def get_sells(timestamp):
    # TODO: copy over code from previous version of storage
    # TODO create a DB SESSION
    session = DB_SESSION()
  
    # if not timestamp:
        # timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = session.query(Sell).filter(Sell.date_created >= timestamp)

    data = []

    for row in rows:
        row = row.to_dict()
        data.append(row)


    session.close()

    print("Request to get_buys with timestamp: {timestamp}, returned {len[data]} results")

    return data, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)


with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.run(port=8090)