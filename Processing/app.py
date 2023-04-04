import connexion
from connexion import NoContent
import datetime
import json
import logging
import logging.config
import requests
import yaml
import apscheduler
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from flask_cors import CORS

import sqlite3
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from stats import Stats

DB_ENGINE = create_engine("sqlite:///stats.sqlite")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def get_latest_stats():
    session = DB_SESSION()
    result = session.query(Stats).order_by(Stats.last_updated.desc()).first()
    session.close()

    #if result:
    #    return {'max_buy_price': result.max_buy_price}, 200
    #return NoContent, 201

    if result:
        return result.to_dict(), 200
    return NoContent, 201

def populate_stats():
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    logging.info(f'Processing stsrted at {timestamp}')
    
    session = DB_SESSION()

    result = session.query(Stats).order_by(Stats.last_updated.desc()).first()

    result = result.to_dict()
    last_updated = result['last_updated']

    buy_response = requests.get(f'http://localhost:8090/buy?timestamp={last_updated}')
    buy_events = buy_response.json()

    for buy_event in buy_events:
        if buy_event['item_price'] > result['max_buy_price']:
            result['max_buy_price'] = buy_event['item_price']
    # max_buy_price = max([event['price'] for event in buy_events] + [result['max_buy_price']])
    num_buys = len(buy_events) + result['num_buys']

    sell_response = requests.get(f"http://localhost:8090/sell?timestamp={last_updated}")
    sell_events = sell_response.json()
    for sell_event in sell_events:
        if sell_event['item_price'] > result['max_sell_price']:
            result['max_sell_price'] = sell_event['item_price']
    # max_sell_price = max([event['price'] for event in sell_events] + [result['max_sell_price']])
    num_sells = len(sell_events) + result['num_sells']

    stat = Stats(
        max_buy_price=result['max_buy_price'],
        num_buys=num_buys,
        max_sell_price=result['max_sell_price'],
        num_sells=num_sells,
        last_updated=timestamp
    )
    session.add(stat)
    session.commit()
    session.close()


    return NoContent, 201

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['period'])
    sched.start()
    
def health():
    return "", 200   


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/processing",
 strict_validation=True, validate_responses=True)
CORS(app.app)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
