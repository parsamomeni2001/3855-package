import connexion
import json
import logging
import logging.config
import requests
import yaml
from flask_cors import CORS
statuses = {
    "receiver": "",
    "storage": "",
    "processing": ""
}


def health():
    return "", 200


def check():
    #Send GET Requests to each service's health endpoint and update the statuses
    try:

        receiver_res = requests.get("http://localhost:8080/health")
        statuses["receiver"] = "Running" if receiver_res.status_code == 200 else "Down"
    
        storage_res = requests.get("http://localhost:8090/health")
        statuses["storage"] = "Running" if storage_res.status_code == 200 else "Down"

        processing_res = requests.get("http://localhost:8100/health")
        statuses["processing"] = "Running" if processing_res.status_code == 200 else "Down"

    except requests.exceptions.RequestException:
            #If there is an exception while sending a request, mark the corresponding service as down
            statuses["receiver"] = "Down"
            statuses["storage"] = " Down"
            statuses["processing"]= "Down"

    return statuses # ignore the instructions to convert this to JSON and leave as-is


app = connexion.FlaskApp(__name__, specification_dir='')

# if you are deploying this to your VM, make sure to add base_path="/health" to the add_api method (and update your NGINX config to proxy requests to the health service)
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)
app.add_url_rule("/health/receiver", "health", health)
app.add_url_rule("/health/storage", "health", health)
app.add_url_rule("/health/processing", "health", health)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    app.run(port=8110)