from http import HTTPStatus
from flask import Flask
from flask import make_response, jsonify

flask = Flask(__name__)


@flask.route('/')
def home():
    return make_response(
        jsonify({'app_name': 'bitcoin-price-predictor',
                 'version': 'v1'}), HTTPStatus.OK
    )
