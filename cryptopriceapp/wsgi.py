from cryptopriceapp.api.main import flask

if __name__ == '__main__':
    flask.run(
        host='0.0.0.0',
        port=9000,
        debug=True
    )
