from flask import Flask, redirect, jsonify, request
from flask.helpers import send_from_directory
from config import Config
from flask_mongoengine import MongoEngine
from flask_swagger_ui import get_swaggerui_blueprint
from app.service import start_service, stop_service


# Flask Initialization
app = Flask(__name__)

# Initialize jwt manager using the secret key
#app.config['JWT_SECRET_KEY'] = 'python-flask-microservices'
#jwt = JWTManager(app)

service_running = {"status": False}

# Redirect to Swagger docs
@app.route('/api', methods=['GET'])
def api_docs():
    """
    Show a message indicating that the service is running.
    """
    return jsonify(message="Service is running.")


@app.route(Config.START, methods=['POST'])
def handle_start_service():
    """
    Proxy to start_service function.
    """
    return start_service()


@app.route(Config.STOP, methods=['POST'])
#@jwt_required()
def handle_stop_service():
    """
    Proxy to stop_service function.
    """
    return stop_service()


def initialize_app():
    SWAGGER_URL = '/api/docs'
    API_URL = '/swagger/swagger.json'

    swaggerui_blueprint = get_swaggerui_blueprint(
        SWAGGER_URL,
        API_URL,
        config={
            'app_name': "Python Microservices"
        })
    app.register_blueprint(swaggerui_blueprint)

    return app
