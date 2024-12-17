from flask import jsonify, request


# Global variable to track service status
service_running = {"status": False}

def start_service():
    """
    Start the Flask Service.
    """
    if service_running["status"]:
        return jsonify({"message": "Service is already running."}), 400

    service_running["status"] = True
    print("Service started")
    return jsonify({"message": "Service started successfully."}), 200


def stop_service():
    """
    Stop the Flask Service.
    """
    if not service_running["status"]:
        return jsonify({"message": "Service is not running."}), 400

    service_running["status"] = False
    print("Service stopped")
    shutdown = request.environ.get('werkzeug.server.shutdown')
    if shutdown is None:
        return jsonify({"message": "Unable to shut down server programmatically."}), 500
    shutdown()
    return jsonify({"message": "Service stopped successfully."}), 200
