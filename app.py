#!/usr/bin/python3
import json
import os.path

from flask import Flask, abort, jsonify, request

from tuya_lib import TuyaApi, TwoSwitches

is_server = os.path.exists("/SERVER/is_server")
prefix = "/tuya-iot-api" if is_server else ""
with open("tokens.json") as f:
    data = json.load(f)

API_KEY = data.get("api-key")

app = Flask(__name__)
tuya_api = TuyaApi.from_data(data)
tuya_api.add_device_from_data(TwoSwitches, data, "room-light")


@app.post(prefix + "/<device_name>/<action>")
def light(device_name, action):
    tuya_api.ensure_token()

    if request.headers.get("X-Key") != API_KEY:
        abort(403)

    dev = tuya_api.device(device_name)

    return process_device(dev, action)


@app.post(prefix + "/<device_name>/<int:n>/<action>")
def light_n(device_name, n, action):
    tuya_api.ensure_token()

    if request.headers.get("X-Key") != API_KEY:
        abort(403)

    dev = tuya_api.device(device_name)
    if dev is None:
        process_device(None, "")

    if not hasattr(dev, "subdevice"):
        abort(400, "not supported on this device")
    dev = dev.subdevice(n)

    return process_device(dev, action)


def process_device(dev, action):
    if dev is None:
        abort(404)

    try:
        if dev.has_action(action):
            state = dev.action(action)
        else:
            abort(400, f"no such action, supported: {', '.join(dev.get_actions())}")
    except Exception as e:
        app.logger.exception("cmd failed")
        return jsonify(error=str(e)), 502
    return jsonify(ok=True, state=state)

if __name__ == '__main__':
    app.run('0.0.0.0', 8044)