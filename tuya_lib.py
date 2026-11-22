import abc
import hashlib
import hmac
import json
import time

import requests

_EMPTY_SHA = hashlib.sha256(b"").hexdigest()


class Device(abc.ABC):
    def action(self, act):
        if not self.has_action(act):
            raise ValueError("invalid action")

        return getattr(self, act)()

    @abc.abstractmethod
    def get_actions(self):
        pass

    def has_action(self, act):
        return act in self.get_actions()


class TuyaApi:
    def __init__(self, tuya_id, secret):
        self.base_url = "https://openapi.tuyaeu.com"
        self.tuya_id = tuya_id
        self.secret = secret
        self._tok = {"value": "", "exp": 0.0}
        self._devices: dict[str, Device] = {}
        self.http = requests.Session()

    def _sign(self, msg, token, t):
        s = self.tuya_id + token + str(t) + msg
        return hmac.new(self.secret.encode(), s.encode(), hashlib.sha256).hexdigest().upper()

    def _call(self, method, path, params=None, body=None, auth=True):
        if auth:
            self.ensure_token()
        params = params or {}
        qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
        url = path + ("?" + qs if qs else "")
        raw = json.dumps(body).encode() if body is not None else b""
        sha = hashlib.sha256(raw).hexdigest() if raw else _EMPTY_SHA
        sts = f"{method}\n{sha}\n\n{url}"
        t = int(time.time() * 1000)
        tok = self._tok["value"] if auth else ""

        headers = {
            "client_id": self.tuya_id,
            "sign": self._sign(sts, tok, t),
            "t": str(t),
            "sign_method": "HMAC-SHA256",
        }
        if auth:
            headers["access_token"] = tok
        if raw:
            headers["Content-Type"] = "application/json"

        r = self.http.request(method, self.base_url + url, headers=headers, data=raw or None, timeout=8)
        return r.json()

    def ensure_token(self):
        if self._tok["value"] and time.time() < self._tok["exp"]:
            return
        r = self._call("GET", "/v1.0/token", {"grant_type": 1}, auth=False)
        if not r.get("success"):
            raise RuntimeError(f"token error: {r}")
        self._tok["value"] = r["result"]["access_token"]
        self._tok["exp"] = time.time() + r["result"]["expire_time"] - 300

    def send(self, device_id, commands):
        # commands: [{code: ..., value: ...}]

        r = self._call("POST", f"/v1.0/devices/{device_id}/commands",
                       body={"commands": commands})
        if not r.get("success"):
            raise RuntimeError(r)

    def read_state(self, device_id, code=None):
        r = self._call("GET", f"/v1.0/devices/{device_id}/status")
        res = {}
        for item in r.get("result", []):
            res[item["code"]] = item["value"]

        if code is None:
            return res
        return res.get(code)

    def device(self, device_name) -> Device:
        return self._devices.get(device_name)

    def add_device(self, device: Device, device_name):
        self._devices[device_name] = device

    def add_device_from_data(self, cls, data, device_name):
        dev = cls.from_data(data, self, device_name)
        self._devices[device_name] = dev

    @classmethod
    def from_data(cls, data):
        return cls(data.get("tuya-id"), data.get("tuya-secret"))


class SingleSwitch(Device):
    def __init__(self, tuya_api: TuyaApi, device_id: str, code="switch_1"):
        self.tuya_api = tuya_api
        self.device_id = device_id
        self.code = code

    def get_actions(self):
        return ["on", "off", "toggle"]

    def on(self):
        self.tuya_api.send(self.device_id, [{"code": self.code, "value": True}])
        return True

    def off(self):
        self.tuya_api.send(self.device_id, [{"code": self.code, "value": False}])
        return False

    def toggle(self):
        state = self.tuya_api.read_state(self.device_id, self.code)
        return self.off() if state else self.on()


class TwoSwitches(Device):
    def __init__(self, tuya_api: TuyaApi, device_id: str, codes=("switch_1", "switch_2")):
        self.tuya_api = tuya_api
        self.device_id = device_id
        self.codes = codes
        self.singles = [SingleSwitch(tuya_api, device_id, code) for code in self.codes]

    def get_actions(self):
        return ["on", "off", "toggle", "normal_toggle"]

    def on(self):
        self.tuya_api.send(self.device_id, [{"code": c, "value": True} for c in self.codes])
        return True

    def off(self):
        self.tuya_api.send(self.device_id, [{"code": c, "value": False} for c in self.codes])
        return False

    def normal_toggle(self):
        state = self.tuya_api.read_state(self.device_id)
        res = [state[self.codes[i]] for i in range(len(self.codes))]
        if any(res):
            return self.off()
        return self.on()

    def toggle(self):
        state = self.tuya_api.read_state(self.device_id)
        res = [state[self.codes[i]] for i in range(len(self.codes))]
        cmd = [{"code": c, "value": not v} for c, v in state.items()]
        self.tuya_api.send(self.device_id, cmd)
        return not all(res)

    @classmethod
    def from_data(cls, data, tuya_api: TuyaApi, device_name):
        return cls(tuya_api, data["devices"].get(device_name)["id"])

    def subdevice(self, device: int):
        return self.singles[device - 1]
