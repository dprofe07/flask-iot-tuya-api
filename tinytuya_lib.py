import abc
import hashlib
import hmac
import json
import threading
import time
import tinytuya as tt


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

    def _send(self, data):
        self._tt_device.send(self._tt_device.generate_payload(tt.CT.CONTROL, data))


class TuyaApi:
    def __init__(self, gate_id, gate_addr, local_key):
        self._gate = tt.Device(gate_id, gate_addr, local_key, version=3.4)
        self._gate.set_socketPersistent(True)

        self._devices: dict[str, Device] = {}

    def device(self, device_name) -> Device:
        return self._devices.get(device_name)

    def add_device(self, device: Device, device_name):
        self._devices[device_name] = device

    def ensure_token(self):
        pass

    def add_device_from_data(self, cls, data, device_name):
        dev_data = data["devices"][device_name]
        tt_dev = tt.OutletDevice(
            dev_data["id"],
            parent=self._gate,
            cid=dev_data["key"],
        )
        dev = cls.from_data(tt_dev, self)
        self._devices[device_name] = dev

    @classmethod
    def from_data(cls, data):
        g_data = data["devices"]["gate"]
        return cls(g_data["id"], g_data["addr"], g_data["key"])

    def start_token_updation_mainloop(self):
        pass


class SingleSwitch(Device):
    def __init__(self, tuya_api: TuyaApi, dev: tt.OutletDevice, code=1):
        self.tuya_api = tuya_api
        self._tt_device = dev
        self.code = code
        self.s_code = str(code)

    def get_actions(self):
        return ["on", "off", "toggle"]

    def on(self):
        self._send({self.code:True})
        return True

    def off(self):
        self._send({self.code:False})
        return False

    def toggle(self):
        state = self._tt_device.status().get('dps')
        if state is None:
            return None
        state = state.get(self.s_code)
        if state is None:
            return None
        return self.off() if state else self.on()


class MultipleSwitches(Device):
    def __init__(self, tuya_api: TuyaApi, tt_dev, codes=(1, 2)):
        self.tuya_api = tuya_api
        self._tt_device : tt.OutletDevice = tt_dev
        self.codes = codes
        self.s_codes = [str(i) for i in codes]
        self.singles = [SingleSwitch(tuya_api, tt_dev, code) for code in self.codes]

    def get_actions(self):
        return ["on", "off", "toggle", "normal_toggle"]

    def on(self):
        self._send({i: True for i in self.codes})
        return True

    def off(self):
        self._send({i: False for i in self.codes})
        return False

    def normal_toggle(self):
        state = self._tt_device.status().get('dps')
        if state is None:
            return None
        res = [state[self.s_codes[i]] for i in range(len(self.codes))]
        if any(res):
            return self.off()
        return self.on()

    def toggle(self):
        state = self._tt_device.status().get('dps')
        if state is None:
            return None
        res = [state[self.s_codes[i]] for i in range(len(self.codes))]
        self._send({c: not v for c, v in state.items()})
        return not all(res)

    @classmethod
    def from_data(cls, dev, tuya_api: TuyaApi):
        return cls(tuya_api, dev)

    def subdevice(self, device: int):
        return self.singles[device - 1]
