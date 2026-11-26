from __future__ import annotations

import abc
import logging
import queue
import select
import threading
import time
from typing import Any

import tinytuya as tt

log = logging.getLogger("tuya")


class TuyaError(RuntimeError):
    pass


def _unwrap(resp):
    """tinytuya возвращает ошибки словарём, а не исключением."""
    if isinstance(resp, dict) and ("Error" in resp or "Err" in resp):
        raise TuyaError(f"Err {resp.get('Err')}: {resp.get('Error')}")
    return resp


class _Gateway:
    """Единственный владелец сокета шлюза. Всё I/O — только в этом потоке."""

    def __init__(self, dev_id, address, local_key, version=3.4,
                 poll_interval=20.0, io_timeout=3.0, heartbeat_every=10.0):
        self.version = version
        self.dev = tt.Device(dev_id, address, local_key, version=version)
        self.dev.set_socketPersistent(True)
        self.dev.set_socketTimeout(io_timeout)
        self.dev.set_socketRetryLimit(2)
        self.dev.set_socketRetryDelay(1)
        self.dev.set_sendRetryLimit(0)  # не дублировать команду при потере ответа

        self._poll_interval = poll_interval
        self._heartbeat_every = heartbeat_every

        self._q: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._shadow: dict[str, dict[str, Any]] = {}
        self._children: dict[str, Any] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="tuya-gw", daemon=True)

    # ---------- состояние ----------

    def register(self, cid: str, codes: list[str], tt_dev):
        with self._lock:
            self._shadow.setdefault(cid, {})
            self._children[cid] = tt_dev

    def merge(self, cid: str, dps: dict):
        if not dps:
            return
        with self._lock:
            self._shadow.setdefault(cid, {}).update(
                {str(k): v for k, v in dps.items()}
            )

    def shadow(self, cid: str, codes: list[str]) -> dict:
        with self._lock:
            d = self._shadow.get(cid, {})
            return {c: d.get(c) for c in codes}

    # ---------- очередь команд ----------

    def submit(self, fn, *args, timeout=15.0, **kwargs):
        if threading.current_thread() is self._thread:
            return fn(*args, **kwargs)
        done, box = threading.Event(), []
        self._q.put((fn, args, kwargs, done, box))
        if not done.wait(timeout):
            raise TuyaError("таймаут ожидания I/O шлюза")
        if isinstance(box[0], Exception):
            raise box[0]
        return box[0]

    def start(self):
        self._thread.start()

    def close(self):
        self._stop.set()
        self._thread.join(timeout=5)
        try:
            self.dev.close()
        except Exception:
            pass

    # ---------- внутренний цикл ----------

    def _readable(self) -> bool:
        sock = getattr(self.dev, "sock", None)  # внутренний атрибут tinytuya
        if sock is None:
            return False
        try:
            return bool(select.select([sock], [], [], 0)[0])
        except (OSError, ValueError):
            return False

    def _ingest(self, payload):
        if isinstance(payload, dict) and payload.get("dps"):
            self.merge(payload.get("cid") or "", payload["dps"])

    def _drain(self, limit=8):
        for _ in range(limit):
            if not self._readable():
                return
            try:
                payload = self.dev.receive()
            except Exception as e:
                log.debug("receive: %s", e)
                return
            if not payload:
                return
            self._ingest(payload)

    def _poll_children(self):
        with self._lock:
            children = list(self._children.items())
        for cid, child in children:
            try:
                resp = _unwrap(child.status())
                if resp:
                    self.merge(cid, resp.get("dps") or {})
            except Exception as e:
                log.warning("poll %s: %s", cid, e)
            self._drain()

    def _run(self):
        last_hb = last_poll = 0.0
        try:
            self.dev.status(nowait=True)  # прайминг соединения
        except Exception as e:
            log.debug("prime: %s", e)

        while not self._stop.is_set():
            try:
                fn, args, kwargs, done, box = self._q.get(timeout=0.25)
            except queue.Empty:
                fn = None

            if fn is not None:
                try:
                    box.append(fn(*args, **kwargs))
                except Exception as e:
                    box.append(e)
                finally:
                    done.set()
                self._drain()
                continue

            self._drain()
            now = time.monotonic()
            if now - last_hb >= self._heartbeat_every:
                last_hb = now
                try:
                    self.dev.heartbeat()
                except Exception as e:
                    log.debug("heartbeat: %s", e)
            if now - last_poll >= self._poll_interval:
                last_poll = now
                self._poll_children()


class Device(abc.ABC):
    def __init__(self, api: "TuyaApi", tt_dev, codes):
        self.tuya_api = api
        self._gw = api.gateway
        self._tt_device = tt_dev
        self.codes = tuple(int(c) for c in codes)
        self.s_codes = [str(c) for c in self.codes]
        self._cid = getattr(tt_dev, "cid", None) or ""

        tt_dev.set_version(self._gw.version)
        tt_dev.set_dpsUsed({c: None for c in self.s_codes})
        self._gw.register(self._cid, self.s_codes, tt_dev)

    # ---------- диспетчер действий ----------

    @abc.abstractmethod
    def get_actions(self) -> list[str]:
        ...

    def has_action(self, act) -> bool:
        return act in self.get_actions()

    def action(self, act):
        if not self.has_action(act):
            raise ValueError(f"invalid action: {act}")
        return getattr(self, act)()

    # ---------- ввод-вывод ----------

    def _write(self, values: dict) -> dict:
        payload = {str(k): bool(v) for k, v in values.items()}
        if len(payload) == 1:
            (k, v), = payload.items()
            _unwrap(self._gw.submit(self._tt_device.set_value, k, v))
        else:
            _unwrap(self._gw.submit(self._tt_device.set_multiple_values, payload))
        self._gw.merge(self._cid, payload)  # write-through, не ждём репорта
        return payload

    def get_state(self) -> dict:
        """Мгновенно, из кэша. Всегда согласован с последней командой."""
        return self._gw.shadow(self._cid, self.s_codes)

    def refresh(self) -> dict:
        """Принудительный опрос устройства (для отладки и сверки)."""
        resp = _unwrap(self._gw.submit(self._tt_device.status))
        if resp:
            self._gw.merge(self._cid, resp.get("dps") or {})
        return self.get_state()

    def _ensure_state(self) -> dict:
        st = self.get_state()
        if any(v is None for v in st.values()):
            st = self.refresh()
        if any(v is None for v in st.values()):
            raise TuyaError(f"состояние неизвестно: {st}")
        return st

    def wait_for(self, expect: dict, timeout=5.0, interval=0.3) -> dict:
        """Ждём, пока устройство само подтвердит состояние. Кэш, не опрос."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            st = self.get_state()
            if all(st.get(str(k)) == v for k, v in expect.items()):
                return st
            time.sleep(interval)
        return self.get_state()


class SingleSwitch(Device):
    def __init__(self, api: "TuyaApi", tt_dev, code=1):
        super().__init__(api, tt_dev, [code])
        self.code = int(code)
        self.s_code = str(code)

    def get_actions(self):
        return ["on", "off", "toggle"]

    def on(self):
        self._write({self.s_code: True})
        return True

    def off(self):
        self._write({self.s_code: False})
        return False

    def toggle(self):
        cur = self._ensure_state()[self.s_code]
        return self.off() if cur else self.on()

    @classmethod
    def from_data(cls, tt_dev, api: "TuyaApi"):
        return cls(api, tt_dev)


class MultipleSwitches(Device):
    def __init__(self, api: "TuyaApi", tt_dev, codes=(1, 2)):
        super().__init__(api, tt_dev, codes)
        self.singles = {c: SingleSwitch(api, tt_dev, c) for c in self.codes}

    def get_actions(self):
        return ["on", "off", "toggle", "normal_toggle"]

    def on(self):
        self._write({c: True for c in self.s_codes})
        return True

    def off(self):
        self._write({c: False for c in self.s_codes})
        return False

    def normal_toggle(self):
        st = self._ensure_state()
        return self.off() if any(st[c] for c in self.s_codes) else self.on()

    def toggle(self):
        """Инвертируем каждый канал независимо (только свои DP)."""
        st = self._ensure_state()
        target = {c: not bool(st[c]) for c in self.s_codes}
        self._write(target)
        return target

    def subdevice(self, code: int) -> SingleSwitch:
        try:
            return self.singles[int(code)]
        except KeyError:
            raise KeyError(f"нет канала {code}, есть {list(self.singles)}") from None

    @classmethod
    def from_data(cls, tt_dev, api: "TuyaApi"):
        return cls(api, tt_dev)


class TuyaApi:
    def __init__(self, gate_id, gate_addr, local_key, version=3.4, poll_interval=20.0):
        self._gateway = _Gateway(gate_id, gate_addr, local_key,
                                 version=version, poll_interval=poll_interval)
        self._devices: dict[str, Device] = {}
        self._gateway.start()

    @property
    def gateway(self) -> _Gateway:
        return self._gateway

    def device(self, name: str) -> Device | None:
        return self._devices.get(name)

    def add_device(self, device: Device, name: str):
        self._devices[name] = device

    def add_device_from_data(self, cls, data, name: str):
        d = data["devices"][name]
        cid = d.get("cid") or d.get("node_id")
        if not cid:
            cid = d.get("key")
            log.warning("%s: cid/node_id не задан, использую поле 'key'. "
                        "Для Zigbee-подустройства там должен быть node_id, "
                        "а не local_key — local_key берётся у шлюза.", name)
        tt_dev = tt.OutletDevice(d["id"], parent=self._gateway.dev, cid=cid)
        dev = cls.from_data(tt_dev, self)
        self._devices[name] = dev
        return dev

    @classmethod
    def from_data(cls, data):
        g = data["devices"]["gate"]
        return cls(g["id"], g["addr"], g["key"],
                   version=float(g.get("version", 3.4)),
                   poll_interval=float(g.get("poll_interval", 20.0)))

    def close(self):
        self._gateway.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
