#!/usr/bin/env python3
"""
Erik Tune: en egen "Ngenic Tune-ersättare" i EN Python-fil.

- Prenumererar på MQTT (konfigurerbart per roll, via webgränssnittet):
  ute-temp, T1/T2, fjv-flöde, fjv-effekt, tappvattenflöde (m3/h), ev. inomhustemp
- Pollar väder-API (Open-Meteo, ingen nyckel) med prognos; räknar ut
  vindavkylning (EC-formeln; Open-Meteos vind har defaultenhet km/h - INGEN x3,6!)
- Räknar ut "Inject": den fejkade utetemperatur som skickas till reglercentralen
  (skall drivas av ESP32+digipot, se separat projekt), samt "Influence":
  Inject minus verklig utetemp (vår "Påverkan").
- Publicerar Inject + Influence + väder till MQTT med HA auto-discovery.
- Loggar allt till SQLite (/data/samples.db) - underlaget till marsregressionen.
- Webgränssnitt (svenska) port 8080: dashboard, MQTT-koppling, topics, tuning.

MQTT-kontrakt:
  erik-tune/inject/temp_c     retain, QoS1   fejkad utetemp (ESP32/digipoten prenumerar)
  erik-tune/influence/c       retain, QoS1   Inject - verklig utetemp
  erik-tune/mode              retain         shadow | active
  erik-tune/heartbeat         retain + LWT   "online"/"offline"
  erik-tune/koldfaktor/c      retain         köldfaktorn just nu (beräknas: ingen API har den)
  erik-tune/forecast/koldfaktor_c retain     köldfaktorn på prognoshorisonten
  erik-tune/weather/now       retain, JSON   aktuellt väder + koldfaktorserien + prognos

Säkerhet: "shadow" är default. Mode "active" är bara en markering för
ESP32-sidan; själva watchdog-logiken (heartbeat-timeout -> digipot till fast
0C-värde) sitter i ESP32:n - se projektanteckningarna.

Kör: all konfiguration via /setup i webgränssnittet; sparas i /data/config.json.

v0.18 (efter extern kodgranskning 24/11 2026):
- TECKENKORRIGERING i regulatorn: positiv "corr" betyder numera ÖKA
  värmebehovet, dvs Inject MINSKAS med corr i control_loop (tidigare
  adderades den, vilket gav positiv återkoppling:
  för kallt hus -> fejk varmare -> ännu kallare).
- Regulatorns fältfilter (30-min-medel) byggs på RÅ rumstemp (indoor_temp_c)
  ur databasen - inte det redan filtrerade ar_c-värdet (rekursivt filter borta).
- influence räknas som Inject - ute EFTER klippning (stämmer vid tak/golv).
- /setup: ändrade topic-kopplingar omprenumereras direkt (inte vid omstart).
- Tidszon: prognosindex väljs med ZoneInfo(config-tz), inte containerns TZ;
  ort-sökningen sparar även tidszonen som hör till valt ställe.
- save_config körs bara när regulatorn faktiskt lärt något (dirty-flagga),
  inte var 60:e minut (SD-kort på Pi).
- DHW-gate (beslut 24/11): balansinlärning och effekt-innerslingan hoppas
  över vid tappstopp (effekt > power_skip_kw ELLER flöde > flow_skip_lh).
- SQLite: trådlås kring all access + WAL-journal (läsare i web-trådar).
- Enkel rensning: samples.db gallras dagligen (logging.retention_days).
- Webb: jinja2 autoescape=True; betrodd intern HTML wrappas i Markup().
- compute_inject/auto_regulate är rena funktioner (testdata som arg) =>
  testbar utan MQTT/FastAPI (se tests/test_regulator.py).
"""

import html
import json
import os
import sqlite3
import threading
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn
from jinja2 import Environment
from markupsafe import Markup

# --------------------------------------------------------------------------
# Konfiguration och beständighet
# --------------------------------------------------------------------------

DATA = Path(os.environ.get("ERIK_DATA", "/data"))
CONFIG_PATH = DATA / "config.json"
DB_PATH = DATA / "samples.db"
HEARTBEAT_FILE = DATA / "heartbeat"
DATA.mkdir(parents=True, exist_ok=True)

WEB_PORT = int(os.environ.get("ERIK_PORT", "8080"))
CYCLE_S = 60  # injektionsberäkning/publicering/logging

# ---- logg rakt till stdout: `docker logs erik-tune` -----------------------------
_log_states: dict = {}    # nyckel -> senaste meddelandet (endast ändringar loggas)
_log_first: set = set()  # roller som redan loggats första gången
_log_lock = threading.Lock()  # _log_states/_log_first nås från flera trådar


def log(msg: str) -> None:
    """Tidsstämplad rad till stdout (python -u, docker logs)."""
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def log_change(key: str, msg: str) -> None:
    """Logga bara om tillståndet ändrats sedan förra raden för nyckeln."""
    with _log_lock:
        if _log_states.get(key) != msg:
            _log_states[key] = msg
            log(msg)

ROLES = {
    "ute_temp_c":        "Utetemperatur (verklig; kan med fördel hämtas från väder-APIt om topic saknas)",
    "indoor_temp_c":     "Rumstemperatur (valfri, för framtida komfortreglering)",
    "t1_c":              "Framledningstemperatur T1",
    "t2_c":              "Returledningstemperatur T2",
    "fjv_flow_lh":       "Fjärrvärme-flöde (l/h)",
    "fjv_power_kw":      "Fjärrvärme-effekt (kW)",
    "tapwater_flow_m3h": "Tappvatten-flöde (m3/h)",
}

DEFAULT_CONFIG = {
    "mqtt": {"host": "", "port": 1883, "user": "", "pass": "",
             "client_id": "erik-tune", "explorer": "on"},
    "location": {"lat": 60.4858, "lon": 15.4342, "tz": "Europe/Stockholm",
                 "place": "Borlänge (default)"},
    "topics": {role: "" for role in ROLES},
    "injector": {
        "mode": "shadow",        # shadow = publicera men styrning är avstängd
        "offset_c": 0.0,         # konstant Påverkan (positiv = energibesparing)
        "pre_gain": 0.0,         # grader Inject per grads prognostiserad ändring
        "pre_hours": 6,          # prognoshorisont
        "koldfaktor_gain": 0.0,  # andel vindavkylning som fejkad kyla (0..1)
        "solar_gain": 0.0,       # grader fejkad värme per 100 W/m2
        "min_inject_c": -30.0, "max_inject_c": 30.0,
    },
    "weather": {"poll_interval_s": 1800},
    "logging": {"retention_days": 400},   # gallring av samples.db (0 = av)
    "publish": {"inject": "on", "discovery": "on"},
    "auto": {
        "enabled": "off",        # självlärande rumsreglering (är/bör)
        "setpoint_c": 21.0,      # Bör-värde: mål-rumstemperatur
        "k_p": 0.5,              # grader korrigering per grad fel (P-ledet)
        "learn_rate": 0.05,      # band-minnets steg per lärtillfälle (I-ledet)
        "learn_interval_s": 900, # lär max var 15 min: huset svarar i timmar
        "deadband_c": 0.3,       # fält inom dödbandet: inget nytt lärande
        "avg_window_s": 1800,    # fältfilter: medel av RÅ rumstemp senaste 30 min
        "band_size": 5,          # köldfaktorbandens bredd, °C
        "power_gain": 0.5,       # grader inject per kW effectgap (snabb innerslinga)
        "power_max_age_s": 900,  # effectvandet far vara max 15 min gammalt
        # DHW-gate (beslut 24/11): tappstopp >10 kW uppmätta, normal radiatorlast
        # p95 = 7,2 kW. Blir effekt ELLER flöde över trösklarna: hoppa över
        # balansinlärning och stäng effekt-korrigeringen (corr_p = 0) - annars
        # lär regulatorn in duschar som "balanseffekt" per köld-band och driver
        # inject fel vid ren radiatorlast.
        "power_skip_kw": 8.0,    # kW: över = tappstopp, effekt-regleringen pausas
        "flow_skip_lh": 300.0,   # l/h primärt flöde: över = dubbelkontroll tappstopp
        # Frön från årsdata (5 552 h dec25-nov26, Tin ~21,4 C): matad effekt
        # per köldfaktorband, justerat ~+2,5 C (kold är vindkylt vs ute),
        # inkl varmvatten ~1,2 kW som golv. EMA (0.9/0.1) rätar fröna mot
        # det verkliga värdet inom några dagar; band utan frö lär från nuet.
        "p_bal": {"25": 1.2, "20": 1.2, "15": 1.1, "10": 1.3, "5": 1.6,
                  "0": 2.2, "-5": 3.3, "-10": 4.6, "-15": 5.5, "-20": 5.9,
                  "-25": 7.0},   # band -> balanserad effekt (kW)
        "max_correction_c": 3.0,
        "sample_max_age_s": 1800,
        "bands": {},             # minnet: band -> inlärd korrigering (POSITIVT = mer värmebehov)
    },
}


def load_config() -> dict:
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text())
    else:
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    # fyll saknade nycklar (framåtkompatibelt)
    def merge(dst, src):
        for k, v in src.items():
            if k not in dst:
                dst[k] = json.loads(json.dumps(v))
            elif isinstance(v, dict) and isinstance(dst[k], dict):
                merge(dst[k], v)
    merge(cfg, DEFAULT_CONFIG)
    return cfg


def save_config(cfg) -> None:
    tmp = CONFIG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
    tmp.replace(CONFIG_PATH)


CONFIG = load_config()

# --------------------------------------------------------------------------
# Körningstillstånd
# --------------------------------------------------------------------------

_lock = threading.Lock()
_state = {
    "samples": {},        # roll -> {"ts", "value", "topic"}
    "weather": {},        # se poll_weather
    "mqtt_connected": False,
    "mqtt_error": "",
    "inject_c": None,
    "influence_c": None,
    "last_calc": 0,
    "last_weather_ok": 0,
    "last_learn": 0,      # regulatorn: senaste lärtillfället (bands-minnet)
    "last_reg": None,     # senaste reglersvar (dashboarden)
    "last_prune": 0,      # senaste gallringen av samples.db
    "boot_ts": time.time(),
}

# --------------------------------------------------------------------------
# SQLite: underlaget för marsregressionen I = c + d*T_ute
# --------------------------------------------------------------------------

# Anslutningen delas av flera trådar (control_loop skriver, webtrådar läser
# i dashboarden) => ett lås krävs: check_same_thread=False stänger bara av
# pysqlites trådkontroll, den skyddar inte mot samtidig access.
_db_lock = threading.Lock()
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
with _db_lock:
    _db.execute("CREATE TABLE IF NOT EXISTS samples ("
                " ts INTEGER NOT NULL, key TEXT NOT NULL, value REAL NOT NULL,"
                " PRIMARY KEY (ts, key))")
    _db.execute("CREATE INDEX IF NOT EXISTS idx_samples_key ON samples(key, ts)")
    try:
        _db.execute("PRAGMA journal_mode=WAL")  # låser mindre vid läs/skriv samtidigt
    except sqlite3.Error:
        pass   # journal-läget är en optimering, inte ett krav
    _db.commit()


def log_sample(key: str, value: float) -> None:
    with _db_lock:
        _db.execute("INSERT OR REPLACE INTO samples VALUES (?,?,?)",
                    (int(time.time()), key, float(value)))
        _db.commit()


def read_series(key: str, seconds: int):
    since = int(time.time() - seconds)
    with _db_lock:
        return _db.execute(
            "SELECT ts, value FROM samples WHERE key=? AND ts>=? ORDER BY ts",
            (key, since)).fetchall()


def prune_samples(retention_days: int) -> int:
    """Gallra gamla rader ur samples.db (körs ~dagligen från control_loop).
    ~10 rader/min utan rensning = flera miljoner rader per år."""
    if retention_days and int(retention_days) > 0:
        cutoff = int(time.time() - int(retention_days) * 86400)
        with _db_lock:
            cur = _db.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
            _db.commit()
            return cur.rowcount
    return 0


# --------------------------------------------------------------------------
# MQTT
# --------------------------------------------------------------------------

_topic_to_role: dict = {}    # base-topic -> lista av roller (en topic kan mata flera)
_role_json_key: dict = {}   # roll -> JSON-nyckel (t.ex. 'temp1'), "" = hela payload
_seen: dict = {}  # topic -> {"payload","ts","fields"}: topicutforskaren
_NOISE_KEYS = {"rssi", "linkquality", "lqi", "battery", "voltage",
               "last_seen", "elapsed", "strength"}  # husgerad, inte mätvärden


def record_seen(msg) -> None:
    """Minnestabell över allt som publicerats (topicutforskaren i webb-UI:t).
    JSON-objekt med numeriska värden bryts ut per nyckel i 'fields'.
    Eget/styrtopic (erik-tune/) loggas inte: det ar bokföring av egna
    utmatningar, inte ingangar."""
    if msg.topic.startswith("erik-tune/"):
        return
    try:
        payload = msg.payload.decode("utf-8", "replace")[:80]
    except Exception:
        payload = ""
    fields = {}
    try:
        obj = json.loads(msg.payload)
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (int, float)):
                    fields[k] = v
                elif isinstance(v, str):
                    try:
                        fields[k] = float(v)  # siffror som strängar
                    except ValueError:
                        pass
    except (TypeError, ValueError):
        pass
    with _lock:
        _seen[msg.topic] = {"payload": payload, "ts": time.time(),
                            "fields": fields}
        if len(_seen) > 3000:  # kapa: släpp de äldsta
            for t in sorted(_seen, key=lambda k: _seen[k]["ts"])[:1000]:
                del _seen[t]


def parse_value(payload: bytes, key: str = None):
    """Tal, tal-i-JSON eller strängsiffra -> float, annars None.
    key: hämta en viss nyckel ur ett JSON-objekt (t.ex. 'temp1')."""
    try:
        return float(payload)
    except (TypeError, ValueError):
        pass
    try:
        obj = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if isinstance(obj, dict):
        if key:
            v = obj.get(key)
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                try:
                    return float(v)   # "72.18" som sträng i JSON
                except ValueError:
                    return None
            return None
        for k in ("state", "value", "temperature"):
            if k in obj and isinstance(obj[k], (int, float)):
                return float(obj[k])
        for k, v in obj.items():
            if k not in _NOISE_KEYS and isinstance(v, (int, float)):
                return float(v)  # rätt mätvärde, inte rssi/battery/linkquality
    if isinstance(obj, (int, float)):
        return float(obj)
    return None


def discovery_cfg(unique_id, name, state_topic, uom, device_class=None):
    cfg = {
        "name": name, "unique_id": unique_id, "state_topic": state_topic,
        "unit_of_measurement": uom, "state_class": "measurement",
        "device": {"identifiers": ["erik-tune"], "name": "Erik Tune",
                   "model": "Egen Tune-kopia", "manufacturer": "Erik Olsson"},
    }
    if device_class:
        cfg["device_class"] = device_class
    return json.dumps(cfg)


def publish_discovery(client):
    if CONFIG["publish"].get("discovery") != "on":
        return
    ph = max(1, int(CONFIG["injector"].get("pre_hours", 6)))
    pre = "homeassistant/sensor"
    items = [
        ("erik_tune_inject_temp_c", "Erik Tune Injected Outdoor",
         "erik-tune/inject/temp_c", "°C", "temperature"),
        ("erik_tune_influence_c", "Erik Tune Influence",
         "erik-tune/influence/c", "°C", "temperature"),
        ("erik_tune_koldfaktor_c", "Erik Tune Köldfaktor",
         "erik-tune/koldfaktor/c", "°C", "temperature"),
        ("erik_tune_solar_wm2", "Erik Tune Solinstrålning",
         "erik-tune/solar/wm2", "W/m²", "irradiance"),
        ("erik_tune_forecast_solar_wm2",
         f"Erik Tune Solinstrålningprognos ({ph} h)",
         "erik-tune/forecast/solar_wm2", "W/m²", "irradiance"),
        ("erik_tune_forecast_temp_c",
         f"Erik Tune Prognostemp ({ph} h)",
         "erik-tune/forecast/temp_c", "°C", "temperature"),
        ("erik_tune_forecast_drop_c",
         f"Erik Tune Prognosändring ({ph} h)",
         "erik-tune/forecast/drop_c", "°C", None),
        ("erik_tune_forecast_koldfaktor_c",
         f"Erik Tune Köldfaktorprognos ({ph} h)",
         "erik-tune/forecast/koldfaktor_c", "°C", "temperature"),
        ("erik_tune_mode", "Erik Tune Mode",
         "erik-tune/mode", None, None),
    ]
    for uid, name, topic, uom, dc in items:
        cfg = json.loads(discovery_cfg(uid, name, topic, uom, dc))
        if uom is None:  # mode: bara en etikett, ingen enhet
            cfg.pop("unit_of_measurement")
            cfg.pop("state_class", None)
            cfg.pop("device_class", None)
        client.publish(f"{pre}/{uid}/config", json.dumps(cfg),
                       retain=True, qos=1)
    # Number-entitet: HA-slider som sätter offset (Påverkan) via MQTT
    num = {
        "name": "Erik Tune Påverkan", "unique_id": "erik_tune_offset",
        "state_topic": "erik-tune/offset/c",
        "command_topic": "erik-tune/cmd/offset",
        "min": -10.0, "max": 10.0, "step": 0.1, "mode": "slider",
        "unit_of_measurement": "°C",
        "device": {"identifiers": ["erik-tune"], "name": "Erik Tune",
                   "model": "Egen Tune-kopia", "manufacturer": "Erik Olsson"},
    }
    client.publish("homeassistant/number/erik_tune_offset/config",
                   json.dumps(num), retain=True, qos=1)
    # Number-entitet: Bör-värdet (mål-rumstemperatur), styrbar från HA
    num2 = dict(num)
    num2.update({"name": "Erik Tune Börvärde",
                 "unique_id": "erik_tune_borvarde",
                 "device_class": "temperature",
                 "min": 10.0, "max": 30.0, "step": 0.5,
                 "state_topic": "erik-tune/setpoint/c",
                 "command_topic": "erik-tune/cmd/setpoint"})
    client.publish("homeassistant/number/erik_tune_borvarde/config",
                   json.dumps(num2), retain=True, qos=1)


def handle_cmd(client, msg) -> None:
    """MQTT-kommandon (t.ex. från HA): cmd/offset <- tal, cmd/mode <- shadow|active."""
    payload = msg.payload.decode("utf-8", "replace").strip()
    sub = msg.topic[len("erik-tune/cmd/"):]
    changed = False
    if sub == "offset":
        try:
            CONFIG["injector"]["offset_c"] = round(float(payload), 2)
            changed = True
        except ValueError:
            pass
    elif sub == "mode" and payload in ("shadow", "active"):
        CONFIG["injector"]["mode"] = payload
        changed = True
    elif sub == "setpoint":
        try:
            v = round(float(payload), 1)
            if 5 <= v <= 30:
                CONFIG["auto"]["setpoint_c"] = v
                changed = True
        except ValueError:
            pass
    if changed:
        save_config(CONFIG)
        publish_state(client)
        log(f"MQTT-kommando: {sub} = {payload}")


def publish_state(client=None) -> None:
    """Offset + mode som retained state, så HA-slidern dashboarden hänger med."""
    if client is None:
        client = globals().get("client")
    if client is None:
        return
    client.publish("erik-tune/offset/c", CONFIG["injector"]["offset_c"],
                   retain=True, qos=1)
    client.publish("erik-tune/mode", CONFIG["injector"]["mode"],
                   retain=True, qos=1)
    client.publish("erik-tune/setpoint/c", CONFIG["auto"]["setpoint_c"],
                   retain=True, qos=1)


def on_connect(client, userdata, flags, reason_code, properties):
    if not getattr(reason_code, "is_failure", True):
        client.publish("erik-tune/heartbeat", "online", retain=True, qos=1)
        with _lock:
            _state["mqtt_connected"] = True
            _state["mqtt_error"] = ""
        client.subscribe("erik-tune/cmd/#", qos=1)
        if CONFIG["mqtt"].get("explorer", "on") == "on":
            client.subscribe("#", qos=0)  # topicutforskaren: se allt på busen
        for role, spec in CONFIG["topics"].items():
            if spec:
                base, _, key = spec.partition("|")  # topic|json-nyckel
                client.subscribe(base, qos=1)
                with _lock:
                    lst = _topic_to_role.setdefault(base, [])
                    if role not in lst:      # samma topic kan mata flera roller
                        lst.append(role)
                    _role_json_key[role] = key or ""
        publish_discovery(client)
        n = sum(1 for s in CONFIG["topics"].values() if s)
        log(f"MQTT ansluten {CONFIG['mqtt']['host']}:{CONFIG['mqtt']['port']}"
            f" ({n} rollkopplingar, utforskare "
            f"{'på' if CONFIG['mqtt'].get('explorer', 'on') == 'on' else 'av'})")
    else:
        with _lock:
            _state["mqtt_error"] = f"connect misslyckades: {reason_code}"
        log_change("conn", f"MQTT connect misslyckades: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    with _lock:
        _state["mqtt_connected"] = False
        _state["mqtt_error"] = f"frånkopplad: {reason_code}"
    log_change("disc", f"MQTT frånkopplad: {reason_code}")


def on_message(client, userdata, msg):
    if msg.topic.startswith("erik-tune/cmd/"):
        handle_cmd(client, msg)  # kommandon från HA (setpoint, mode)
        return
    record_seen(msg)          # minnestabellen: ALLA topics, aven rollkopplade
    with _lock:               # mqtt_manager kan tömma maparna parallellt
        roles = list(_topic_to_role.get(msg.topic) or [])
        jkeys = {r: _role_json_key.get(r) for r in roles}
    if not roles:
        return
    for role in roles:  # en topic kan mata flera roller (t1_c + t2_c)
        val = parse_value(msg.payload, jkeys.get(role) or None)
        if val is None:
            continue
        with _lock:
            _state["samples"][role] = {"ts": time.time(), "value": val,
                                       "topic": msg.topic}
            first = role not in _log_first
            _log_first.add(role)
        if first:
            log(f"roll {role} = {val} ({msg.topic})   [första värdet]")


def build_client():
    c = CONFIG["mqtt"]
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=c["client_id"])
    client.will_set("erik-tune/heartbeat", "offline", qos=1, retain=True)
    if c["user"]:
        client.username_pw_set(c["user"], c["pass"])
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    return client


client = build_client()
_loop_started = False
_mqtt_rebuild = threading.Event()


def mqtt_manager():
    """Håller kopplingen vid liv; bygger om klienten vid konfigurationsändring.
    connect_async + loop_start återansluter automatiskt vid driftstörningar."""
    global client, _loop_started
    while True:
        if _mqtt_rebuild.is_set():
            _mqtt_rebuild.clear()
            log("MQTT-klient byggs om (ny konfiguration)")
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass
            with _lock:
                _topic_to_role.clear()
                _role_json_key.clear()
            client = build_client()
            _loop_started = False
        host, port = CONFIG["mqtt"]["host"], int(CONFIG["mqtt"]["port"])
        if host and not _loop_started:
            try:
                client.connect_async(host, port, keepalive=60)
                client.loop_start()
                _loop_started = True
            except Exception as exc:
                with _lock:
                    _state["mqtt_error"] = f"connect-fel: {exc}"
                log_change("conn", f"MQTT connect-fel: {exc}")
        time.sleep(5)


# --------------------------------------------------------------------------
# Väder: Open-Meteo (ingen API-nyckel) + EC-köldfaktor
# --------------------------------------------------------------------------

def ec_koldfaktor(t_c, v_kmh):
    """Environment Canada-vindavkylning. OBS: vind i km/h."""
    if t_c is None:
        return None
    if t_c <= 10 and v_kmh is not None and v_kmh > 4.8:
        x = v_kmh ** 0.16
        return round(13.12 + 0.6215 * t_c - 11.37 * x + 0.3965 * t_c * x, 1)
    return t_c


OM_URL = ("https://api.open-meteo.com/v1/forecast"
          "?latitude={lat}&longitude={lon}"
          "&hourly=temperature_2m,wind_speed_10m,shortwave_radiation,cloud_cover"
          "&forecast_days=3&timezone={tz}")


def _now_label(tz_name: str) -> str:
    """'YYYY-MM-DDTHH:00' i konfigurerad tidszon (ZoneInfo ur standardbiblioteket).
    Oberoende av containerns TZ-miljövariabel: körs containern i UTC blir
    inte index längre fel mot Open-Meteos lokala tidsstämplar. Ogiltig
    tidszon faller tillbaka på UTC i stället för att krascha väderloopen."""
    try:
        now = datetime.now(ZoneInfo(tz_name or "Europe/Stockholm"))
    except Exception:
        now = datetime.now(ZoneInfo("UTC"))
    return now.strftime("%Y-%m-%dT%H:00")


def poll_weather():
    cfg = CONFIG["location"]
    url = OM_URL.format(lat=cfg["lat"], lon=cfg["lon"], tz=cfg["tz"])
    r = requests.get(url, timeout=40)
    r.raise_for_status()
    h = r.json()["hourly"]
    times, temps = h["time"], h["temperature_2m"]
    local_now = _now_label(cfg.get("tz"))   # Open-Meteos tider är i samma tz
    idx = 0
    for i, t in enumerate(times):
        if t <= local_now:
            idx = i
        else:
            break
    wind = h["wind_speed_10m"][idx]        # km/h (Open-Meteos defaultenhet)
    rad = h["shortwave_radiation"][idx]    # W/m2
    cc = h["cloud_cover"][idx]
    temp = temps[idx]
    kold = ec_koldfaktor(temp, wind)
    # Ingen API (SMHI eller Open-Meteo) levererar köldfaktor som prognos -
    # men vi har temperatur + vind per prognostimme, sa vi raknar ut serien sjalva:
    kold_series = [ec_koldfaktor(t, w)
                   for t, w in zip(temps, h["wind_speed_10m"])]
    horizon = max(1, int(CONFIG["injector"]["pre_hours"]))
    h_i = min(idx + horizon, len(temps) - 1)
    t_h = temps[h_i]
    drop = round(t_h - temp, 1)
    kold_h = kold_series[h_i]
    kold_drop = round(kold_h - kold, 1)
    rad_h = h["shortwave_radiation"][h_i]    # sol pa samma horisont

    with _lock:
        _state["last_weather_ok"] = time.time()
        _state["weather"] = {
            "ts": time.time(), "temp_c": temp, "wind_kmh": wind,
            "koldfaktor_c": kold, "solar_wm2": rad, "cloud_cover": cc,
            "forecast_temp_c": t_h, "forecast_drop_c": drop,
            "forecast_koldfaktor_c": kold_h, "forecast_solar_wm2": rad_h,
            "koldfaktor_drop_c": kold_drop,
            "series": list(zip(times[idx:idx + 13], temps[idx:idx + 13])),
            "koldfaktor_series": list(zip(times[idx:idx + 13],
                                          kold_series[idx:idx + 13])),
        }
        weather = dict(_state["weather"])

    client.publish("erik-tune/koldfaktor/c", kold, retain=True, qos=1)
    client.publish("erik-tune/forecast/koldfaktor_c", kold_h,
                   retain=True, qos=1)
    client.publish("erik-tune/solar/wm2", round(rad, 1), retain=True, qos=1)
    client.publish("erik-tune/forecast/solar_wm2", round(rad_h, 1),
                   retain=True, qos=1)
    client.publish("erik-tune/forecast/temp_c", t_h, retain=True, qos=1)
    client.publish("erik-tune/forecast/drop_c", drop, retain=True, qos=1)
    client.publish("erik-tune/weather/now",
                   json.dumps(weather, ensure_ascii=False), retain=True, qos=1)
    log(f"väder OK: {temp} C, köldfaktor {kold} C | prognos {horizon} h:"
        f" {t_h} C (sol {round(rad, 1)}/{round(rad_h, 1)} W/m2)")


def weather_loop():
    while True:
        try:
            poll_weather()
            _log_states.pop("weather_err", None)   # felet är läkt: logga nästa fel igen
        except Exception as exc:
            with _lock:
                if not _state["mqtt_error"]:
                    _state["mqtt_error"] = f"väderfel: {exc}"
            log_change("weather_err", f"väderfel: {exc}")
        time.sleep(int(CONFIG["weather"]["poll_interval_s"]))


# --------------------------------------------------------------------------
# Injektionshjärnan
# --------------------------------------------------------------------------

def get_sample(role):
    with _lock:
        s = _state["samples"].get(role)
    if s and time.time() - s["ts"] < 3600:
        return s["value"]
    return None


def current_ute():
    """Föredrar MQTT-ute; faller tillbaka på väder-APIts nu-prognos."""
    ute = get_sample("ute_temp_c")
    if ute is not None:
        return ute
    with _lock:
        w = _state["weather"]
    if w and time.time() - w["ts"] < 4000:
        return w["temp_c"]
    return None


def clip_inject(v, p):
    """Klipp till [min_inject_c, max_inject_c] med två decimaler."""
    return round(max(float(p["min_inject_c"]),
                     min(float(p["max_inject_c"]), v)), 2)


def compute_inject(ute, w=None, p=None):
    """
    REN FUNKTION (testbar utan MQTT/DB): Inject = ute + offset +
    koldfaktorterm + solterm + prognosterm, klippt.
      ute: verklig utetemp (None => None, None)
      w:   väder-dict (koldfaktor_c, solar_wm2, forecast_drop_c)
      p:   injector-config (offset_c, *_gain, min/max)
    Med default (alla gains = 0, offset = 0) blir Inject = ute, dvs
    Influence = 0: man ser tydligt vad man gör när man vrider på parametrarna.
    OBS tecknet: HÖGRE inject = fejkad VARMARE ute = MINDRE värme till huset.
    """
    if ute is None:
        return None, None
    w = w or {}
    if p is None:
        p = CONFIG["injector"]
    inject = float(p["offset_c"])

    kold = w.get("koldfaktor_c")
    if kold is not None and kold < ute:
        inject += float(p["koldfaktor_gain"]) * (kold - ute)  # kyliga halvan

    rad = w.get("solar_wm2") or 0.0
    inject += float(p["solar_gain"]) * rad / 100.0

    drop = w.get("forecast_drop_c")
    if drop is not None:
        inject += float(p["pre_gain"]) * drop  # kallare fram = mer värme nu

    inject += ute
    inject = clip_inject(inject, p)
    return inject, round(inject - ute, 2)


def auto_regulate(a, indoor, ar_hist, kold, pw, flow_lh, now, last_learn):
    """
    REN FUNKTION (testbar utan MQTT/DB): självlärande rumsreglering (är/bör)
    med P-led på felet + lärt bias per köldfaktorband + snabb effekt-innerslinga.

      a:          auto-config-dict; MUTERAS (bands/p_bal består mellan anrop)
      indoor:     (ts, value) färskt rumsvärde, eller None
      ar_hist:    [(ts, value)] RÅ rumstemp-serie inom avg_window_s
      kold:       köldfaktor (fallboll prognostemp): bandindex
      pw, flow_lh: fjv-effekt kW / primärflöde l/h; kan vara None. Anroparen
                  skickar bara FÄRSKA värden (< power_max_age_s).
      now:        epoch-sekunder; last_learn: epoch för senaste lärtillfälle
    Return: (reg, dirty). reg=None => reglering av / inga färska data.
    dirty=True <=> bands/p_bal uppdaterats (då sparar control_loop config).

    TECKENKONVENTION (v0.18): POSITIV corr = ÖKA värmebehovet = MER värme.
    control_loop applicerar den som Inject MINUS corr, eftersom HÖGRE inject
    = fejkad varmare ute = MINDRE värme till huset (värmekurvan faller).
    """
    if a.get("enabled") != "on":
        return None, False
    if not indoor or now - indoor[0] > int(a["sample_max_age_s"]):
        return None, False
    if kold is None:
        return None, False

    # Trögt system (tidskonstant ca 48 h): fältfilter = 30-min-medel av RÅ
    # rumstemp (indoor_temp_c), inte momentanvärden (dörr, spis, sol...) och
    # INTE det redan filtrerade ar_c-värdet (v0.17:s bugg: rekursivt filter
    # med mångdubbelt längre eftersläpning). Serien måste täcka minst halva
    # fönstret (glesa zigbee-givare, ~12 min, är OK), annars momentanvärdet.
    win = int(a["avg_window_s"])
    hist = [(ts, v) for ts, v in (ar_hist or []) if now - ts <= win]
    if len(hist) >= 2 and now - hist[0][0] >= win / 2:
        ar = sum(v for _, v in hist) / len(hist)
    else:
        ar = indoor[1]       # uppstart: ingen täckande historik än

    bor = float(a["setpoint_c"])
    err = bor - ar          # positivt = för kallt = behöver MER värme
    band = int(kold // int(a["band_size"])) * int(a["band_size"])
    key = str(band)
    bias = float(a["bands"].get(key, 0.0))
    mx = float(a["max_correction_c"])
    dirty = False
    hold = abs(err) <= float(a["deadband_c"])

    # DHW-gate: tappstopp ger effekttoppar 17-33 kW/660-1020 l/h (nov-data)
    # medan normal radiatorlast ligger under ~7,2 kW. Blir effekt ELLER flöde
    # över tröskeln: hoppa över balansinlärning OCH sätt corr_p=0, annars
    # lär regulatorn in duschar som balanseffekt per köld-band.
    pw_tap = ((pw is not None and pw > float(a.get("power_skip_kw", 8.0)))
              or (flow_lh is not None
                  and flow_lh > float(a.get("flow_skip_lh", 300.0))))

    # Innerslinga: effekt (kW) är snabbare än rumstempen (minuter mot
    # timmar). När huset ligger rätt lär vi bandets balanseffekt
    # (EMA 0.9/0.1); när det avviker driver effektgapet regleringen direkt.
    corr_p = 0.0
    if pw is not None and not pw_tap and float(a["power_gain"]) > 0:
        if hold:
            if key not in a["p_bal"]:      # nytt band i frötabellen = spara
                dirty = True
            old = float(a["p_bal"].get(key, pw))
            new = round(old * 0.9 + pw * 0.1, 3)   # EMA mot verkligheten
            a["p_bal"][key] = new
            if abs(new - old) >= 0.05:   # bara märkbar ändring triggar sparning
                dirty = True
        elif key in a["p_bal"]:
            corr_p = max(-mx, min(mx,
                float(a["power_gain"]) * (float(a["p_bal"][key]) - pw)))

    if hold:
        return {"err": 0.0, "corr": round(bias, 2), "band": band,
                "ar_c": round(ar, 2), "bor_c": bor, "bias": round(bias, 3),
                "new_bias": round(bias, 3), "learned": False,
                "learn_ts": last_learn, "hold": True, "pw": pw,
                "flow_lh": flow_lh, "pw_tap": pw_tap,
                "corr_p": 0.0}, dirty    # dödband: håll minnet
    corr = max(-mx, min(mx, bias + float(a["k_p"]) * err + corr_p))
    new_bias = bias
    learned = False
    if now - last_learn >= int(a["learn_interval_s"]):
        new_bias = max(-mx, min(mx, bias + float(a["learn_rate"]) * err))
        a["bands"][key] = round(new_bias, 3)
        learned = True
        dirty = True
        last_learn = now      # lärtakt: max var 15 min (huset svarar i timmar)
    return {"err": round(err, 2), "corr": round(corr, 2), "band": band,
            "ar_c": round(ar, 2), "bor_c": bor, "bias": round(bias, 3),
            "new_bias": round(new_bias, 3), "learned": learned,
            "learn_ts": last_learn, "pw": pw, "flow_lh": flow_lh,
            "pw_tap": pw_tap, "corr_p": round(corr_p, 2)}, dirty



def apply_regulation(inject_c, reg, ute, p):
    """KORRIGERINGENS TECKEN (v0.18): positiv corr = öka värmebehovet = MER
    värme till huset. Reglercentralens kurva är FALLANDE, så mer värme fås
    genom LÄGRE inject: därför subtraheras corr (v0.17 adderade, vilket gav
    positiv återkoppling). Influence räknas EFTER klippningen så att
    "Influence = Inject - verklig ute" håller även vid tak/golv."""
    inject_c = clip_inject(inject_c - reg["corr"], p)
    return inject_c, round(inject_c - ute, 2)


def control_loop():
    while True:
        try:
            with _lock:
                w = dict(_state["weather"] or {})
                samples = dict(_state["samples"])   # skalet kopieras; posterna ersätts aldrig
                last_learn = _state["last_learn"]
                last_prune = _state["last_prune"]

            # gallra samples.db ~en gång per dygn (logging.retention_days)
            if time.time() - last_prune >= 86400:
                removed = prune_samples(int(CONFIG["logging"].get("retention_days", 400)))
                with _lock:
                    _state["last_prune"] = time.time()
                if removed:
                    log(f"samples.db: gallrade {removed} rader (äldre än "
                        f"{CONFIG['logging'].get('retention_days')} dagar)")

            ute_now = current_ute()
            p = CONFIG["injector"]
            inject_c, influence_c = compute_inject(ute_now, w, p)

            # Regulatorn: ren funktion, all historik skickas som argument.
            a = CONFIG["auto"]
            indoor = samples.get("indoor_temp_c")
            pwrec = samples.get("fjv_power_kw")
            flrec = samples.get("fjv_flow_lh")
            pw = (pwrec["value"] if pwrec and
                  time.time() - pwrec["ts"] <= int(a["power_max_age_s"]) else None)
            flow_lh = (flrec["value"] if flrec and
                       time.time() - flrec["ts"] <= int(a["power_max_age_s"])
                       else None)
            kold = w.get("koldfaktor_c")
            if kold is None:
                kold = w.get("temp_c")   # fallboll: prognostemp som bandindex
            # RÅ rumstemp ur databasen (inte ar_c: rekursivt filter i v0.17)
            ar_hist = read_series("indoor_temp_c", int(a["avg_window_s"]))
            reg, dirty = auto_regulate(a, indoor, ar_hist, kold, pw, flow_lh,
                                       time.time(), last_learn)
            if reg is not None:
                with _lock:
                    _state["last_learn"] = reg["learn_ts"]
                if reg["learned"]:
                    log(f"regulatorn lär: band {reg['band']}, "
                        f"fel {reg['err']:+.2f} C"
                        f" -> bias {reg['new_bias']:+.2f} C"
                        + (f", effekt {reg['pw']} kW"
                           if reg["pw"] is not None else ""))
                if reg["pw_tap"]:
                    log_change("tap",
                               f"tappstopp: effekt {reg['pw']} kW / "
                               f"flöde {reg['flow_lh']} l/h - "
                               "balansinlärning och effekt-korrigeringen pausad")
                else:
                    with _log_lock:
                        _log_states.pop("tap", None)   # tappstopps-läget läkt

            if inject_c is not None and reg is not None:
                inject_c, influence_c = apply_regulation(
                    inject_c, reg, ute_now, p)
                client.publish("erik-tune/room/ar_c", reg["ar_c"],
                               retain=True, qos=1)
                client.publish("erik-tune/room/bor_c", reg["bor_c"],
                               retain=True, qos=1)
                log_sample("ar_c", reg["ar_c"])   # display/marsdata; regulatorn
                log_sample("err_c", reg["err"])     # läser RÅ indoor_temp_c
                if dirty:   # bands/p_bal uppdaterat: minnet består omstarten
                    save_config(CONFIG)
                with _lock:
                    _state["last_reg"] = reg
            if inject_c is not None:
                with _lock:
                    _state["inject_c"] = inject_c
                    _state["influence_c"] = influence_c
                    _state["last_calc"] = time.time()
                if CONFIG["publish"].get("inject") == "on":
                    client.publish("erik-tune/inject/temp_c", inject_c,
                                   retain=True, qos=1)
                    client.publish("erik-tune/influence/c", influence_c,
                                   retain=True, qos=1)
                client.publish("erik-tune/mode", CONFIG["injector"]["mode"],
                               retain=True, qos=1)
                client.publish("erik-tune/offset/c",
                               CONFIG["injector"]["offset_c"],
                               retain=True, qos=1)
                log_sample("inject_c", inject_c)
                log_sample("influence_c", influence_c)
            for role in ROLES:
                val = get_sample(role)
                if val is not None:
                    log_sample(role, val)
            HEARTBEAT_FILE.write_text(str(int(time.time())))
        except Exception:
            import traceback
            traceback.print_exc()
        time.sleep(CYCLE_S)


# ---------------------------------------------------------------------------
# Webgränssnitt (svenska, LAN-internt; ingen autentisering: aldrig portforw.)
# ---------------------------------------------------------------------------

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

CSS = """
body{font-family:system-ui,sans-serif;background:#12161c;color:#e7ecf2;max-width:1100px;margin:0 auto;padding:24px;color-scheme:dark}
h1{font-size:1.5em}h2{font-size:1.1em;color:#9fb4cc;margin:24px 0 8px}
.card{background:#1b2530;border-radius:10px;padding:14px 18px;margin:8px 0;display:flex;justify-content:space-between;align-items:baseline;gap:12px}
.big{font-size:1.6em;font-weight:600}
header a,a{color:#8ab4e8;text-decoration:none}
header a{margin-right:14px}
form{display:inline}
label{display:block;margin:8px 0 2px;font-size:.9em;color:#9fb4cc}
input,select{background:#243242;color:#e7ecf2;border:1px solid #3a4c60;border-radius:6px;padding:6px 8px;width:100%;box-sizing:border-box}
table{border-collapse:collapse;width:100%}td,th{padding:6px 10px;border-bottom:1px solid #2a3a4c;text-align:left;vertical-align:top}
button{background:#35639c;border:none;color:#fff;padding:8px 16px;border-radius:6px;margin-top:12px;cursor:pointer}
.warn{background:#5a3535}.ok{background:#2e4a35}.muted{color:#7e93a8}
code{background:#243242;padding:1px 5px;border-radius:4px}
"""

BASE_T = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
{% if dash %}<meta http-equiv="refresh" content="30">{% endif %}
<title>Erik Tune</title><style>{{ css }}</style></head><body>
<header><h1>Erik Tune <span class="muted">egen värmestyrning</span></h1>
<nav><a href="/">Dashboard</a><a href="/setup">MQTT &amp; topics</a>
<a href="/topics">Topicutforskaren</a>
<a href="/tuning">Tuning</a></nav></header>{{ body }}</body></html>"""

DASH_T = """
<h2>Läge</h2>
<div class="card {{ 'ok' if mqtt_ok else 'warn' }}">
 <span>MQTT {{ host }}: {{ 'ansluten' if mqtt_ok else err }}</span>
 <span class="big">{{ mode }}</span></div>
<div class="card"><span>Väder-API Open-Meteo</span>
 <span class="muted">{{last_weather}}</span></div>

<h2>Vår styrning</h2>
<div class="card"><span>Inject (fejkad utetemp)</span><span class="big">{{ inject }} °C</span></div>
<div class="card"><span>Influence (vår Påverkan)</span><span class="big">{{ influence }} °C</span></div>
<div class="card"><span>Justera Påverkan (offset_c) direkt</span>
<form method="post" action="/offset">
<input name="v" type="number" step="0.5" value="{{ off }}" style="max-width:110px">
<button>Spara</button></form></div>
<div class="card"><span>Verklig utetemp (MQTT, annars prognos)</span><span class="big">{{ ute }} °C</span></div>
{{ spark }}

<h2>MQTT-ingångar</h2>
<table><tr><th>Roll</th><th>Värde</th><th>Senast</th><th>Topic</th></tr>
{% for r in rows %}<tr><td>{{r.role}}</td><td>{{r.val}}</td>
<td class="muted">{{r.age}}</td><td class="muted">{{r.topic}}{{r.diag}}</td></tr>{% endfor %}
</table>

<h2>Rumsreglering: är/bör</h2>
<div class="card"><span>Är-värde (rumstemperatur)</span><span class="big">{{ ar }} °C</span></div>
<div class="card"><span>Bör-värde (mål rumstemperatur)</span>
<form method="post" action="/setpoint">
<input name="v" type="number" step="0.5" value="{{ bor }}" style="max-width:110px">
<button>Spara</button></form></div>
<div class="card {{ 'ok' if auto_on else 'warn' }}">
<span>Självlärande reglering {{ 'PÅ' if auto_on else 'AV' }}</span>
<form method="post" action="/auto"><button>{{ 'Stäng av' if auto_on else 'Slå på' }}</button></form></div>
{{ learn_table }}

<h2>Väder, {{ place }} (Open-Meteo, lat {{lat}} lon {{lon}})</h2>
<div class="card"><span>Ute / Köldfaktor</span><span class="big">{{ w_temp }} / {{ w_kold }} °C</span></div>
<div class="card"><span>Vind / moln / sol</span><span class="muted">{{w_wind}} km/h, {{w_cc}} %, {{w_rad}} W/m²</span></div>
<div class="card"><span>Prognos {{pre_h}} h</span><span class="muted">{{w_ft}} °C (ändring {{w_drop}})</span></div>
<div class="card"><span>Köldfaktor prognos {{pre_h}} h</span><span class="muted">{{w_fk}} °C (ändring {{w_kold_drop}})</span></div>
{{ w_forecast_svg }}
"""

SETUP_T = """
<h2>Position: var på planeten?</h2>
<div class="card"><span>Valt ställe för väderprognos</span>
 <span class="big">{{ place }}<div class="muted">tidszon {{cfg["location"]["tz"]}} (styr prognos-indexet)</div></span></div>
<form method="get" action="/geo/search">
<label>Sök ort (Open-Meteo geokodning, svenska)</label>
<input name="q" placeholder="t.ex. Borlänge, Säter, Mora...">
<button>Sök</button></form>
<p class="muted">Välj träff i resultatlistan; koordinaterna lagras och styr
väder-APIt. Fine-justera lat/lon nedan till exakt tomt-position för bästa
gårdsprognos (microklimat, höjd över havet).</p>

<h2>MQTT-broker</h2>
<form method="post" action="/setup">
<label>Host</label><input name="host" value="{{cfg['mqtt']['host']}}" placeholder="192.168.1.10">
<label>Port</label><input name="port" value="{{cfg['mqtt']['port']}}">
<label>Användare (frivilligt)</label><input name="user" value="{{cfg['mqtt']['user']}}">
<label>Lösenord (frivilligt)</label><input name="passw" type="password" value="{{cfg['mqtt']['pass']}}">
<label>Topicutforskare (prenumerera på alla topics)</label>
<select name="explorer">
 <option value="on" {{ex1}}>on: listar allt som publiceras under /topics</option>
 <option value="off" {{ex2}}>off: prenumererar bara på rollerna</option>
</select>
<h2>Fine-position väder-API (fylls även i av ort-sökningen ovan)</h2>
<label>Latitud</label><input name="lat" value="{{cfg['location']['lat']}}">
<label>Longitud</label><input name="lon" value="{{cfg['location']['lon']}}">

<h2>MQTT-topics att prenumerera på</h2>
<table><tr><th>Roll</th><th>Topic-filter</th></tr>
{% for role, hint in roles.items() %}<tr>
<td>{{role}}<div class="muted">{{hint}}</div></td>
<td><input name="topic_{{role}}" value="{{cfg['topics'][role]}}"
   placeholder="t.ex. homeassistant/sensor/namn/state"></td></tr>{% endfor %}
</table>
<button>Spara</button></form>
<h2>Topics på brokern</h2>
<div class="card"><span>Sedda topics (senaste timmen, minnestabellen)</span>
<span class="big">{{ n_seen }}</span></div>
{% if seen_rows %}
<table><tr><th>Senast sedd topic</th><th>Värde</th></tr>
{% for r in seen_rows %}<tr>
<td>{{r.topic}}<div class="muted">{{r.age}}</div></td>
<td class="muted">{{r.payload}}</td></tr>{% endfor %}
</table>
{% endif %}
<p><a href="/topics">Öppna topicutforskaren: lista allt som publiceras,
sök på namn/värde och koppla en topic direkt till en roll - inga
topic-namn att skriva utantill.</a></p>
<p class="muted">Tips: HA-entiteter som inte redan finns på MQTT (SMHI-ute,
köldfaktorn, Tado) streamas enklast ut med en automation som gör
<code>mqtt.publish</code> vid varje ändring – se README.
Topics som publicerar JSON-objekt (t.ex. wmbus-mätarna) kopplas som
<code>topic|nyckel</code>, t.ex. <code>fjv/kamstrup/values|temp1</code> –
topicutforskaren bryter ut nycklarna automatiskt.</p>
"""

TUNE_T = """
<h2>Injektionsparametrar</h2>
<form method="post" action="/tuning">
<table>
<tr><td>Mode</td><td><select name="mode">
 <option value="shadow" {{sel}}>shadow (publicerar, ESP32/digipot av)</option>
 <option value="active" {{sel2}}>active</option></select></td></tr>
<tr><td>offset_c<div class="muted">konstant Påverkan; positiv = energibesparing</div></td>
<td><input name="offset_c" value="{{i['offset_c']}}"></td></tr>
<tr><td>pre_gain<div class="muted">grader Inject per grads prognosändring; positiv = värmer ikapp köldknäcken</div></td>
<td><input name="pre_gain" value="{{i['pre_gain']}}"></td></tr>
<tr><td>pre_hours</td><td><input name="pre_hours" value="{{i['pre_hours']}}"></td></tr>
<tr><td>koldfaktor_gain<div class="muted">0–1 av vindavkylningen som fejkad extrakyla; dämpa: ert hus förlorar bara ~1,6 kWh/dag extra per m/s vind</div></td>
<td><input name="koldfaktor_gain" value="{{i['koldfaktor_gain']}}"></td></tr>
<tr><td>solar_gain<div class="muted">grader fejkad extra värme per 100 W/m²</div></td>
<td><input name="solar_gain" value="{{i['solar_gain']}}"></td></tr>
<tr><td>Publicera Inject/Influence</td>
<td><select name="pub"><option value="on" {{p1}}>on</option>
<option value="off" {{p2}}>off</option></select></td></tr>
</table><button>Spara</button></form>
<h2>Formel</h2>
<p class="muted">Inject = ute + offset + koldfaktor_gain·(köldfaktor − ute, enbart kyliga)
+ solar_gain·sol/100 + pre_gain·(T_prognos(pre_hours) − ute), klippt till [min, max].
Influence = Inject − ute. Nollställda gains ger Inject = ute.</p>
"""


def render(body_t, dash=False, **ctx):
    # autoescape=True: MQTT-payloads, topic-namn, söksträngar m.m. escapes
    # automatiskt (trasiga payloads kan inte sönder layouten). Betrodd
    # intern HTML (spark/forecast/learn_table/pager/role_opts/diag) skickas
    # in inslagen i Markup(...) av anroparen - se länkarna nedan.
    env = Environment(autoescape=True)
    body = env.from_string(body_t).render(**ctx)
    return HTMLResponse(env.from_string(BASE_T).render(
        css=CSS, body=Markup(body), dash=dash))


def fmt_age(ts):
    if not ts:
        return "-"
    s = int(time.time() - ts)
    return f"{s//60} min" if s < 3600 else f"{s//3600} h"


def topic_diag(spec):
    """Diagnos-HTML for en rollkoppling utan friskt sample: talar om
    om topiken pratar pa busen och om JSON-nyckeln finns. Syns pa
    dashboardens ingangstabell sa fel kopplingar syns direkt."""
    if not spec:
        return ""
    base, _, key = spec.partition("|")
    with _lock:
        seen = _seen.get(base)
        fields = {k: v for k, v in (seen.get("fields") or {}).items()
                  if k not in _NOISE_KEYS} if seen else None
    if not seen:
        return ' <span class="warn">topic aldrig sedd sedan start</span>'
    out = f' <span class="warn">sedd {fmt_age(seen["ts"])} sedan</span>'
    if key and key not in (fields or {}):
        # nycklar/värden kommer från MQTT-payloads: escape:a även om hela
        # diag-blocket wrappas i Markup av anroparen (den stänger escape)
        ks = ", ".join(html.escape(k) for k in list(fields)[:6]) \
            if fields else "inga num. nycklar"
        out += (f' <span class="warn">nyckeln {html.escape(key)} saknas!'
                f' finns: {ks}</span>')
    return out


def spark_svg():
    def path_for(key, color):
        pts = read_series(key, 86400)
        if len(pts) < 2:
            return None, None
        vals = [p[1] for p in pts]
        lo, hi = min(vals), max(vals)
        pad = (hi - lo) or 1.0
        coords = " ".join(
            f"{(p[0] - pts[0][0]) / 86400 * 600:.0f},"
            f"{40 - (p[1] - lo) / pad * 36:.0f}" for p in pts)
        svg = (f'<svg width="600" height="44" style="margin-top:6px">'
               f'<polyline fill="none" stroke="{color}" stroke-width="2" '
               f'points="{coords}"/></svg>')
        return f"{lo:.1f}..{hi:.1f} °C (24 h)", svg
    rng_i, svg_i = path_for("inject_c", "#8ab4e8")
    rng_f, svg_f = path_for("influence_c", "#e8c98a")
    html = ""
    if svg_i:
        html += (f'<div class="card"><span>Inject 24 h&nbsp;'
                 f'<span class="muted">{rng_i}</span></span>{svg_i}</div>')
    if svg_f:
        html += (f'<div class="card"><span>Influence 24 h&nbsp;'
                 f'<span class="muted">{rng_f}</span></span>{svg_f}</div>')
    return html


@app.get("/", response_class=HTMLResponse)
def dashboard():
    if not CONFIG["mqtt"]["host"]:
        return RedirectResponse("/setup")
    w = _state["weather"]
    s = _state["samples"]
    rows = []
    for role in ROLES:
        rec = s.get(role)
        spec = CONFIG["topics"][role] or ""
        stale = (not rec) or (time.time() - rec["ts"] > 1800)
        rows.append({
            "role": role,
            "val": f"{rec['value']:.2f}" if rec else "-",
            "age": fmt_age(rec["ts"]) if rec else "-",
            "topic": (rec["topic"] if rec else (spec or "-")),
            "diag": Markup(topic_diag(spec)) if stale else Markup(""),
        })

    forecast_svg = ""
    series = (w or {}).get("series") or []
    if series:
        vals = [v for _, v in series]
        lo, hi = min(vals), max(vals)
        pad = (hi - lo) or 1.0
        coords = " ".join(f"{i * 50},{40 - (v - lo) / pad * 36:.0f}"
                          for i, (_, v) in enumerate(series))
        forecast_svg = ('<div class="card"><span>Prognos 12 h</span>'
                        '<svg width="600" height="44" style="margin-top:6px">'
                        '<polyline fill="none" stroke="#8ab4e8" stroke-width="2"'
                        f' points="{coords}"/></svg></div>')

    a = CONFIG["auto"]
    rec = _state["samples"].get("indoor_temp_c")
    ar = f"{rec['value']:.1f}" if rec else "-"
    learn_rows = ""
    for bd, v in sorted(((int(k), val) for k, val in a["bands"].items()),
                        reverse=True)[:12]:
        learn_rows += (f"<tr><td>{bd} … {bd + int(a['band_size'])} °C"
                       f" köldfaktor</td><td>{v:+.2f} °C</td></tr>")
    learn_table = ""
    if learn_rows:
        learn_table = ("<h2>Regulatorns minne (inlärd korrigering per"
                       " köldfaktorband)</h2><table><tr><th>Band</th>"
                       "<th>Korrigering</th></tr>"
                       f"{learn_rows}</table>"
                       '<p class="muted">Positiv korrigering = ökat'
                       " värmebehov: Inject sänks med samma belopp"
                       " (högre inject = fejk varmare = mindre värme).</p>")
    if _state.get("last_reg"):
        lg = _state["last_reg"]
        learn_table += (f'<p class="muted">Senaste reglering: fel {lg["err"]} °C'
                        f', korrigering {lg["corr"]} °C (positivt = mer värme),'
                        f' band {lg["band"]}'
                        + (f', effekt {lg["pw"]} kW'
                           if lg.get("pw") is not None else '')
                        + (', tappstopp: regleringen pausad'
                           if lg.get("pw_tap") else '')
                        + '.</p>')

    return render(DASH_T, dash=True,
        mqtt_ok=_state["mqtt_connected"],
        host=CONFIG["mqtt"]["host"] or "-",
        place=CONFIG["location"].get("place") or "-",
        err=_state["mqtt_error"] or "inte ansluten",
        mode=CONFIG["injector"]["mode"],
        last_weather=(f"senast {fmt_age(_state['last_weather_ok'])}"
                      if _state["last_weather_ok"] else "väntar på svar"),
        inject=_state["inject_c"], influence=_state["influence_c"],
        off=CONFIG["injector"]["offset_c"],
        ute=current_ute(), rows=rows,
        lat=CONFIG["location"]["lat"], lon=CONFIG["location"]["lon"],
        w_temp=((w or {}).get("temp_c", "-")),
        w_kold=((w or {}).get("koldfaktor_c", "-")),
        w_wind=((w or {}).get("wind_kmh", "-")),
        w_cc=((w or {}).get("cloud_cover", "-")),
        w_rad=((w or {}).get("solar_wm2", "-")),
        w_ft=((w or {}).get("forecast_temp_c", "-")),
        w_drop=((w or {}).get("forecast_drop_c", "-")),
        w_fk=((w or {}).get("forecast_koldfaktor_c", "-")),
        w_kold_drop=((w or {}).get("koldfaktor_drop_c", "-")),
        pre_h=CONFIG["injector"]["pre_hours"],
        spark=Markup(spark_svg()), w_forecast_svg=Markup(forecast_svg),
        ar=ar, bor=a["setpoint_c"],
        auto_on=(a.get("enabled") == "on"), learn_table=Markup(learn_table))


@app.get("/setup", response_class=HTMLResponse)
def setup_get():
    with _lock:
        seen = sorted(_seen.items(), key=lambda kv: kv[1]["ts"], reverse=True)
    def _disp(d):
        f = d.get("fields")
        if f:
            f = {k: v for k, v in f.items() if k not in _NOISE_KEYS}
            return " ".join(f"{k}={v}" for k, v in list(f.items())[:5])
        return d["payload"][:40]

    seen_rows = [{"topic": t, "payload": _disp(d),
                  "age": fmt_age(d["ts"])} for t, d in seen[:12]]
    ex = CONFIG["mqtt"].get("explorer", "on")
    return render(SETUP_T, cfg=CONFIG, roles=ROLES,
                  place=CONFIG["location"].get("place") or "inte valt",
                  seen_rows=seen_rows, n_seen=len(seen),
                  ex1="selected" if ex != "off" else "",
                  ex2="selected" if ex == "off" else "")


GEO_T = """
<h2>Ort-sökning: "{{ q }}"</h2>
{% if hits %}
<table><tr><th>Ställe</th><th>Koordinater</th><th></th></tr>
{% for h in hits %}<tr>
<td>{{ h.name }}<div class="muted">{{ h.admin }}, {{ h.country }}</div></td>
<td class="muted">{{ h.lat }}, {{ h.lon }}</td>
<td><a href="/geo/set?lat={{ h.lat }}&lon={{ h.lon }}&tz={{ h.tz }}&name={{ h.name_url }}">Välj</a></td>
</tr>{% endfor %}
</table>
{% else %}
<p class="muted">Inga träffar - prova annan stavning eller sök på närmaste
tätort.</p>{% endif %}
<p><a href="/setup">&laquo; Tillbaka till konfigureringen</a></p>
"""

GEO_URL = ("https://geocoding-api.open-meteo.com/v1/search"
           "?name={q}&count=8&language=sv&format=json")

TOPICS_T = """
<h2>Topics som publiceras på brokern</h2>
<form method="get" action="/topics">
<label>Filtrera (sök i topic-namn eller värde)</label>
<input name="q" value="{{ q }}">
<label><input type="checkbox" name="showall" {{ allsel }}>
Visa även HA-discovery, egna erik-tune och systemtopics</label>
<button>Filtrera</button></form>
<p class="muted">{{ n }} rader visas på sidan (av {{ total }} filtrerade;
{{ seen_total }} topics sedda totalt; sortering: senast sedd först).
Retained-meddelanden syns direkt efter anslutning.
JSON-topics (t.ex. wmbus-mätare) listas per nyckel: raden
<code>fjv/kamstrup/values ◆ temp1</code> kopplar rollen till
<code>fjv/kamstrup/values|temp1</code>.</p>
<table><tr><th>Topic</th><th>Senaste värde</th><th>Senast</th>
<th>Koppla till roll</th></tr>
{% for r in rows %}
<tr><td>{{r.topic}}</td><td class="muted">{{r.payload}}</td>
<td class="muted">{{r.age}}</td><td>
<form method="post" action="/topics/use">
<input type="hidden" name="topic" value="{{r.assign}}">
<select name="role">{{role_opts}}</select>
<button>Använd</button></form></td></tr>
{% endfor %}
</table>
{{ pager }}
<p><a href="/setup">&laquo; Tillbaka till konfigureringen</a></p>
"""


@app.get("/topics", response_class=HTMLResponse)
def topics(q: str = "", showall: str = "", page: int = 1):
    """Topicutforskare: se allt som publiceras, filtrera, koppla till roll."""
    with _lock:
        items = list(_seen.items())
    items.sort(key=lambda kv: kv[1]["ts"], reverse=True)
    seen_total = len(items)
    if not showall:
        items = [(t, d) for t, d in items if not (
            t.startswith("homeassistant/") and t.endswith("/config"))
            and not t.startswith("erik-tune/")
            and not t.startswith("$")]
    qf = (q or "").strip().lower()
    if qf:
        def _hit(t, d):
            if qf in t.lower() or qf in d["payload"].lower():
                return True
            for k, v in (d.get("fields") or {}).items():
                if qf in k.lower() or qf in str(v).lower():
                    return True   # sök även på JSON-nyckel/värde
            return False
        items = [(t, d) for t, d in items if _hit(t, d)]
    all_rows = []
    for t, d in items:      # ALLA filtrerade topics, ingen 200-topp
        fields = d.get("fields") or {}
        if not showall:    # brus som rssi/battery doljs; "visa alla" tar fram allt
            fields = {k: v for k, v in fields.items()
                      if k not in _NOISE_KEYS}
        if fields:   # JSON-topic: en rad per nyckel = valbara direkt
            for k in sorted(fields):
                all_rows.append({"topic": f"{t} ◆ {k}",
                                 "payload": str(fields[k]),
                                 "age": fmt_age(d["ts"]),
                                 "assign": f"{t}|{k}"})
        else:
            rows_ = {"topic": t, "payload": d["payload"],
                    "age": fmt_age(d["ts"]), "assign": t}
            all_rows.append(rows_)
    PAGE = 250
    n_pages = (len(all_rows) + PAGE - 1) // PAGE
    page = max(1, min(page, n_pages or 1))
    rows = all_rows[(page - 1) * PAGE: page * PAGE]

    def _link(p):
        params = {}
        if q:
            params["q"] = q
        if showall:
            params["showall"] = "1"
        params["page"] = str(p)
        return f'<a href="/topics?{urllib.parse.urlencode(params)}">{p}</a>'

    if n_pages > 1:
        parts = [f"<strong>{p}</strong>" if p == page else _link(p)
                 for p in range(1, n_pages + 1)]
        pager = '<p class="muted">Sida: ' + " ".join(parts) + "</p>"
    else:
        pager = ""
    role_opts = "".join(f'<option value="{r}">{r}</option>' for r in ROLES)
    return render(TOPICS_T, rows=rows, q=q or "",
                  role_opts=Markup(role_opts),
                  allsel="checked" if showall else "",
                  n=len(rows), total=len(all_rows),
                  seen_total=seen_total, pager=Markup(pager))


@app.post("/topics/use")
async def topics_use(request: Request):
    """Koppla en sedd topic till en roll (från topicutforskaren)."""
    f = await request.form()
    role, topic = f.get("role", ""), f.get("topic", "").strip()
    if role in ROLES and topic:
        CONFIG["topics"][role] = topic
        save_config(CONFIG)
        _mqtt_rebuild.set()  # prenumerera om med den nya rollkopplingen
        log(f"rollkoppling: {role} <- {topic}")
    return RedirectResponse("/setup", status_code=303)


@app.get("/geo/search", response_class=HTMLResponse)
def geo_search(q: str = ""):
    """Slå upp ort via Open-Meteos geokodnings-API, serversidat (ingen JS)."""
    q = (q or "").strip()
    if len(q) < 2:
        return RedirectResponse("/setup")
    try:
        r = requests.get(GEO_URL.format(q=urllib.parse.quote(q)), timeout=20)
        r.raise_for_status()
        results = r.json().get("results") or []
    except Exception:
        results = []
    hits = [{"name": g["name"],
             "admin": g.get("admin1") or "",
             "country": g.get("country") or "",
             "lat": round(g["latitude"], 4),
             "lon": round(g["longitude"], 4),
             "tz": g.get("timezone") or "",
             "name_url": urllib.parse.quote(g["name"])}
            for g in results]
    return render(GEO_T, q=q, hits=hits)


@app.get("/geo/set")
def geo_set(lat: float = 0.0, lon: float = 0.0, tz: str = "", name: str = ""):
    if -90 <= lat <= 90 and -180 <= lon <= 180:
        CONFIG["location"]["lat"] = lat
        CONFIG["location"]["lon"] = lon
        CONFIG["location"]["place"] = name or f"{lat}, {lon}"
        # tidszonen styr både Open-Meteos tidsstämplar och _now_label:
        # index i prognosserien blir rätt oavsett containerns TZ
        tz = (tz or "").strip()
        if tz:
            try:
                ZoneInfo(tz)   # ogiltig tidszon förkastas tyst
                CONFIG["location"]["tz"] = tz
            except Exception:
                pass
        save_config(CONFIG)
    return RedirectResponse("/setup", status_code=303)


def _form_to_config_ports(f):
    mqtt_changed = any([
        f.get("host", "") != CONFIG["mqtt"]["host"],
        f.get("port", str(CONFIG["mqtt"]["port"])) != str(CONFIG["mqtt"]["port"]),
        f.get("user", "") != CONFIG["mqtt"]["user"],
        f.get("passw", "") != CONFIG["mqtt"]["pass"]])
    CONFIG["mqtt"].update({"host": f.get("host", "").strip(),
                           "port": int(f.get("port") or 1883),
                           "user": f.get("user", "").strip(),
                           "pass": f.get("passw", "")})
    return mqtt_changed


@app.post("/setup")
async def setup_post(request: Request):
    global CONFIG
    f = await request.form()
    mqtt_changed = _form_to_config_ports(f)
    ex = "off" if f.get("explorer") == "off" else "on"
    if ex != CONFIG["mqtt"].get("explorer", "on"):
        CONFIG["mqtt"]["explorer"] = ex
        mqtt_changed = True
    try:
        CONFIG["location"]["lat"] = float(f.get("lat"))
        CONFIG["location"]["lon"] = float(f.get("lon"))
    except (TypeError, ValueError):
        pass  # ogiltig position ignoreras, konfig behålls
    old_topics = dict(CONFIG["topics"])
    for role in ROLES:
        CONFIG["topics"][role] = f.get(f"topic_{role}", "").strip()
    if CONFIG["topics"] != old_topics:
        mqtt_changed = True   # nya rollkopplingar: prenumerera om NU
    save_config(CONFIG)
    if mqtt_changed:
        _mqtt_rebuild.set()
    return RedirectResponse("/", status_code=303)


@app.get("/tuning", response_class=HTMLResponse)
def tune_get():
    i = CONFIG["injector"]
    return render(TUNE_T, i=i,
                  sel="selected" if i["mode"] == "shadow" else "",
                  sel2="selected" if i["mode"] == "active" else "",
                  p1="selected" if CONFIG["publish"]["inject"] == "on" else "",
                  p2="selected" if CONFIG["publish"]["inject"] == "off" else "")


@app.post("/tuning")
async def tune_post(request: Request):
    global CONFIG
    f = await request.form()
    i = CONFIG["injector"]
    i["mode"] = "active" if f.get("mode") == "active" else "shadow"
    for k in ("offset_c", "pre_gain", "koldfaktor_gain", "solar_gain"):
        try:
            i[k] = float(f.get(k) or 0)
        except ValueError:
            pass
    old_ph = i.get("pre_hours", 6)
    try:
        i["pre_hours"] = max(1, min(48, int(f.get("pre_hours") or 6)))
    except ValueError:
        pass
    if i["pre_hours"] != old_ph:
        _mqtt_rebuild.set()   # oppublicera discovery: ny horisont i namnen
    CONFIG["publish"]["inject"] = "off" if f.get("pub") == "off" else "on"
    save_config(CONFIG)
    return RedirectResponse("/", status_code=303)


@app.post("/offset")
async def offset_post(request: Request):
    """Snabbjustering av Påverkan från dashboarden (samma som HA-slidern)."""
    f = await request.form()
    try:
        v = round(float(f.get("v")), 2)
        if -30 <= v <= 30:
            CONFIG["injector"]["offset_c"] = v
            save_config(CONFIG)
            publish_state()
    except (TypeError, ValueError):
        pass  # ogiltig inmatning ignoreras, värdet behålls
    return RedirectResponse("/", status_code=303)


@app.post("/setpoint")
async def setpoint_post(request: Request):
    """Bör-värde (mål rumstemperatur) från dashboarden."""
    f = await request.form()
    try:
        v = round(float(f.get("v")), 1)
        if 5 <= v <= 30:
            CONFIG["auto"]["setpoint_c"] = v
            save_config(CONFIG)
            publish_state()
            log(f"börvärde = {v} C (webben)")
    except (TypeError, ValueError):
        pass
    return RedirectResponse("/", status_code=303)


@app.post("/auto")
async def auto_post():
    """Slå på/av självlärande rumsreglering."""
    CONFIG["auto"]["enabled"] = (
        "off" if CONFIG["auto"].get("enabled") == "on" else "on")
    save_config(CONFIG)
    log(f"självlärande reglering: {CONFIG['auto']['enabled'].upper()}")
    return RedirectResponse("/", status_code=303)


@app.get("/healthz")
def healthz():
    age = None
    if HEARTBEAT_FILE.exists():
        age = time.time() - HEARTBEAT_FILE.stat().st_mtime
    if age is None or age > 600:
        return HTMLResponse("stale", status_code=503)
    return HTMLResponse("ok")


# --------------------------------------------------------------------------
# Start
# --------------------------------------------------------------------------

if __name__ == "__main__":
    log("Erik Tune startar: webb på port "
        f"{WEB_PORT}, data i {DATA}, mqtt {CONFIG['mqtt']['host'] or 'ej satt'}"
        f" | mode {CONFIG['injector']['mode']}, reglering "
        f"{CONFIG['auto'].get('enabled', 'off')}")
    threading.Thread(target=mqtt_manager, daemon=True).start()
    threading.Thread(target=weather_loop, daemon=True).start()
    threading.Thread(target=control_loop, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=WEB_PORT, log_level="warning")

