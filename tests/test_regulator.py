#!/usr/bin/env python3
"""Erik Tune v0.18: regressions- och logiktester (ren Python, inga extra paket).

Körs antingen med pytest eller direkt:
    python3 tests/test_regulator.py          (eller: pytest tests/test_regulator.py -q)

Tung ramverk (FastAPI/paho/requests/uvicorn) stubbas ut vid import av app.py,
jinja2/markupsafe används riktiga (tests/vendor, ren Python). Databasen hamnar
i en tillfällig ERIK_DATA-katalog. Inget nätverk, ingen broker, inga trådar.

Täcker granskningens punkter:
  polaritet (corr appliceras som Inject - corr), rekursivt ar_c-filter,
  influence=Inject-ute efter klippning, /setup topic-omprenumerering,
  tidszon via ZoneInfo, DHW-gate (power_skip_kw/flow_skip_lh), dirty-flagga,
  SQLite-lås+WAL, gallring, autoescape/XSS, samt rena funktioner.
"""

import asyncio
import inspect
import json
import os
import sys
import tempfile
import time
import types
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
VENDOR = HERE / "vendor"
if VENDOR.exists():                     # ren-python jinja2/markupsafe för testerna
    sys.path.insert(0, str(VENDOR))
sys.path.insert(0, str(HERE.parent))

if not os.environ.get("ERIK_DATA"):     # DB/config i temp, aldrig /data
    os.environ["ERIK_DATA"] = tempfile.mkdtemp(prefix="erik-tune-test-")


# ---------------------------------------------------------------------------
# Stubbmoduler för de tunga beroendena (app.py skall gå att importera rakt av)
# ---------------------------------------------------------------------------
PUBLISHED = []          # (topic, payload) som MQTT-stubben tagit emot


class _Resp:
    def __init__(self, payload=None):
        self._json = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._json


_requests_canned = {}


def _requests_get(url, timeout=40):
    if url in _requests_canned:
        return _Resp(_requests_canned.pop(url))
    return _Resp({})   # tom prognos => poll_weather ger KeyError => hanteras av loopen


class _ReqModule(types.ModuleType):
    get = staticmethod(_requests_get)


class _CallbackAPIVersion:
    VERSION2 = 2


class _ClientStub:
    def __init__(self, api_version=None, client_id=None):
        self.client_id = client_id
        self.on_connect = self.on_disconnect = self.on_message = None

    def will_set(self, *a, **kw): pass
    def username_pw_set(self, *a, **kw): pass
    def connect_async(self, *a, **kw): pass
    def connect(self, *a, **kw): pass
    def loop_start(self, *a, **kw): pass
    def loop_stop(self, *a, **kw): pass
    def disconnect(self, *a, **kw): pass
    def subscribe(self, *a, **kw): pass

    def publish(self, topic, payload=None, retain=False, qos=0):
        PUBLISHED.append((topic, payload))
        return types.SimpleNamespace(rc=0)


def _seed_modules():
    def mod(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
        return m

    if "fastapi" not in sys.modules:
        class _FastAPIStub:
            def __init__(self, **kw):
                self.routes = {}

            def get(self, path, response_class=None):
                def deco(fn):
                    self.routes[("GET", path)] = fn
                    return fn
                return deco

            def post(self, path):
                def deco(fn):
                    self.routes[("POST", path)] = fn
                    return fn
                return deco

        class _RequestStub:
            def __init__(self, form_data=None):
                self._form = form_data or {}

            async def form(self):
                return self._form

        class _HTMLResponse:
            def __init__(self, content="", status_code=200):
                self.content = content
                self.body = content.encode() if isinstance(content, str) else content
                self.status_code = status_code

        class _RedirectResponse:
            def __init__(self, url, status_code=307):
                self.headers = {"location": url}
                self.status_code = status_code

        m = mod("fastapi", FastAPI=_FastAPIStub, Request=_RequestStub)
        m.responses = mod("fastapi.responses",
                          HTMLResponse=_HTMLResponse,
                          RedirectResponse=_RedirectResponse)
        mod("uvicorn", run=lambda **kw: None)
        mod("requests", get=_requests_get)
        paho = mod("paho")
        paho.mqtt = mod("paho.mqtt")
        paho.mqtt.client = mod("paho.mqtt.client",
                               CallbackAPIVersion=_CallbackAPIVersion,
                               Client=_ClientStub)


_seed_modules()
import app  # noqa: E402  (stubbar + vendor finns nu på plats)


NOW = time.time()


def _auto(**over):
    """Färsk auto-config; regleringen påslagen, over appliceras efter default."""
    a = json.loads(json.dumps(app.DEFAULT_CONFIG["auto"]))
    a["enabled"] = "on"
    a.update(over)
    return a


def _p(**over):
    p = json.loads(json.dumps(app.DEFAULT_CONFIG["injector"]))
    p.update(over)
    return p


# ---------------------------------------------------------------------------
# compute_inject / clip_inject
# ---------------------------------------------------------------------------

def test_clip_inject():
    p = _p()
    assert app.clip_inject(40.0, p) == 30.0
    assert app.clip_inject(-40.0, p) == -30.0
    assert app.clip_inject(5.556, p) == 5.56


def test_compute_inject_none_without_ute():
    assert app.compute_inject(None, {}, _p()) == (None, None)


def test_compute_inject_identity_default_gains():
    p = _p()  # offset 0, alla gains 0
    inj, inf = app.compute_inject(5.0, {"koldfaktor_c": -3.0, "solar_wm2": 400.0,
                                       "forecast_drop_c": -8.0}, p)
    assert inj == 5.0 and inf == 0.0


def test_compute_inject_offset_sign():
    # positiv offset = fejkad VARMARE ute = MINDRE värme till huset
    inj, inf = app.compute_inject(5.0, {}, _p(offset_c=2.0))
    assert inj == 7.0 and inf == 2.0


def test_compute_inject_koldfaktor_term():
    # koldfaktor under ute + gain 0.5 => inject under ute (fejk kallare)
    inj, inf = app.compute_inject(-2.0, {"koldfaktor_c": -8.0},
                                  _p(koldfaktor_gain=0.5))
    assert inj == -5.0 and inf == -3.0


def test_compute_inject_solar_and_pre_terms():
    inj, _ = app.compute_inject(0.0,
                                {"solar_wm2": 200.0, "forecast_drop_c": -6.0},
                                _p(solar_gain=0.5, pre_gain=1.0))
    # 0 + 0.5*200/100 (=+1, sol fejk varmare) + 1.0*-6 (= -6, kyla fram)
    assert inj == -5.0


def test_compute_inject_clip_30():
    inj, inf = app.compute_inject(-29.0, {}, _p(offset_c=-3.0))
    assert inj == -30.0 and inf == -1.0   # klippt: influence = inject - ute


# ---------------------------------------------------------------------------
# apply_regulation: TECKNET + influence efter klippning
# ---------------------------------------------------------------------------

def test_apply_regulation_sign():
    # positiv corr = öka värmebehovet => inject SÄNKS (mer värme till huset)
    inj, inf = app.apply_regulation(5.0, {"corr": 2.0}, 5.0, _p())
    assert inj == 3.0 and inf == -2.0


def test_apply_regulation_influence_holds_at_floor():
    # inject slår i golvet -30: influence skall vara -30 - ute, inte "oclippat"
    inj, inf = app.apply_regulation(-29.5, {"corr": 2.0}, -28.0, _p())
    assert inj == -30.0
    assert inf == -2.0    # = -30 - (-28); gamla koden hade gett -1.5-2.0 felväg


def test_sign_regression_tripwires():
    src_apply = inspect.getsource(app.apply_regulation)
    assert 'inject_c - reg["corr"]' in src_apply      # subtraheras, aldrig adderas
    src_ctrl = inspect.getsource(app.control_loop)
    assert "apply_regulation(" in src_ctrl
    assert 'inject_c + reg["corr"]' not in src_ctrl  # v0.17:s positiva återkoppling
    assert 'read_series("indoor_temp_c"' in src_ctrl  # RÅ serie, inte ar_c
    assert 'read_series("ar_c"' not in src_ctrl        # rekursivt filter borta


# ---------------------------------------------------------------------------
# auto_regulate: direction, dödband, filter, inlärning, DHW-gate, dirty
# ---------------------------------------------------------------------------

def _reg(a, temp=20.0, hist=None, kold=-5.0, pw=None, flow=None,
         last_learn=0.0, now=None):
    now = now if now is not None else time.time()
    indoor = (now, temp)
    return app.auto_regulate(a, indoor, hist if hist is not None else [],
                             kold, pw, flow, now, last_learn)


def test_regulate_disabled():
    reg, dirty = _reg(_auto(enabled="off", setpoint_c=21.0), temp=20.0)
    assert reg is None and dirty is False


def test_regulate_stale_indoor():
    a = _auto(sample_max_age_s=1800)
    old = NOW - 99999
    reg, dirty = app.auto_regulate(a, (old, 20.0), [], -5.0, None, None,
                                   time.time(), 0)
    assert reg is None


def test_regulate_without_kold():
    reg, dirty = _reg(_auto(), kold=None)
    assert reg is None


def test_polarity_cold_house_lowers_inject():   # GRANSKNINGENS PUNKT 1
    a = _auto(setpoint_c=21.5)                   # k_p 0.5, bands tomt
    now = time.time()
    hist = [(now - 1800, 20.0), (now - 900, 20.0), (now - 60, 20.0)]
    reg, _ = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None, now, 0)
    assert reg["err"] == 1.5                     # för kallt: positivt fel
    assert reg["corr"] > 0                       # ökat värmebehov ...
    inj, inf = app.apply_regulation(5.0, reg, 5.0, _p())
    assert inj < 5.0 and inf < 0                 # ... => fejk KALLARE => MER värme


def test_polarity_warm_house_raises_inject():
    a = _auto(setpoint_c=21.5)
    now = time.time()
    indoor = (now, 23.0)                        # 1,5 gr för varmt
    hist = [(now - 1800, 23.0), (now - 60, 23.0)]
    reg, _ = app.auto_regulate(a, indoor, hist, -5.0, None, None, now, 0)
    assert reg["err"] == -1.5 and reg["corr"] < 0
    inj, inf = app.apply_regulation(5.0, reg, 5.0, _p())
    assert inj > 5.0 and inf > 0                 # fejk varmare => mindre värme


def test_deadband_hold_keeps_memory():
    a = _auto(setpoint_c=21.5)
    a["bands"]["-5"] = 1.2
    now = time.time()
    indoor = (now, 21.4)                          # fel 0.1 <= deadband 0.3
    reg, dirty = app.auto_regulate(a, indoor, [(now - 60, 21.4)], -5.0,
                                  None, None, now, 0)
    assert reg["hold"] is True
    assert reg["corr"] == 1.2                     # corr = bias (minnet hålls)
    assert reg["learned"] is False and dirty is False
    assert a["bands"]["-5"] == 1.2                # inget nytt lärande


def test_ar_filter_mean_of_raw_series():         # GRANSKNINGENS PUNKT 2
    a = _auto(avg_window_s=1800)
    now = time.time()
    hist = [(now - 1700, 19.0), (now - 900, 20.0), (now - 100, 21.0)]
    reg, _ = app.auto_regulate(a, (now, 22.0), hist, -5.0, None, None, now, 0)
    assert reg["ar_c"] == 20.0                    # medel av RÅ-serien...
    # ...(v0.17 läste ar_c som redan var filtrerat => rekursivt filter)


def test_ar_filter_insufficient_coverage_uses_momentary():
    a = _auto(avg_window_s=1800)
    now = time.time()
    hist = [(now - 120, 21.0)]                    # täcker inte halva fönstret
    reg, _ = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None, now, 0)
    assert reg["ar_c"] == 20.0                     # momentanvärdet gäller


def test_bias_learning_rate_and_interval():
    a = _auto(setpoint_c=21.5, learn_rate=0.05, learn_interval_s=900)
    now = time.time()
    hist = [(now - 1700, 20.0), (now - 60, 20.0)]
    reg, dirty = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None, now, 0)
    assert reg["learned"] is True and dirty is True
    assert a["bands"]["-5"] == 0.075               # 0.05 * err 1.5
    # direkt efteråt: learn_interval skall blockera nytt lärande
    last = reg["learn_ts"]
    reg2, dirty2 = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None,
                                    now + 10, last)
    assert reg2["learned"] is False and dirty2 is False
    # 900+ sekunder senare: lär igen
    reg3, dirty3 = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None,
                                     now + 950, last)
    assert reg3["learned"] is True and a["bands"]["-5"] == 0.15


def test_bias_clipped_at_max_correction():
    a = _auto(setpoint_c=21.5, max_correction_c=0.1)
    a["bands"]["-5"] = 0.09
    now = time.time()
    hist = [(now - 1700, 20.0), (now - 60, 20.0)]
    reg, _ = app.auto_regulate(a, (now, 20.0), hist, -5.0, None, None, now, 0)
    assert a["bands"]["-5"] == 0.1                # klippt


def test_p_bal_ema_learning_and_dirty():
    a = _auto(setpoint_c=21.5)                     # frö '-5': 3.3 kW
    now = time.time()
    indoor = (now, 21.4)                           # dödband => balansinlärning
    reg, dirty = app.auto_regulate(a, indoor, [], -5.0, 4.0, None, now, 0)
    assert a["p_bal"]["-5"] == 3.37                # 0.9*3.3 + 0.1*4.0
    assert dirty is True                           # delta 0.07 >= 0.05 => spara


def test_p_bal_new_band_dirty_puts_seed():
    a = _auto(setpoint_c=21.5)
    a["p_bal"] = {}                                # inga frön alls
    reg, dirty = app.auto_regulate(a, (time.time(), 21.4), [], -5.0, 4.0,
                                  None, time.time(), 0)
    assert a["p_bal"]["-5"] == 4.0 and dirty is True


def test_p_bal_small_delta_not_dirty():
    a = _auto(setpoint_c=21.5)
    now = time.time()
    reg, dirty = app.auto_regulate(a, (now, 21.4), [], -5.0, 3.32, None, now, 0)
    assert a["p_bal"]["-5"] == 3.302               # EMA rör sig...
    assert dirty is False                         # ...men för lite för disk


def test_dhw_gate_power_skips_learning_and_corr_p():   # BESLUT 24/11
    a = _auto(setpoint_c=21.5)                     # frö '-5': 3.3
    now = time.time()
    reg, dirty = app.auto_regulate(a, (now, 21.4), [], -5.0, 17.0, None, now, 0)
    assert reg["pw_tap"] is True
    assert a["p_bal"]["-5"] == 3.3                 # ingen inlärning av duschen
    assert dirty is False
    # utanför dödbandet: corr_p skall vara stängd trots gap 17 vs 3.3
    a2 = _auto(setpoint_c=21.5)
    reg2, _ = app.auto_regulate(a2, (now, 20.0), [], -5.0, 17.0, None, now, 0)
    assert reg2["pw_tap"] is True and reg2["corr_p"] == 0.0


def test_dhw_gate_flow_triggers_too():
    a = _auto(setpoint_c=21.5)
    now = time.time()
    reg, dirty = app.auto_regulate(a, (now, 21.4), [], -5.0, 5.0, 660.0, now, 0)
    assert reg["pw_tap"] is True                  # flödet > 300 l/h gaterar
    assert a["p_bal"]["-5"] == 3.3 and dirty is False


def test_corr_p_direction_and_clip():
    a = _auto(setpoint_c=18.0)                     # utanför dödband, pw färsk
    now = time.time()
    # huset underförsörjt (2.0 < balans 3.3): corr_p positiv = MER värmebehov
    reg, _ = app.auto_regulate(a, (now, 17.0), [(now - 1500, 17.0)], -5.0,
                               2.0, None, now, 0)
    assert reg["corr_p"] == 0.65                  # 0.5 * (3.3-2.0)
    # stort gap klipps vid max_correction
    a2 = _auto(setpoint_c=18.0)
    a2["p_bal"]["-5"] = 9.9
    reg2, _ = app.auto_regulate(a2, (now, 10.0), [(now - 1500, 10.0)], -5.0,
                                0.0, None, now, 0)
    assert reg2["corr_p"] == 3.0                   # clip 0.5*(9.9-0) -> 3.0
    # slutften: ingen färsk effekt => ren P-reglering
    reg3, _ = app.auto_regulate(a, (now, 17.0), [(now - 1500, 17.0)], -5.0,
                               None, None, now, 0)
    assert reg3["corr_p"] == 0.0


def test_hold_result_carries_learn_ts_and_pw():
    a = _auto(setpoint_c=21.5)
    now = time.time()
    reg, _ = app.auto_regulate(a, (now, 21.4), [], -5.0, 4.4, None, now, 777.0)
    assert reg["learn_ts"] == 777.0 and reg["pw"] == 4.4 and reg["hold"]


def test_persistent_cold_band_converges_and_clips():
    # ihållande för kallt hus i bandet: bias växer monotont tills klippning,
    # och inject sjunker monotont mot "fullt värmebehov" (stabilisering)
    a = _auto(setpoint_c=21.5, learn_interval_s=60)  # lär varje cykel (taket snabbt)
    p = _p()
    last_learn, corr_prev, inject_prev = 0.0, None, None
    for cycle in range(80):                 # 80 minuterscykler
        now = NOW + cycle * 60
        reg, dirty = app.auto_regulate(a, (now, 20.0),
                                       [(now - 1500, 20.0)], -5.0, None, None,
                                       now, last_learn)
        last_learn = reg["learn_ts"]
        assert reg["corr"] >= 0             # aldrig fel tecken när huset är kallt
        if corr_prev is not None:
            assert reg["corr"] >= corr_prev - 1e-9   # monotont växande behov
        corr_prev = reg["corr"]
        inj, inf = app.apply_regulation(5.0, reg, 5.0, p)
        if inject_prev is not None:
            assert inj <= inject_prev + 1e-9       # inject faller monotont
        inject_prev = inj
    assert reg["corr"] == 3.0               # klippt vid max_correction_c
    assert a["bands"]["-5"] == 3.0


# ---------------------------------------------------------------------------
# Tidszon (ZoneInfo ur config, inte containerns TZ)
# ---------------------------------------------------------------------------

def test_now_label_uses_configured_zone():
    for attempt in range(2):      # robust mot timsläpp mellan raderna
        want = datetime.now(ZoneInfo("Europe/Stockholm")).strftime("%Y-%m-%dT%H:00")
        if app._now_label("Europe/Stockholm") == want:
            return
        time.sleep(0.2)
    assert False, app._now_label("Europe/Stockholm")


def test_now_label_invalid_zone_falls_back():
    got = app._now_label("Foo/Bar")
    want = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:00")
    assert got == want


def test_poll_weather_picks_current_hour_in_config_tz():
    tz = app.CONFIG["location"]["tz"] or "Europe/Stockholm"
    now = datetime.now(ZoneInfo(tz)).replace(minute=0, second=0, microsecond=0)
    hours = [(now + timedelta(hours=d)) for d in (-2, -1, 0, 1, 2, 3)]
    times = [h.strftime("%Y-%m-%dT%H:00") for h in hours]
    temps = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    winds = [4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    rads = [0.0, 10.0, 20.0, 30.0, 40.0, 50.0]
    ccs = [0, 20, 40, 60, 80, 100]
    key = app.OM_URL.format(lat=app.CONFIG["location"]["lat"],
                            lon=app.CONFIG["location"]["lon"],
                            tz=tz)
    _requests_canned[key] = {"hourly": {"time": times, "temperature_2m": temps,
                                       "wind_speed_10m": winds,
                                       "shortwave_radiation": rads,
                                       "cloud_cover": ccs}}
    app.poll_weather()
    w = app._state["weather"]
    assert w["temp_c"] == 12.0          # aktuell timme (t <= now, sista)
    assert w["koldfaktor_c"] == app.ec_koldfaktor(12.0, 6.0)
    hz = min(0 + max(1, int(app.CONFIG["injector"]["pre_hours"])),
             len(temps) - 1)
    assert w["forecast_temp_c"] == temps[hz]


# ---------------------------------------------------------------------------
# Webb: autoescape/XSS, /setup topic-omprenumerering, tidszon vid ortval
# ---------------------------------------------------------------------------

def test_render_escapes_untrusted():
    resp = app.render('<p>{{ q }}</p>', q="<script>alert(1)</script>")
    assert "&lt;script&gt;" in resp.content
    assert "<script>alert" not in resp.content


def test_render_passes_markup_trusted_html():
    resp = app.render("<div>{{ m }}</div>", m=app.Markup('<b class="ok">x</b>'))
    assert '<b class="ok">x</b>' in resp.content


def test_dashboard_escapes_mqtt_data():
    app.CONFIG["mqtt"]["host"] = "broker.test"     # slipper redirect till /setup
    saved = (dict(app.CONFIG["topics"]), dict(app._state["samples"]))
    try:
        evil_topic = 'ev<script>il/state'
        # roll MED färskt sample: topic-namnet (externt) skall escapas
        app.CONFIG["topics"]["ute_temp_c"] = evil_topic
        app._state["samples"]["ute_temp_c"] = {"ts": time.time(), "value": 5.0,
                                               "topic": evil_topic}
        app._state["samples"]["indoor_temp_c"] = {"ts": time.time(),
                                                 "value": 21.0,
                                                 "topic": evil_topic}
        # roll UTAN sample men med spec + sedd topic: topic_diag (Markup =
        # betrodd) interpolerar JSON-nycklar som MASTE vara html.escape:ade
        app.CONFIG["topics"]["t1_c"] = evil_topic + "|miss<script>ing"
        with app._lock:
            app._seen[evil_topic] = {"payload": "<x>", "ts": time.time(),
                                     "fields": {"k<script>ey": 1.0, "temp1": 2.0}}
        app._state["last_reg"] = {"err": 0.5, "corr": 0.7, "band": -5,
                                 "pw": 17.0, "pw_tap": True}
        resp = app.dashboard()
        content = resp.content
        assert "<script>" not in content       # inget oescapat externt någonsin
        assert "ev&lt;script&gt;il/state" in content
        assert "miss&lt;script&gt;ing" in content   # diag: nyckeln escapad
        assert "k&lt;script&gt;ey" in content       # diag: fältnamn escapade
        assert "tappstopp" in content          # gate-synlig i UI:t
    finally:
        app.CONFIG["topics"], app._state["samples"] = saved
        with app._lock:
            app._seen.pop('ev<script>il/state', None)
        app._state["last_reg"] = None
        app.CONFIG["mqtt"]["host"] = ""


def _post_setup(fields):
    req = sys.modules["fastapi"].Request(fields)
    return asyncio.run(app.setup_post(req))


def test_setup_topic_change_triggers_resubscribe():   # GRANSKNINGENS PUNKT 4
    saved = {r: app.CONFIG["topics"][r] for r in app.ROLES}
    app._mqtt_rebuild.clear()
    try:
        fields = {"host": app.CONFIG["mqtt"]["host"],
                  "port": str(app.CONFIG["mqtt"]["port"]),
                  "user": app.CONFIG["mqtt"]["user"],
                  "passw": app.CONFIG["mqtt"]["pass"],
                  "explorer": "on",
                  "lat": str(app.CONFIG["location"]["lat"]),
                  "lon": str(app.CONFIG["location"]["lon"])}
        for r in app.ROLES:
            fields[f"topic_{r}"] = ""
        fields["topic_ute_temp_c"] = "zigbee2mqtt/matsalen"
        _post_setup(fields)
        assert app._mqtt_rebuild.is_set(), "topicbyte via /setup skall omprenumerera"
    finally:
        app._mqtt_rebuild.clear()
        for r in app.ROLES:
            app.CONFIG["topics"][r] = saved[r]


def test_setup_unchanged_topics_no_resubscribe():
    app._mqtt_rebuild.clear()
    try:
        fields = {"host": app.CONFIG["mqtt"]["host"],
                  "port": str(app.CONFIG["mqtt"]["port"]),
                  "user": app.CONFIG["mqtt"]["user"],
                  "passw": app.CONFIG["mqtt"]["pass"],
                  "explorer": app.CONFIG["mqtt"].get("explorer", "on"),
                  "lat": str(app.CONFIG["location"]["lat"]),
                  "lon": str(app.CONFIG["location"]["lon"])}
        for r in app.ROLES:
            fields[f"topic_{r}"] = app.CONFIG["topics"][r]
        _post_setup(fields)
        assert not app._mqtt_rebuild.is_set(), "oförändrat /setup skall inte ombygga"
    finally:
        app._mqtt_rebuild.clear()


def test_geo_set_stores_timezone():
    saved = dict(app.CONFIG["location"])
    try:
        app.geo_set(lat=59.33, lon=18.07, tz="Europe/Stockholm",
                    name="Stockholm")
        assert app.CONFIG["location"]["tz"] == "Europe/Stockholm"
        app.geo_set(lat=0.0, lon=0.0, tz="Not/AZone", name="X")
    finally:
        app.CONFIG["location"] = saved


# ---------------------------------------------------------------------------
# SQLite: lås, gallring
# ---------------------------------------------------------------------------

def test_prune_samples_removes_old_only():
    now = int(time.time())
    old = now - 450 * 86400
    with app._db_lock:
        app._db.execute("INSERT OR REPLACE INTO samples VALUES (?,?,?)",
                        (old, "test_prune_gammal", 1.0))
        app._db.execute("INSERT OR REPLACE INTO samples VALUES (?,?,?)",
                        (now, "test_prune_färsk", 2.0))
        app._db.commit()
    try:
        removed = app.prune_samples(400)
        assert removed >= 1
        with app._db_lock:
            old_left = app._db.execute(
                "SELECT COUNT(*) FROM samples WHERE key='test_prune_gammal'"
            ).fetchone()[0]
            new_left = app._db.execute(
                "SELECT COUNT(*) FROM samples WHERE key='test_prune_färsk'"
            ).fetchone()[0]
        assert old_left == 0 and new_left == 1
    finally:
        with app._db_lock:
            app._db.execute("DELETE FROM samples WHERE key LIKE 'test_prune_%'")
            app._db.commit()


def test_defaults_have_gate_and_retention():
    assert app.DEFAULT_CONFIG["auto"]["power_skip_kw"] == 8.0
    assert app.DEFAULT_CONFIG["auto"]["flow_skip_lh"] == 300.0
    assert app.DEFAULT_CONFIG["logging"]["retention_days"] == 400



def test_load_config_backfills_old_config():
    # Pi:ns befintliga config.json är från en äldre version: deep-mergeen skall
    # fylla i power_skip_kw/flow_skip_lh, logging.retention_days osv. behålla
    # Eriks värden (bands, p_bal, topics, broker).
    old = {
        "mqtt": {"host": "192.168.8.149", "port": 1883, "user": "",
                 "pass": "", "client_id": "erik-tune", "explorer": "off"},
        "location": {"lat": 60.4858, "lon": 15.4342, "tz": "Europe/Stockholm",
                     "place": "Borlänge"},
        "topics": {"ute_temp_c": "ha/sensor/ute/state", "fjv_power_kw":
                   "fjv/kamstrup/values|power"},
        "injector": {"mode": "shadow", "offset_c": 0.5, "pre_gain": 0.2,
                     "pre_hours": 6, "koldfaktor_gain": 0.3, "solar_gain": 0.1,
                     "min_inject_c": -30.0, "max_inject_c": 30.0},
        "weather": {"poll_interval_s": 1800},
        "publish": {"inject": "on", "discovery": "on"},
        "auto": {"enabled": "on", "setpoint_c": 21.0, "k_p": 0.5,
                 "learn_rate": 0.05, "learn_interval_s": 900,
                 "deadband_c": 0.3, "avg_window_s": 1800, "band_size": 5,
                 "power_gain": 0.5, "power_max_age_s": 900,
                 "bands": {"-5": 0.3}, "p_bal": {"-5": 3.9},
                 "max_correction_c": 3.0, "sample_max_age_s": 1800},
    }
    app.CONFIG_PATH.write_text(json.dumps(old, ensure_ascii=False))
    try:
        cfg = app.load_config()
        assert cfg["auto"]["power_skip_kw"] == 8.0       # nyckeln fylld
        assert cfg["auto"]["flow_skip_lh"] == 300.0
        assert cfg["auto"]["p_bal"]["-5"] == 3.9          # Erik:s data orört
        for band, v in app.DEFAULT_CONFIG["auto"]["p_bal"].items():
            if band in old["auto"]["p_bal"]:
                continue  # banden har redan Eriks värde och skall bevaras
            assert cfg["auto"]["p_bal"].get(band) == v    # frön fylls där lucka
        assert cfg["auto"]["p_bal"]["-5"] == 3.9          # ...och aldrig skrivs över
        assert cfg["logging"]["retention_days"] == 400    # ny sektion fylld
        assert cfg["mqtt"]["host"] == "192.168.8.149"      # och inget raderat
        assert cfg["auto"]["bands"]["-5"] == 0.3
    finally:
        app.CONFIG_PATH.unlink(missing_ok=True)
        app.load_config()   # globalen i modulen rördes inte; disk rensad

# ---------------------------------------------------------------------------
# Endpoint-röktest: alla sidor renderar med riktiga jinja2 (autoescape) + stubbar,
# betrodd HTML kommer fram rå, extern data escapas
# ---------------------------------------------------------------------------

def test_endpoints_smoke():
    saved = (dict(app.CONFIG["mqtt"]), dict(app.CONFIG["topics"]),
             dict(app._state["samples"]))
    try:
        app.CONFIG["mqtt"]["host"] = "broker.test"
        # 300 sedda topics = paginering: pager (Markup) + rollval (Markup)
        with app._lock:
            for i in range(301):
                app._seen[f"smoke/topic{i}"] = {"payload": str(i), "ts": time.time(),
                                                "fields": {}}
        # söksträngen escapas i utdata, rollval/paginering är betrodd HTML
        resp = app.topics(q="<script>", showall="", page=1)
        assert "&lt;script&gt;" in resp.content     # q escapas i input-raden
        resp = app.topics(q="smoke/", showall="", page=1)
        assert "<option" in resp.content            # role_opts rå (Markup)
        assert '<a href="/topics?' in resp.content  # pager rå (Markup): 301 rader
        with app._lock:
            for i in range(301):
                app._seen.pop(f"smoke/topic{i}", None)

        # ort-sökning med elakt namn + tidszon från geokodnings-APIt
        key = app.GEO_URL.format(q=urllib.parse.quote("borlänge"))
        _requests_canned[key] = {"results": [
            {"name": "Bo<script>ränge", "latitude": 60.48, "longitude": 15.43,
             "country": "Sverige", "admin1": "Dalarna",
             "timezone": "Europe/Stockholm"}]}
        resp = app.geo_search(q="borlänge")
        assert "Bo&lt;script&gt;ränge" in resp.content
        assert "Europe/Stockholm" in resp.content      # tz följer med i länken

        # /setup och /tuning renderar med config (autoescape på platshållare)
        assert "MQTT-broker" in app.setup_get().content
        assert "Injektionsparametrar" in app.tune_get().content

        # dashboard utan konfigurerad broker redirectar till /setup
        app.CONFIG["mqtt"]["host"] = ""
        r = app.dashboard()
        assert isinstance(r, sys.modules["fastapi"].responses.RedirectResponse)

        # healthz: heartbeat-fil saknas => stale
        app.HEARTBEAT_FILE.unlink(missing_ok=True)
        h = app.healthz()
        assert h.status_code == 503
        app.HEARTBEAT_FILE.write_text(str(int(time.time())))
        assert app.healthz().status_code == 200
    finally:
        app.CONFIG["mqtt"], app.CONFIG["topics"], app._state["samples"] = saved


# ---------------------------------------------------------------------------
# Runner (pytest plockar upp samma funktioner automatiskt)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for n, f in tests:
        try:
            f()
            print(f"PASS  {n}")
        except Exception as exc:                       # noqa: BLE001
            failed += 1
            print(f"FAIL  {n}: {type(exc).__name__}: {exc}")
            import traceback
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} OK")
    sys.exit(1 if failed else 0)

