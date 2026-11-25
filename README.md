# Erik Tune

A self-hosted, single-file "smart heating brain": a DIY alternative to commercial
smart heating controllers such as [Ngenic Tune](https://ngenic.se) for district
heating substations. Erik Tune computes a **faked outdoor temperature**
("Inject") from weather forecasts, wind chill and solar gain, publishes it over
MQTT, and logs everything to SQLite. In active use, an ESP32 + digital
potentiometer replaces the outdoor sensor signal of the heating controller
(that hardware is separate; this repo is the brain).

> Not affiliated with Ngenic AB. "Erik Tune" is a personal hobby project
> inspired by how Ngenic Tune works.

## How it works

The injection brain runs one formula (all parameters tunable in the web UI):

```
Inject = outdoor
       + offset
       + windchill_gain * (windchill - outdoor)   # only when windchill is colder
       + solar_gain * radiation / 100             # W/m2, warms the signal
       + pre_gain * (forecast_temp(pre_hours) - outdoor)  # weather front lookahead
       clamped to [inject_min, inject_max]
```

With all gains at zero, Inject equals the real outdoor temperature and
Influence is 0: a safe starting point. Every term is visible in the UI so you
always know why the signal moves.

**Influence** = Inject minus real outdoor temperature, the same vocabulary as
Ngenic Tune, so both can run in parallel all winter for A/B comparison.

Optionally, a self-learning room controller (`auto`, off by default) nudges
Inject toward a target room temperature: a P-term on the filtered room error,
a learned bias per wind-chill band (slow integral), and a fast inner loop on
the measured district-heating power. **Sign convention (v0.18)**: a positive
correction means *increase heat demand* and is applied as `Inject − corr`,
because the substation's heating curve is falling: a higher faked outdoor
temperature means less heat. Balance learning and the power loop are paused
during tap-water stops (config `auto.power_skip_kw` / `auto.flow_skip_lh`:
measured tap stops peak at 17–33 kW / 660–1020 l/h vs normal radiator load
p95 = 7.2 kW).

## Features

- **Single Python file** (`app.py`) with everything: MQTT, weather, brain, web UI, logging
- **MQTT with Home Assistant auto-discovery**: sensors appear under the device
  "Erik Tune": *Injected Outdoor*, *Influence*, *Köldfaktor* (wind chill now),
  *Köldfaktorprognos* (wind chill at horizon), *Solinstrålning*, *Prognostemp*,
  *Prognosändring*, *Mode* (names in the UI are Swedish)
- **Weather from [Open-Meteo](https://open-meteo.com)** (no API key), polled every
  30 minutes: temperature, wind, shortwave radiation, cloud cover
- **Wind chill (EC apparent temperature) computed internally**, both for now and
  for the whole forecast series: no weather API ships wind chill as a forecast
- **Built-in city search** (Open-Meteo geocoding): pick your position at
  `http://<host>:8080` and the forecast follows you, anywhere on the planet
- **SQLite logging** (every 60 s): outdoor, indoor, inject, influence, wind chill,
  solar, T1/T2, flow and power: your own regression data for heating-curve tuning
- **Shadow mode by default**: everything is published and logged, nothing is
  controlled. Switching to `active` mode is explicit, and the watchdog/failsafe
  lives on the ESP32 side (heartbeat timeout there parks the digipot at a fixed
  safe value)

## MQTT contract

| Topic | Payload | Purpose |
|---|---|---|
| `erik-tune/inject/temp_c` | float, retain, QoS 1 | The faked outdoor temperature |
| `erik-tune/influence/c` | float, retain | Inject − real outdoor temperature |
| `erik-tune/mode` | `shadow` \| `active` | Publication-only marker |
| `erik-tune/heartbeat` | `online`/`offline` | Retained + MQTT LWT |
| `erik-tune/koldfaktor/c` | float | Wind chill right now |
| `erik-tune/forecast/koldfaktor_c` | float | Wind chill at horizon (+N h) |
| `erik-tune/weather/now` | JSON | Full weather: temp, wind, radiation, wind chill series |
| `erik-tune/solar/wm2` | int | Shortwave radiation |
| `erik-tune/forecast/temp_c` | float | Forecast temperature at horizon |
| `erik-tune/forecast/drop_c` | float | Forecast temp − current temp |

Subscribable roles (configured in the UI): outdoor temperature, indoor
temperature, heating supply/return (T1/T2), district heating flow (l/h), power
(kW), tap water flow (m3/h). Any role left empty simply stays unlogged, and if
the outdoor temperature topic is empty, Open-Meteo's current temperature is used.

## Quick start (Docker)

```bash
git clone https://github.com/Eswede/erik-tune.git
cd erik-tune
docker compose up -d --build
```

Open `http://<host>:8080`: the setup page guides you through MQTT broker,
position (city search) and topic roles. Configuration is stored in
`data/config.json`, samples in `data/samples.db`.

Verify the flow with a broker on the LAN:

```bash
mosquitto_sub -h <broker> -t 'erik-tune/#' -v
```

Within a minute you should see `heartbeat`, `inject/temp_c`, `influence/c` and
the weather JSON on `erik-tune/weather/now`.

### Home Assistant

Sensors appear automatically via MQTT discovery. Home assistant entities that
lack an MQTT topic (SMHI outdoor temperature, template sensors, radiator
thermostats) can be streamed onto the bus with one automation: see
[`examples/ha_stream_automation.yaml`](examples/ha_stream_automation.yaml).

## Development

The regulation brain is pure functions (`compute_inject`, `auto_regulate`,
`apply_regulation`) so it can be tested without a broker, web server or
network (heavy frameworks are stubbed automatically):

```bash
python3 tests/test_regulator.py    # or: pytest tests -q
```

## Security notes

- The web UI has **no authentication**: keep it LAN-only, never port-forward.
  Use a VPN (e.g. Tailscale/WireGuard) for remote access.
- `shadow` is the default mode. Nothing controls your heating until the
  downstream hardware (and you) say so.

## License

[MIT](LICENSE)
