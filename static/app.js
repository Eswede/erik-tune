/* Erik Tune v0.20 - tyst live-uppdatering av dashboarden.
 * Pollar /api/state var 15:e sekund och skriver om hero-värden och
 * statusrutor på plats: ingen sidomladdning, ingen blink, scrollen står kvar.
 * Registrerar även service workern (fungerar bara över HTTPS/localhost;
 * på HTTP i LAN misslyckas det tyst och gränssnittet kör som vanlig sida). */
(function () {
  'use strict';

  try {
    if ('serviceWorker' in navigator &&
        (location.protocol === 'https:' || ['localhost', '127.0.0.1']
          .indexOf(location.hostname) >= 0)) {
      navigator.serviceWorker.register('/static/sw.js').catch(function () {});
    }
  } catch (e) { /* ignoreera: SW är en förbättring, inget krav */ }

  function set(id, text) {
    var el = document.getElementById(id);
    if (el && text !== undefined && text !== null) el.textContent = text;
  }

  function tick() {
    fetch('/api/state', {cache: 'no-store'})
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        set('v-ar', d.ar);
        set('v-ute', d.ute);
        set('v-inject', d.inject + ' \u00b0C');
        set('v-influence', d.influence + ' \u00b0C');
        var dot = document.getElementById('d-mqtt');
        if (dot) dot.className = 'dot ' + (d.mqtt_ok ? 'ok' : 'warn');
      })
      .catch(function () { /* nätverk borta: behåll gamla värden */ });
  }

  setInterval(tick, 15000);
  tick();
})();
