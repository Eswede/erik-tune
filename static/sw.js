/* Erik Tune v0.20 - service worker.
 * Nätverk först (värden ska vara färska), cache som offline-fallbak.
 * Registreras bara över HTTPS (eller localhost); på HTTP i LAN misslycksas
 * registreringen tyst och gränssnittet kör som vanlig webbsida. */
var CACHE = 'erik-tune-v20';
var ASSETS = ['/', '/static/app.js', '/static/hero.svg',
              '/static/manifest.webmanifest', '/static/icon-192.png',
              '/static/icon-512.png'];

self.addEventListener('install', function (e) {
  e.waitUntil(caches.open(CACHE)
    .then(function (c) { return c.addAll(ASSETS); })
    .then(function () { return self.skipWaiting(); }));
});

self.addEventListener('activate', function (e) {
  e.waitUntil(caches.keys()
    .then(function (ks) {
      return Promise.all(ks.filter(function (k) {
        return k !== CACHE;
      }).map(function (k) { return caches.delete(k); }));
    })
    .then(function () { return self.clients.claim(); }));
});

self.addEventListener('fetch', function (e) {
  if (e.request.method !== 'GET') return;
  e.respondWith(
    fetch(e.request).then(function (resp) {
      if (resp.ok) {
        var copy = resp.clone();
        caches.open(CACHE)
          .then(function (c) { c.put(e.request, copy); })
          .catch(function () {});
      }
      return resp;
    }).catch(function () {
      return caches.match(e.request).then(function (m) { return m || caches.match('/'); });
    })
  );
});
