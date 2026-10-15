// scripts/build.js rewrites the name with the commit, so every deploy installs a
// new worker and drops the caches of the old one
const V = 'cantography-dev';

const PRECACHE = [
    './',
    './index.html',
    './data/BE.json',
    './data/BE_0.json.gz',
];

self.addEventListener('install', e => {
    self.skipWaiting();
    e.waitUntil(
        caches.open(V).then(c => c.addAll(PRECACHE).catch(() => {}))
    );
});

self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== V).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

self.addEventListener('fetch', e => {
    if (e.request.method !== 'GET') return;
    // Page: network-first so deploys show up immediately; cache is the offline fallback
    if (e.request.mode === 'navigate') {
        e.respondWith(
            fetch(e.request).then(res => {
                if (res.ok) {
                    const clone = res.clone();
                    caches.open(V).then(c => c.put(e.request, clone));
                }
                return res;
            }).catch(() => caches.match(e.request).then(hit => hit || caches.match('./index.html')))
        );
        return;
    }
    // Everything else, data included, is cache-first within a deploy: the cache is
    // named after the commit, so a new deploy fetches each file once again
    e.respondWith(
        caches.match(e.request).then(hit => {
            if (hit) return hit;
            return fetch(e.request).then(res => {
                if (res.ok && res.type !== 'opaque') {
                    const clone = res.clone();
                    caches.open(V).then(c => c.put(e.request, clone));
                }
                return res;
            }).catch(() => caches.match(e.request));
        })
    );
});
