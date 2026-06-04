const V = 'swissviz-v2';

const PRECACHE = [
    './',
    './index.html',
    './data/BS.json',
    './data/BS_0.json.gz',
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
