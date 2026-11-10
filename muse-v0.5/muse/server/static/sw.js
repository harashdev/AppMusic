/* Service worker tối giản: mở được giao diện khi mất mạng (dữ liệu danh sách phát lấy từ bộ nhớ cục bộ). */
const CACHE = 'muse-shell-v1';
self.addEventListener('install', e => { e.waitUntil(caches.open(CACHE).then(c => c.add('/')).catch(() => {})); self.skipWaiting(); });
self.addEventListener('activate', e => { e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim())); });
self.addEventListener('fetch', e => {
  const r = e.request;
  if (r.method !== 'GET' || r.mode !== 'navigate') return;       // chỉ xử lý việc mở trang
  e.respondWith(fetch(r).then(res => { const cp = res.clone(); caches.open(CACHE).then(c => c.put('/', cp)).catch(() => {}); return res; })
    .catch(() => caches.match('/').then(x => x || Response.error())));
});
