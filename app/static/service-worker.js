const CACHE_VERSION = "vaultpack-pwa-v8";
const STATIC_CACHE = `${CACHE_VERSION}-static`;

const STATIC_ASSETS = [
  "/static/style.css",
  "/static/favicon.png",
  "/static/favicon-dark.png",
  "/static/vaultpack-favicon.svg",
  "/static/vaultpack-favicon-dark.svg",
  "/static/vaultpack-icon.svg",
  "/static/vaultpack-icon-dark.svg",
  "/static/vaultpack-icon-adaptive.svg",
  "/static/vaultpack-icon-180.png",
  "/static/vaultpack-icon-192.png",
  "/static/vaultpack-icon-512.png",
  "/static/vaultpack-icon-dark-180.png",
  "/static/vaultpack-icon-dark-192.png",
  "/static/vaultpack-icon-dark-512.png",
  "/static/vaultpack-icon-adaptive-180.png",
  "/static/vaultpack-icon-adaptive-192.png",
  "/static/vaultpack-icon-adaptive-512.png",
  "/static/apple-touch-icon.png",
  "/static/icon-192.png",
  "/static/icon-512.png",
  "/static/manifest.webmanifest"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys
        .filter((key) => key.startsWith("vaultpack-pwa-") && key !== STATIC_CACHE)
        .map((key) => caches.delete(key))
    ))
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  if (url.pathname === "/static/style.css") {
    event.respondWith(networkFirst(request));
    return;
  }

  if (url.pathname === "/static/manifest.webmanifest" || url.pathname === "/manifest.webmanifest") {
    event.respondWith(networkFirst(request));
    return;
  }

  if (url.pathname.startsWith("/static/")) {
    event.respondWith(cacheFirst(request));
    return;
  }

  if (request.mode === "navigate") {
    event.respondWith(networkOnlyPage(request));
  }
});

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;

  const response = await fetch(request);
  if (response.ok) {
    const cache = await caches.open(STATIC_CACHE);
    cache.put(request, response.clone());
  }
  return response;
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(STATIC_CACHE);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    throw new Error("Network request failed and no cached response is available.");
  }
}

async function networkOnlyPage(request) {
  try {
    return await fetch(request);
  } catch {
    return new Response(
      "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>vaultpack 离线</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0d1118;color:#eef4fb;font:16px system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}.card{max-width:360px;padding:24px;border:1px solid #2c3747;border-radius:16px;background:#151c28;box-shadow:0 18px 44px rgba(0,0,0,.24)}h1{margin:0 0 10px;font-size:18px}p{margin:0;color:#9da8b8;line-height:1.6}</style></head><body><section class=\"card\"><h1>当前离线</h1><p>vaultpack 需要连接到管理面板才能查看任务和运行记录。网络恢复后请重新打开页面。</p></section></body></html>",
      {
        status: 503,
        headers: { "Content-Type": "text/html; charset=utf-8" }
      }
    );
  }
}
