const CACHE_NAME = 'redspace-v3';
const CALL_TAG = 'redspace-call';

// ── Lifecycle ──────────────────────────────────────────────────────────────
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});

// ── Push Notifications ─────────────────────────────────────────────────────
self.addEventListener('push', e => {
  if (!e.data) return;
  let data = {};
  try { data = e.data.json(); } catch (_) { data = { title: 'REDSPACE', body: e.data.text() }; }

  const isCall = data.type === 'call';

  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
      // If the app is open and visible, the user can already see the message via socket.
      // Suppress non-call notifications — calls always ring regardless.
      const appVisible = clients.some(c => c.visibilityState === 'visible');
      if (appVisible && !isCall) return;

      const options = isCall ? {
        body: data.body || 'Chamada recebida',
        icon: '/icons/icon-192.png',
        badge: '/icons/icon-192.png',
        tag: CALL_TAG,
        renotify: true,
        requireInteraction: true,
        vibrate: [500, 200, 500, 200, 500],
        actions: [{ action: 'open', title: '📞 Abrir' }],
        data: { url: data.url || '/', type: 'call', from: data.from || '' },
      } : {
        body: data.body || 'Nova mensagem',
        icon: '/icons/icon-192.png',
        badge: '/icons/icon-192.png',
        tag: data.tag || 'redspace-msg',
        renotify: true,
        silent: false,
        vibrate: [120, 60, 120],
        data: { url: data.url || '/', type: data.type || 'message', tag: data.tag },
      };

      return self.registration.showNotification(data.title || 'REDSPACE', options);
    })
  );
});

// ── Notification Click ─────────────────────────────────────────────────────
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = e.notification.data?.url || '/';

  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
      const existing = clients.find(c => c.url.includes(self.location.origin));
      if (existing) {
        existing.focus();
        existing.postMessage({ type: 'notif_click', url, notifType: e.notification.data?.type, tag: e.notification.data?.tag });
      } else {
        self.clients.openWindow(url);
      }
    })
  );
});

// ── Subscription Refresh ───────────────────────────────────────────────────
// The browser rotates push keys periodically. When this happens we must save
// the new subscription to the server *directly from the SW* — we cannot rely
// on a client tab being open to relay the message.
self.addEventListener('pushsubscriptionchange', e => {
  const oldEndpoint = e.oldSubscription ? e.oldSubscription.endpoint : null;
  e.waitUntil(
    self.registration.pushManager.subscribe(e.oldSubscription
      ? { userVisibleOnly: true, applicationServerKey: e.oldSubscription.options.applicationServerKey }
      : { userVisibleOnly: true }
    ).then(async sub => {
      // 1. Save directly to server (works even when app is closed)
      if (oldEndpoint) {
        await fetch('/api/push/resubscribe', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ oldEndpoint, subscription: sub.toJSON() })
        }).catch(() => {});
      }
      // 2. Also notify any open tabs so they can update their local state
      const clients = await self.clients.matchAll({ type: 'window' });
      clients.forEach(c => c.postMessage({ type: 'push_resubscribe', subscription: sub.toJSON() }));
    }).catch(() => {})
  );
});
