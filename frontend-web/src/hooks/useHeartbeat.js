/**
 * hooks/useHeartbeat.js
 * ─────────────────────
 * Smart keep-alive heartbeat for Render free-tier services.
 *
 * Strategy:
 *  - Track user activity (mouse, click, keydown) via window event listeners.
 *  - Every HEARTBEAT_INTERVAL ms, check if activity occurred in the last
 *    HEARTBEAT_INTERVAL ms.  If yes → fire a lightweight /ping to both
 *    backend and parser_backend to prevent Render's 15-minute sleep.
 *  - If the user is idle, skip the ping entirely — no wasted compute hours.
 *
 * Mount this hook once at the App root (inside <App> after authentication).
 */

import { useEffect, useRef } from 'react';

const HEARTBEAT_INTERVAL = 5 * 60 * 1000; // 5 minutes

const BACKEND_URL   = import.meta.env.VITE_API_BASE_URL   || 'http://localhost:3000';
const PARSER_URL    = import.meta.env.VITE_PARSER_API_URL || 'http://localhost:8000';

async function ping(url) {
  try {
    await fetch(`${url}/ping`, {
      method: 'GET',
      // Use 'no-cors' so the browser doesn't block the preflight on a
      // cross-origin /ping that returns no special CORS headers.
      // We don't need to read the response body.
      mode: 'no-cors',
      cache: 'no-store',
    });
  } catch {
    // Silently swallow — a sleeping service may reject the very first
    // request as it wakes up; subsequent ones will succeed.
  }
}

export function useHeartbeat() {
  const lastActivityRef = useRef(Date.now());

  useEffect(() => {
    // ── Activity tracker ─────────────────────────────────────
    const markActivity = () => {
      lastActivityRef.current = Date.now();
    };

    const events = ['mousemove', 'mousedown', 'keydown', 'touchstart', 'scroll'];
    events.forEach(e => window.addEventListener(e, markActivity, { passive: true }));

    // ── Heartbeat interval ───────────────────────────────────
    const timer = setInterval(() => {
      const idleSince = Date.now() - lastActivityRef.current;
      if (idleSince < HEARTBEAT_INTERVAL) {
        // User was active within the last window — ping both services.
        ping(BACKEND_URL);
        ping(PARSER_URL);
      }
      // else: user is idle, skip ping — save free-tier compute hours.
    }, HEARTBEAT_INTERVAL);

    return () => {
      events.forEach(e => window.removeEventListener(e, markActivity));
      clearInterval(timer);
    };
  }, []);
}
