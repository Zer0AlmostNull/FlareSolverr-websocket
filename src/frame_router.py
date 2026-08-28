import json
import logging
import threading
import time
from typing import List, Optional

import metrics as m

logger = logging.getLogger(__name__)

# Cross-tab "same message" window. Both tabs in the SAME Chrome share the CDP
# MonotonicTime, so the same WS frame delivered to old+new tab carries near-equal
# cdp_ts. Wall-clock (time.time()) is NOT shared and must not be used for identity.
CDP_OVERLAP_WINDOW_S = 2.0

# Domain-independent marker for "payload has no per-message unique field".
_NO_KEY = object()


def _try_json(payload: str):
    if not isinstance(payload, str):
        return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def _msg_key_from_json(url: str, obj) -> str:
    """Extract a per-message unique key by parsing the payload JSON, branching
    on the message envelope so we never rely on repetition-prone snapshot fields.

    Domain/URL is read from the frame's url so the same extractor works for any
    site. Returns _NO_KEY when no reliable per-message field exists (caller then
    falls back to byte-identical payload + CDP-ts proximity)."""
    if not isinstance(obj, dict):
        return _NO_KEY

    # ---- JSON-RPC (mevx.io): an 'id' UUID on responses/pongs is unique ----
    if obj.get("jsonrpc") == "2.0" and obj.get("id") is not None:
        return f"jsonrpc:id:{obj['id']}"

    # ---- mevx.io 'subscribeFlashPool' notification: snapshot, no id ----
    # Key = method + poolAddress + createdAt (stable per token). We only treat
    # two frames as "same" when these match AND cdp_ts is within the window AND
    # payloads are byte-identical (handled by caller). This never invents an id
    # and never collapses distinct tokens.
    if obj.get("method"):
        params = obj.get("params")
        if isinstance(params, dict):
            pool = params.get("poolAddress")
            created = params.get("createdAt")
            if pool is not None and created is not None:
                return f"flash:{obj['method']}:{pool}:{created}"

    # ---- gmgn.ai channel envelopes ----
    channel = obj.get("channel")
    if channel == "public_broadcast":
        for item in (obj.get("data") or []):
            if not isinstance(item, dict):
                continue
            ed = item.get("ed")
            if isinstance(ed, dict) and ed.get("sig_id"):
                return f"gmgn:sig:{ed['sig_id']}"
            if item.get("et") == "twitter_watched":
                ed = item.get("ed")
                if isinstance(ed, dict) and ed.get("id"):
                    return f"gmgn:tw:{ed['id']}"
    if obj.get("t") == "callout_global":
        uids = [d.get("uid") for d in (obj.get("data") or []) if isinstance(d, dict) and d.get("uid")]
        if uids:
            return f"gmgn:callout:{':'.join(str(u) for u in uids)}"
    if obj.get("t") == "route_info":
        d = obj.get("d")
        if isinstance(d, dict) and d:
            # value embeds a ms:seq monotonic counter -> unique-ish
            k = next(iter(d.values()), None)
            if k is not None:
                return f"gmgn:route:{k}"

    return _NO_KEY


def _message_key(frame: dict):
    """Best-effort unique key for a frame, or None if only payload matters."""
    url = frame.get("url", "")
    payload = frame.get("payload", "")
    obj = _try_json(payload)
    if obj is not None:
        k = _msg_key_from_json(url, obj)
        if k is not _NO_KEY:
            return (k, payload)   # strong key: field-level (payload as tie-break)
    return (payload,)             # weak key: byte-identical payload only


def _same_message(a: dict, b: dict) -> bool:
    """True if a and b are the SAME underlying WS message (duplicate)."""
    ka, kb = _message_key(a), _message_key(b)
    if ka != kb:
        return False
    # Strong keys must also be CDP-temporally close to count as the SAME frame.
    ca, cb = a.get("cdp_ts"), b.get("cdp_ts")
    if ca is not None and cb is not None:
        if abs(ca - cb) <= CDP_OVERLAP_WINDOW_S:
            return True
        return False
    # CDP ts missing on at least one side (unit tests or partial capture).
    # NEVER collapse based on payload alone without CDP proximity —
    # identical payloads can be distinct messages (e.g. repeated coin_price ticks).
    # Return False to keep both frames (zero-drop); caller's merge logic
    # will dedup only when CDP timestamps are available.
    return False


class FrameRouter:
    def __init__(self, chrome_manager):
        self.mgr = chrome_manager
        self._url_locks: dict = {}
        self._lock = threading.Lock()

    def _get_url_lock(self, url: str) -> threading.Lock:
        with self._lock:
            if url not in self._url_locks:
                self._url_locks[url] = threading.Lock()
            return self._url_locks[url]

    def drain(self, url: str) -> List[dict]:
        return self.mgr.drain_tab(url) or []

    def get_last_frame_ts(self, url: str) -> Optional[float]:
        tab = self.mgr.get_tab(url)
        if not tab:
            return None
        return tab.last_frame_ts

    def tab_status(self, url: str) -> str:
        tab = self.mgr.get_tab(url)
        if not tab:
            return "failed"
        s = tab.status
        if s in ("starting", "warming", "handoff"):
            return "starting"
        if s == "running":
            return "running"
        if s == "retiring":
            return "unhealthy"  # tab exists but being replaced = stale
        return "failed"

    def _merge_dedup(self, old_frames, new_frames, maxlen=2000):
        """Merge old+new preserving order, dropping NEW frames that duplicate an
        OLD frame (the shadow re-delivers the tail of the old stream). Dedup via
        _same_message: field-level key when available, else payload+CDP window.
        Returns merged list trimmed to maxlen (keeping newest frames).

        Zero-drop guarantee: old unique frames are NEVER evicted to make room for
        new frames that turn out to be duplicates. Dedup first (dropping only
        genuine duplicates), then trim the merged result to maxlen only if it
        still exceeds the cap (which requires genuinely-new frames)."""
        result = list(old_frames)
        seen = {_message_key(f): f for f in old_frames}
        for f in new_frames:
            k = _message_key(f)
            prior = seen.get(k)
            if prior is not None and _same_message(prior, f):
                continue          # duplicate of an old frame -> drop
            seen[k] = f
            result.append(f)
        # Final trim to the hard cap (maxlen is a bounded buffer); only reached
        # when genuinely-new frames pushed the total past the cap.
        if len(result) > maxlen:
            result = result[-maxlen:]
        return result

    def handoff(self, url: str, old_tab, new_tab):
        """Zero-drop handoff: promote new_tab to primary, merge buffers without loss.

        Returns "success" after a normal handoff, or "stale" if a concurrent
        restart/recover re-pointed the url before we could (the shadow is then
        retired and no buffers are touched).

        Lock order (CRITICAL, must be globally consistent):
          url_lock -> old_tab.lock -> new_tab.lock
        Never call ChromeManager._lock while holding these tab locks in the same
        thread (avoids lock-order inversion deadlock)."""
        url_lock = self._get_url_lock(url)
        with url_lock:
            # CAS FIRST (under url_lock only, so we are NOT holding any tab lock
            # when calling into ChromeManager._lock): re-point the url to new_tab
            # ONLY if it still points at old_tab. Returns True if it did, or
            # False if a concurrent restart/recover already re-pointed the url
            # elsewhere (a stale handoff). Computing this up front means a stale
            # handoff aborts before mutating any buffer.
            swapped = self.mgr.swap_primary(url, old_tab.tab_id, new_tab.tab_id)
            if not swapped:
                # Concurrent restart/recover already re-pointed the url to a fresh
                # primary. old_tab is a stale pre-restart object that must NOT be
                # retired (retiring it would drop the url's fresh primary) and we
                # must NOT re-drain or recycle buffers. This shadow is no longer
                # the successor, so retire IT (it isn't primary) and abort cleanly.
                # Closes the recycle-mid-restart data-loss hole.
                logger.info(f"FrameRouter: handoff stale for {url} "
                            f"(concurrent re-point); retiring shadow {new_tab.tab_id}")
                self.mgr.retire_tab_id(new_tab.tab_id)
                m.WS_RECONNECT_TOTAL.labels(url=url, result="stale").inc()
                return "stale"
            with old_tab.lock:
                with new_tab.lock:
                    old_frames = list(old_tab.frame_buffer)
                    new_frames = list(new_tab.frame_buffer)
                    old_tab.frame_buffer.clear()
                    new_tab.frame_buffer.clear()

                    # Merge stale (old) frames with fresh (new) frames, dropping
                    # new frames that re-deliver the tail of the old stream.
                    # Dedup uses field-level keys when the payload has them
                    # (mevx jsonrpc id, gmgn channels), else byte-identical
                    # payload + CDP-ts/clock proximity. Order of old is preserved
                    # then new. Trim to new tab's buffer maxlen to avoid silent eviction.
                    maxlen = getattr(new_tab.frame_buffer, "maxlen", 2000)
                    result = self._merge_dedup(old_frames, new_frames, maxlen=maxlen)

                    new_tab.frame_buffer.extend(result)
                    new_tab.status = "running"
                    old_tab.status = "retiring"

            self.mgr.retire_tab_id(old_tab.tab_id)
            # Legacy metric: track handoff as a reconnect
            m.WS_RECONNECT_TOTAL.labels(url=url, result="handoff").inc()

            # Post-close re-drain: the old tab may have received frames between
            # clearing its buffer and the async close completing. The url now
            # points at the NEW tab, so drain_tab(url) would only re-clear/re-read
            # the new tab's buffer (a no-op that also double-counts metrics).
            # Recover the old tab's trailing frames directly and merge them into
            # the new primary, deduping so already-promoted frames are not re-leaked
            # (never re-counting already-fed frames).
            tail = []
            with old_tab.lock:                          # url_lock -> old.lock
                if old_tab.frame_buffer:
                    tail = list(old_tab.frame_buffer)
                    old_tab.frame_buffer.clear()
            if tail:
                with new_tab.lock:                      # -> new.lock (order preserved)
                    existing = list(new_tab.frame_buffer)
                    merged = self._merge_dedup(
                        existing, tail,
                        maxlen=getattr(new_tab.frame_buffer, "maxlen", 2000),
                    )
                    new_tab.frame_buffer.clear()
                    new_tab.frame_buffer.extend(merged)
            return "success"
