import hashlib
import json
import logging
import threading
import time
from typing import List, Optional, Tuple

import metrics as m

logger = logging.getLogger(__name__)

# Cross-tab "same message" window. Both tabs in the SAME Chrome share the CDP
# MonotonicTime, so the same WS frame delivered to old+new tab carries near-equal
# cdp_ts. Wall-clock (time.time()) is NOT shared and must not be used for identity.
CDP_OVERLAP_WINDOW_S = 2.0

# Cross-process "same message" window. Dual-Chrome instances have distinct CDP
# monotonic base times, so cross-process deduplication uses wall-clock arrival
# timestamps (time.time()).
CROSS_PROCESS_OVERLAP_WINDOW_S = 3.0

# Domain-independent marker for "payload has no per-message unique field".
_NO_KEY = object()


def _try_json(payload: str):
    if not isinstance(payload, str):
        return None
    try:
        return json.loads(payload)
    except Exception:
        return None


def extract_semantic_messages(payload: str) -> List[Tuple[str, str, bool]]:
    """Granular protocol-aware batch unpacking & semantic message extraction.

    Returns a list of (key, normalized_payload, is_data) tuples:
      - key: Domain-level unique message identifier (or normalized payload for keyless).
      - normalized_payload: Normalized string representation of the semantic message.
      - is_data: True for trade/pool data; False for control/heartbeat/ping frames.
    """
    if not isinstance(payload, str) or not payload.strip():
        return []

    lines = [line.strip() for line in payload.split("\n") if line.strip()]
    if not lines:
        return []

    results: List[Tuple[str, str, bool]] = []

    for line in lines:
        obj = _try_json(line)
        if obj is None:
            # Fallback / non-JSON string
            is_ping_pong = line.lower() in ("ping", "pong", "2", "3")
            results.append((line, line, not is_ping_pong))
            continue

        if isinstance(obj, dict):
            # ---- 1. MevX / JSON-RPC ----
            # subscribeFlashPool notification
            if obj.get("method") == "subscribeFlashPool":
                params = obj.get("params") or {}
                pool = params.get("poolAddress", "")
                created = params.get("createdAt", "")
                h = hashlib.sha256(line.encode("utf-8")).hexdigest()[:8]
                key = f"flash:{pool}:{created}:{h}"
                results.append((key, line, True))
                continue

            # JSON-RPC requests/responses with 'id'
            if obj.get("jsonrpc") == "2.0" and obj.get("id") is not None:
                msg_id = obj["id"]
                key = f"jsonrpc:id:{msg_id}"
                is_ping_pong = (
                    obj.get("method") in ("ping", "pong")
                    or obj.get("result") in ("pong", "ping", "PONG", "PING")
                )
                results.append((key, line, not is_ping_pong))
                continue

            # ---- 2. GMGN Protocol ----
            action = obj.get("action")
            if action in ("heartbeat", "ping", "pong"):
                key = f"gmgn:heartbeat:{action}"
                results.append((key, line, False))
                continue

            if obj.get("ping") is not None:
                results.append((f"gmgn:ping:{obj['ping']}", line, False))
                continue
            if obj.get("pong") is not None:
                results.append((f"gmgn:pong:{obj['pong']}", line, False))
                continue

            t = obj.get("t")
            channel = obj.get("channel")

            if channel in ("major_coin_price", "coin_price") or t in ("coin_price", "major_coin_price"):
                results.append(("gmgn:major_coin_price", line, False))
                continue

            if channel == "chain_stat" or t == "chain_stat":
                results.append(("gmgn:chain_stat", line, False))
                continue

            # public_broadcast array decomposition
            if channel == "public_broadcast":
                data = obj.get("data")
                if isinstance(data, list) and len(data) > 0:
                    for item in data:
                        if isinstance(item, dict):
                            ed = item.get("ed") if isinstance(item.get("ed"), dict) else {}
                            sig_id = ed.get("sig_id")
                            tw_id = ed.get("id") if item.get("et") == "twitter_watched" else None
                            if sig_id is not None:
                                key = f"gmgn:sig:{sig_id}"
                            elif tw_id is not None:
                                key = f"gmgn:tw:{tw_id}"
                            else:
                                item_json = json.dumps(item, separators=(',', ':'))
                                key = f"gmgn:item:{hashlib.sha256(item_json.encode('utf-8')).hexdigest()[:8]}"
                            norm_item = json.dumps(item, separators=(',', ':'))
                            results.append((key, norm_item, True))
                        else:
                            results.append((f"gmgn:raw:{item}", str(item), True))
                elif isinstance(data, list) and len(data) == 0:
                    results.append(("gmgn:public_broadcast:empty", line, False))
                else:
                    results.append((line, line, True))
                continue

            # callout_global
            if t == "callout_global" or channel == "callout_global":
                data = obj.get("data")
                uids = []
                if isinstance(data, list):
                    uids = [str(d.get("uid")) for d in data if isinstance(d, dict) and d.get("uid") is not None]
                key = f"gmgn:callout:{':'.join(uids)}" if uids else "gmgn:callout"
                results.append((key, line, True))
                continue

            # route_info
            if t == "route_info" or channel == "route_info":
                d = obj.get("d")
                counter = ""
                if isinstance(d, dict) and d:
                    counter = str(next(iter(d.values())))
                key = f"gmgn:route:{counter}" if counter else "gmgn:route"
                results.append((key, line, False))
                continue

            # Other GMGN channels (e.g. token_social_info) -> data frame
            if channel:
                key = f"gmgn:{channel}"
                results.append((key, line, True))
                continue

            # Fallback dict
            is_ping_pong = obj.get("method") in ("ping", "pong") or obj.get("result") in ("pong", "ping")
            results.append((line, line, not is_ping_pong))
            continue

        elif isinstance(obj, list):
            for sub_obj in obj:
                sub_str = json.dumps(sub_obj, separators=(',', ':'))
                sub_extracted = extract_semantic_messages(sub_str)
                results.extend(sub_extracted)
            continue

        else:
            results.append((line, line, True))

    return results


def _msg_key_from_json(url: str, obj) -> str:
    """Legacy helper preserved for backward compatibility."""
    if not isinstance(obj, dict):
        return _NO_KEY

    if obj.get("jsonrpc") == "2.0" and obj.get("id") is not None:
        return f"jsonrpc:id:{obj['id']}"

    if obj.get("method"):
        params = obj.get("params")
        if isinstance(params, dict):
            pool = params.get("poolAddress")
            created = params.get("createdAt")
            if pool is not None and created is not None:
                h = hashlib.sha256(json.dumps(obj).encode("utf-8")).hexdigest()[:8]
                return f"flash:{pool}:{created}:{h}"

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
            k = next(iter(d.values()), None)
            if k is not None:
                return f"gmgn:route:{k}"

    return _NO_KEY


def _message_key(frame: dict):
    """Best-effort unique key for a frame, or tuple of keys for batch messages."""
    payload = frame.get("payload", "")
    extracted = extract_semantic_messages(payload)
    if not extracted:
        return (payload,)
    if len(extracted) == 1:
        return (extracted[0][0],)
    return tuple(m[0] for m in extracted)


def _same_message(a: dict, b: dict) -> bool:
    """True if a and b are the SAME underlying WS message (intra-process duplicate)."""
    ka, kb = _message_key(a), _message_key(b)
    if ka != kb:
        return False
    ca, cb = a.get("cdp_ts"), b.get("cdp_ts")
    if ca is not None and cb is not None:
        if abs(ca - cb) <= CDP_OVERLAP_WINDOW_S:
            return True
        return False
    return False


def _same_message_cross_process(a: dict, b: dict) -> bool:
    """True if a and b are the SAME underlying WS message (cross-process duplicate).

    Dual-Chrome instances have distinct CDP monotonic clocks, so cross-process
    deduplication compares extracted semantic keys (or payload equality for keyless)
    combined with wall-clock arrival timestamp proximity (<= 3.0s).
    """
    ka, kb = _message_key(a), _message_key(b)
    if ka != kb:
        return False
    ta, tb = a.get("timestamp"), b.get("timestamp")
    if ta is not None and tb is not None:
        if abs(ta - tb) <= CROSS_PROCESS_OVERLAP_WINDOW_S:
            return True
        return False
    return False


def _merge_dedup(old_frames, new_frames, maxlen=2000, cross_process=False):
    """Merge old+new preserving order, dropping NEW frames that duplicate an
    OLD frame (or prior new frame). Dedup via _same_message (intra-process,
    CDP monotonic proximity <= 2.0s) or _same_message_cross_process (cross-process,
    wall-clock arrival proximity <= 3.0s).
    Returns merged list trimmed to maxlen (keeping newest frames).

    Zero-drop guarantee: old unique frames are NEVER evicted to make room for
    new frames that turn out to be duplicates. Dedup first (dropping only
    genuine duplicates), then trim the merged result to maxlen only if it
    still exceeds the cap (which requires genuinely-new frames)."""
    same_fn = _same_message_cross_process if cross_process else _same_message
    result = list(old_frames)
    seen = {_message_key(f): f for f in old_frames}
    for f in new_frames:
        k = _message_key(f)
        prior = seen.get(k)
        if prior is not None and same_fn(prior, f):
            continue          # duplicate of a prior frame -> drop
        seen[k] = f
        result.append(f)
    if len(result) > maxlen:
        result = result[-maxlen:]
    return result


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

    def _merge_dedup(self, old_frames, new_frames, maxlen=2000, cross_process=False):
        """Merge old+new preserving order, dropping NEW frames that duplicate an
        OLD frame (or prior new frame). Dedup via _same_message (intra-process,
        CDP monotonic proximity <= 2.0s) or _same_message_cross_process (cross-process,
        wall-clock arrival proximity <= 3.0s).
        Returns merged list trimmed to maxlen (keeping newest frames)."""
        return _merge_dedup(old_frames, new_frames, maxlen=maxlen, cross_process=cross_process)

    def handoff(self, url: str, old_tab, new_tab, cross_process: bool = False):
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
                    result = self._merge_dedup(old_frames, new_frames, maxlen=maxlen, cross_process=cross_process)

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
                        cross_process=cross_process,
                    )
                    new_tab.frame_buffer.clear()
                    new_tab.frame_buffer.extend(merged)
            return "success"
