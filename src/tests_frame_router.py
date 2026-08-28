import threading
import time
import unittest
from collections import deque
from unittest import mock

from chrome_manager import TabState
from frame_router import FrameRouter


def _make_tab_state(url, tab_id="t"):
    ts = TabState(tab_id=tab_id, url=url, tab=mock.Mock(), target_id="t",
                  frame_buffer=deque(maxlen=2000))
    ts.status = "running"
    return ts


def _frame(seq, url="u", cdp_ts=None):
    return {"timestamp": time.time(), "type": "webSocketFrameReceived",
            "url": url, "payload": f"payload-{seq}", "cdp_ts": cdp_ts}


def _raw_frame(payload, url, cdp_ts=None):
    return {"timestamp": time.time(), "type": "webSocketFrameReceived",
            "url": url, "payload": payload, "cdp_ts": cdp_ts}


class TestFrameRouter(unittest.TestCase):
    def setUp(self):
        # FrameRouter delegates drain to the manager; we use a lightweight stub.
        class FakeMgr:
            def __init__(self):
                self.tabs = {}          # tab_id -> TabState (id-keyed, like real mgr)
                self.url_index = {}     # url -> primary tab_id
            def get_tab(self, url):
                tid = self.url_index.get(url)
                if tid and tid in self.tabs:
                    return self.tabs[tid]
                for ts in self.tabs.values():
                    if ts.url == url:
                        return ts
                return None
            def drain_tab(self, url):
                ts = self.get_tab(url)
                if not ts:
                    return []
                with ts.lock:
                    f = list(ts.frame_buffer)
                    ts.frame_buffer.clear()
                return f
            def swap_primary(self, url, old_id, new_id):
                if self.url_index.get(url) == old_id:
                    self.url_index[url] = new_id
                    return True
                return False
            def retire_tab_id(self, tab_id):
                for url, cur in list(self.url_index.items()):
                    if cur == tab_id:
                        del self.url_index[url]
                self.tabs.pop(tab_id, None)
        self.mgr = FakeMgr()
        self.router = FrameRouter(self.mgr)

    def _register(self, ts):
        # Mirror real ChromeManager.create_tab semantics: the FIRST tab for a url
        # becomes its primary (indexed); later tabs for the SAME url (shadows
        # used for zero-drop handoff) are registered in .tabs but NOT indexed.
        self.mgr.tabs[ts.tab_id] = ts
        if ts.url not in self.mgr.url_index:
            self.mgr.url_index[ts.url] = ts.tab_id

    def test_drain_returns_and_clears(self):
        ts = _make_tab_state("u", "t1")
        for i in range(5):
            ts.frame_buffer.append(_frame(i))
        self._register(ts)
        self.assertEqual(len(self.router.drain("u")), 5)
        self.assertEqual(len(self.router.drain("u")), 0)

    def test_handoff_dedups_overlapping_frames(self):
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        # Old captured frames 0..9; new captured the SAME 8,9 (overlap by payload)
        # plus 10,11.  Total unique = 12 (0..11).
        # Overlapping frames carry close CDP timestamps (same Chrome) so the
        # byte-identical-payload duplicates are recognized and dropped.
        cdp0 = 1000.0  # both tabs share the Chrome's CDP MonotonicTime clock
        for i in range(10):
            old.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
        for i in range(8, 12):
            new.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
        self._register(old)
        self._register(new)  # new is a SHADOW (same url, distinct tab_id)
        self.router.handoff("u", old, new)
        drained = self.router.drain("u")
        payloads = [f["payload"] for f in drained]
        self.assertEqual(len(set(payloads)), 12)  # no duplicates
        self.assertEqual(len(payloads), 12)
        # new promoted (primary now t2), old closed
        self.assertEqual(self.mgr.url_index["u"], "t2")
        self.assertNotIn("t1", self.mgr.tabs)
        self.assertEqual(new.status, "running")
        self.assertEqual(old.status, "retiring")

    def test_handoff_disjoint_keeps_all(self):
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        for i in range(5):
            old.frame_buffer.append(_frame(i))
        for i in range(100, 105):
            new.frame_buffer.append(_frame(i))
        self._register(old)
        self._register(new)
        self.router.handoff("u", old, new)
        drained = self.router.drain("u")
        self.assertEqual(len(drained), 10)

    def test_handoff_recovers_old_tab_tail_frames_after_swap(self):
        # The old tab captures MORE frames between its buffer-clear (during the
        # merge) and the async close completing. Those tail frames must be
        # recovered into the new primary, not silently retired with the old tab.
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        cdp0 = 1000.0
        for i in range(5):
            old.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
        for i in range(5, 9):
            new.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
        self._register(old)
        self._register(new)

        # Simulate the old tab still receiving frames while being closed: inject
        # them just after swap_primary() re-points the url (i.e. post-merge, about
        # when drain_tab(url) used to run). Some overlap what the new tab already
        # has (6,7,8) -> must be deduped; 9,10 are genuinely new -> must be kept.
        real_before = self.mgr.__class__.swap_primary
        def swap_and_capture(inst, url, old_id, new_id):
            ret = real_before(inst, url, old_id, new_id)
            old_shadow = inst.tabs[old_id]
            for i in range(6, 11):
                old_shadow.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
            return ret
        self.mgr.__class__.swap_primary = swap_and_capture
        try:
            self.router.handoff("u", old, new)
        finally:
            self.mgr.__class__.swap_primary = real_before

        drained = self.router.drain("u")
        payloads = [f["payload"] for f in drained]
        # Unique messages 0..10 -> 11 unique frames, no dupes (incl. the tail).
        self.assertEqual(len(set(payloads)), 11)
        self.assertEqual(len(payloads), 11)
        # Tail frames 9,10 (post-merge) are present; 6,7,8 were deduped.
        self.assertIn("payload-9", payloads)
        self.assertIn("payload-10", payloads)
        self.assertEqual(payloads.count("payload-9"), 1)
        self.assertEqual(payloads.count("payload-10"), 1)
        # Old tab buffer fully consumed before retirement (nothing leaked).
        self.assertEqual(len(old.frame_buffer), 0)

    def test_handoff_recovery_does_not_noop_drain_new_tab(self):
        # Regression: the re-drain must read the OLD tab (the url now points at
        # the NEW tab). drain_tab on the new primary would re-clear the freshly
        # merged buffer and re-leak the same frames to a later drain.
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        cdp0 = 3000.0
        for i in range(3):
            old.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
            new.frame_buffer.append(_frame(i, cdp_ts=cdp0 + i))
        self._register(old)
        self._register(new)

        calls = {"drain_tab": 0}
        original_drain = self.mgr.drain_tab
        def counting_drain(inst, url):
            calls["drain_tab"] += 1
            return original_drain(inst, url)
        self.mgr.drain_tab = counting_drain.__get__(self.mgr)
        try:
            self.router.handoff("u", old, new)
            # handoff itself must NOT drain the url (would hit the NEW primary
            # and no-op-re-clear it); recovery reads the OLD tab directly.
            self.assertEqual(calls["drain_tab"], 0)
        finally:
            self.mgr.drain_tab = original_drain

        # The post-close recovery must NOT have cleared the new primary's merged
        # buffer: draining once returns all unique frames, draining again returns 0.
        first = self.router.drain("u")
        self.assertEqual(len(first), 3)
        self.assertEqual(len(self.router.drain("u")), 0)

    def test_handoff_stale_guard_when_swap_primary_reports_false(self):
        # A concurrent restart/recover re-pointed the url to a FRESH primary
        # (t3), so swap_primary(url, old=t1, new=t2) returns False. handoff must
        # retire the shadow (t2) and NOT retire old (t1) nor re-point the url.
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        fresh = _make_tab_state("u", "t3")
        for i in range(4):
            old.frame_buffer.append(_frame(i))
        for i in range(2, 6):
            new.frame_buffer.append(_frame(i))
        # Register all three (old becomes primary); then simulate the concurrent
        # restart/recover re-pointing the url to the FRESH primary (t3).
        self._register(old)   # u -> t1 (primary)
        self._register(new)   # tabs only (shadow)
        self._register(fresh) # tabs only (shadow)
        self.mgr.url_index["u"] = "t3"   # concurrent re-point
        self.assertEqual(self.mgr.url_index["u"], "t3")

        calls = {"retired": []}
        original_retire = self.mgr.retire_tab_id.__func__
        def recording_retire(inst, tab_id):
            calls["retired"].append(tab_id)
            return original_retire(inst, tab_id)
        self.mgr.retire_tab_id = recording_retire.__get__(self.mgr)

        self.router.handoff("u", old, new)

        # url still points at the fresh primary; old NOT retired; shadow retired.
        self.assertEqual(self.mgr.url_index["u"], "t3")
        self.assertIn("t1", self.mgr.tabs)     # old kept (not retired)
        self.assertIn("t3", self.mgr.tabs)     # fresh primary kept
        self.assertNotIn("t2", self.mgr.tabs)  # shadow retired
        # Ensure old was NOT retired by the handoff itself.
        self.assertNotIn("t1", calls["retired"])
        self.assertIn("t2", calls["retired"])
        # old buffer untouched by re-drain (its tail is not re-merged when stale).
        self.assertEqual(len(old.frame_buffer), 4)

    # ---- Zero-drop / dedup on REAL message shapes ----

    def test_mevx_jsonrpc_id_dedups_overlap(self):
        # Same UUID id delivered to both tabs -> same message -> dedup.
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        payload = ('{"jsonrpc":"2.0","id":"11111111-2222-3333-4444-555555555555",'
                   '"result":{}}')
        old.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=1000.0))
        new.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=1001.5))
        self._register(old)
        self._register(new)
        self.router.handoff("u", old, new)
        self.assertEqual(len(self.router.drain("u")), 1)  # deduped

    def test_mevx_jsonrpc_distinct_ids_not_collapsed(self):
        # Two DIFFERENT real messages (different id) must BOTH be kept (zero-drop).
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        p1 = ('{"jsonrpc":"2.0","id":"aaaa","result":{}}')
        p2 = ('{"jsonrpc":"2.0","id":"bbbb","result":{}}')
        old.frame_buffer.append(_raw_frame(p1, "u", cdp_ts=1000.0))
        new.frame_buffer.append(_raw_frame(p2, "u", cdp_ts=1001.5))
        self._register(old)
        self._register(new)
        self.router.handoff("u", old, new)
        self.assertEqual(len(self.router.drain("u")), 2)  # both distinct

    def test_mevx_flash_snapshot_dedup_same_pool(self):
        # subscribeFlashPool notification has NO id; same pool+createdAt within
        # the CDP window is treated as the re-delivered snapshot.
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        payload = ('{"jsonrpc":"2.0","method":"subscribeFlashPool",'
                   '"params":{"poolAddress":"pump111","createdAt":1720000000}}')
        old.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=2000.0))
        new.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=2001.0))
        self._register(old)
        self._register(new)
        self.router.handoff("u", old, new)
        self.assertEqual(len(self.router.drain("u")), 1)

    def test_merge_never_trims_unique_old_frames_for_dupes(self):
        # Regression: when the old buffer is FULL and the shadow re-delivers only
        # duplicates of the old tail (the normal CDP-overlap case), unique old
        # frames must NOT be evicted to make room for frames that are dropped as
        # duplicates (zero-drop guarantee). Reported by quorum review.
        router = FrameRouter(None)
        old = [_frame(i, cdp_ts=1000.0 + i) for i in range(2000)]          # full
        new = [_frame(i, cdp_ts=1000.0 + i) for i in range(1900, 2000)]    # dupes
        res = router._merge_dedup(old, new, maxlen=2000)
        self.assertEqual(len(res), 2000)           # nothing unique lost
        payloads = [f["payload"] for f in res]
        self.assertEqual(len(set(payloads)), 2000)  # all 2000 unique intact

    def test_merge_still_trims_when_genuinely_over_cap(self):
        # When there ARE genuinely new frames past the cap, the merged result is
        # trimmed to maxlen keeping the NEWEST frames (bounded-buffer behavior).
        router = FrameRouter(None)
        old = [_frame(i, cdp_ts=1000.0 + i) for i in range(10)]
        new = [_frame(i, cdp_ts=2000.0 + i) for i in range(10, 20)]        # real news
        res = router._merge_dedup(old, new, maxlen=10)
        self.assertEqual(len(res), 10)              # capped
        self.assertEqual(res[-1]["payload"], "payload-19")  # newest tail kept

    def test_identical_payload_far_in_time_kept(self):
        # Keyless frames (e.g. repeated coin_price) with the SAME payload but
        # CDP timestamps far apart are DIFFERENT messages -> keep both (zero-drop).
        old = _make_tab_state("u", "t1")
        new = _make_tab_state("u", "t2")
        payload = '{"channel":"chain_stat","t":"coin_price","p":"1.00"}'
        old.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=1000.0))
        new.frame_buffer.append(_raw_frame(payload, "u", cdp_ts=9999.0))
        self._register(old)
        self._register(new)
        self.router.handoff("u", old, new)
        self.assertEqual(len(self.router.drain("u")), 2)

    # ---- tab_status contract mapping ----

    def test_tab_status_maps_internal_to_legacy(self):
        # "starting" | "warming" | "handoff" -> "starting"
        for s in ("starting", "warming", "handoff"):
            ts = _make_tab_state("u", "t1")
            ts.status = s
            self._register(ts)
            self.assertEqual(self.router.tab_status("u"), "starting")
            self.mgr.tabs.clear()
            self.mgr.url_index.clear()

        # "running" -> "running"
        ts = _make_tab_state("u", "t1")
        ts.status = "running"
        self._register(ts)
        self.assertEqual(self.router.tab_status("u"), "running")
        self.mgr.tabs.clear()
        self.mgr.url_index.clear()

        # "retiring" -> "unhealthy"
        ts = _make_tab_state("u", "t1")
        ts.status = "retiring"
        self._register(ts)
        self.assertEqual(self.router.tab_status("u"), "unhealthy")
        self.mgr.tabs.clear()
        self.mgr.url_index.clear()

        # unknown -> "failed"
        ts = _make_tab_state("u", "t1")
        ts.status = "unknown"
        self._register(ts)
        self.assertEqual(self.router.tab_status("u"), "failed")

    def test_tab_status_no_tab_returns_failed(self):
        self.assertEqual(self.router.tab_status("nonexistent"), "failed")
