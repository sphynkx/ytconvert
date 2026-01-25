import json
import os
import threading
import time
import uuid

import redis

from utils.time_ut import now_ts
from utils.files_ut import ensure_dir, rm_tree
from utils.proc_ut import run_cmd_capture
from utils.hash_ut import sha256_file


STATE_QUEUED = 1
STATE_WAITING_UPLOAD = 2
STATE_RUNNING = 3
STATE_DONE = 4
STATE_FAILED = 5
STATE_CANCELED = 6


def new_job_id():
    return uuid.uuid4().hex


def _j(d):
    return json.dumps(d, ensure_ascii=True)


def _u(s, default=None):
    if s is None:
        return default
    if isinstance(s, (bytes, bytearray)):
        s = s.decode("utf-8", errors="replace")
    return s


def _i(s, default=0):
    try:
        return int(_u(s, default))
    except Exception:
        return default


def _f(s, default=0.0):
    try:
        return float(_u(s, default))
    except Exception:
        return default


def parse_variant_id(variant_id):
    """
    Fallback parser for ids like:
      v:1440p:h264+aac:mp4
      a:128k:aac:m4a
    Returns dict with possible keys:
      kind: "video"|"audio"
      height: int
      audio_bitrate_kbps: int
      container: str
      vcodec: str
      acodec: str
    """
    s = (variant_id or "").strip()
    if not s:
        return {}

    parts = s.split(":")
    if len(parts) < 2:
        return {}

    out = {}
    head = parts[0].strip().lower()
    if head == "v":
        out["kind"] = "video"
    elif head == "a":
        out["kind"] = "audio"

    # second part: 1440p / 128k etc
    p1 = parts[1].strip().lower()
    if p1.endswith("p"):
        try:
            out["height"] = int(p1[:-1])
        except Exception:
            pass
    if p1.endswith("k"):
        try:
            out["audio_bitrate_kbps"] = int(p1[:-1])
        except Exception:
            pass

    # codec segment could be like "h264+aac" or "aac"
    if len(parts) >= 3:
        c = parts[2].strip().lower()
        if "+" in c:
            v, a = c.split("+", 1)
            out["vcodec"] = v.strip()
            out["acodec"] = a.strip()
        else:
            # audio-only form often: a:128k:aac:m4a
            if out.get("kind") == "audio":
                out["acodec"] = c
            else:
                out["vcodec"] = c

    # container in last part
    if len(parts) >= 4:
        out["container"] = parts[3].strip().lower()

    return out


class RedisJobStore:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.prefix = cfg.get("redis_prefix") or "ytconvert:"
        self.redis = redis.Redis.from_url(cfg.get("redis_url"), decode_responses=False)
        self._lock = threading.RLock()

        self._metric_uploaded_bytes = 0

    # ---- key helpers

    def _k(self, suffix):
        return f"{self.prefix}{suffix}"

    def _job_key(self, job_id):
        return self._k(f"job:{job_id}")

    def _idem_key(self, idem_key):
        return self._k(f"idem:{idem_key}")

    def _state_set_key(self, state):
        return self._k(f"state:{int(state)}")

    def _job_ids_set_key(self):
        return self._k("jobs")

    # ---- metrics for Info

    def metrics_snapshot(self):
        # best-effort; Redis may be down
        try:
            total = self.redis.scard(self._job_ids_set_key())
            queued = self.redis.scard(self._state_set_key(STATE_QUEUED))
            waiting = self.redis.scard(self._state_set_key(STATE_WAITING_UPLOAD))
            running = self.redis.scard(self._state_set_key(STATE_RUNNING))
            done = self.redis.scard(self._state_set_key(STATE_DONE))
            failed = self.redis.scard(self._state_set_key(STATE_FAILED))
            canceled = self.redis.scard(self._state_set_key(STATE_CANCELED))
        except Exception:
            return {}

        active = queued + waiting + running
        return {
            "jobs_total": float(total),
            "jobs_active": float(active),
            "jobs_queued": float(queued),
            "jobs_waiting_upload": float(waiting),
            "jobs_running": float(running),
            "jobs_done": float(done),
            "jobs_failed": float(failed),
            "jobs_canceled": float(canceled),
        }

    # ---- JobStore interface

    def create_job(self, video_id, variants, options, idempotency_key=""):
        with self._lock:
            if idempotency_key:
                try:
                    existing = self.redis.get(self._idem_key(idempotency_key))
                except Exception:
                    existing = None
                if existing:
                    jid = _u(existing, "")
                    job = self.get(jid)
                    if job:
                        return job, True

            job_id = new_job_id()
            job = RedisConvertJob(self.cfg, self.logger, self, job_id)

            now = now_ts()
            job_fields = {
                "job_id": job_id,
                "video_id": video_id,
                "state": str(STATE_WAITING_UPLOAD),
                "percent": "0",
                "message": "waiting upload",
                "meta_json": _j({}),
                "error_json": _j({}),
                "created_at_ts": str(now),
                "started_at_ts": "0",
                "finished_at_ts": "0",
                "received_bytes": "0",
                "expected_offset": "0",
                "upload_done": "0",
                "variants_json": _j(variants or []),
                "options_json": _j(options or {}),
                "ready_variant_ids_json": _j([]),
                "results_by_variant_id_json": _j({}),
            }

            pipe = self.redis.pipeline()
            pipe.hset(self._job_key(job_id), mapping=job_fields)
            pipe.sadd(self._job_ids_set_key(), job_id)
            pipe.sadd(self._state_set_key(STATE_WAITING_UPLOAD), job_id)
            if idempotency_key:
                pipe.set(self._idem_key(idempotency_key), job_id)
            pipe.execute()

            return job, False

    def get(self, job_id):
        job_id = (job_id or "").strip()
        if not job_id:
            return None
        try:
            exists = self.redis.exists(self._job_key(job_id))
        except Exception:
            return None
        if not exists:
            return None
        return RedisConvertJob(self.cfg, self.logger, self, job_id)

    def cleanup_loop(self, stop_event):
        while not stop_event.is_set():
            stop_event.wait(30)
            self.cleanup_expired()

    def cleanup_expired(self):
        ttl = int(self.cfg.get("ttl_seconds") or 0)
        if ttl <= 0:
            return

        try:
            job_ids = list(self.redis.smembers(self._job_ids_set_key()))
        except Exception:
            return

        now = now_ts()
        removed = 0

        for jid_b in job_ids:
            jid = _u(jid_b, "")
            if not jid:
                continue

            k = self._job_key(jid)
            try:
                h = self.redis.hmget(k, "state", "finished_at_ts")
            except Exception:
                continue

            state = _i(h[0], 0)
            finished_at = _i(h[1], 0)

            if state not in (STATE_DONE, STATE_FAILED, STATE_CANCELED):
                continue
            if finished_at <= 0:
                continue
            if (now - finished_at) < ttl:
                continue

            try:
                # remove redis keys
                pipe = self.redis.pipeline()
                pipe.delete(k)
                pipe.srem(self._job_ids_set_key(), jid)
                for st in (STATE_QUEUED, STATE_WAITING_UPLOAD, STATE_RUNNING, STATE_DONE, STATE_FAILED, STATE_CANCELED):
                    pipe.srem(self._state_set_key(st), jid)
                pipe.execute()
            except Exception:
                continue

            # remove job workdir
            workdir = os.path.join(self.cfg["workdir"], jid)
            rm_tree(workdir)

            removed += 1

        if removed:
            self.logger.info("cleanup: removed %d expired jobs", removed)


class RedisConvertJob:
    def __init__(self, cfg, logger, store, job_id):
        self.cfg = cfg
        self.logger = logger
        self.store = store
        self.job_id = job_id

        self.workdir = os.path.join(self.cfg["workdir"], self.job_id)
        self.src_path = os.path.join(self.workdir, "source.bin")
        self.src_meta_path = os.path.join(self.workdir, "source_meta.json")
        self.results_dir = os.path.join(self.workdir, "results")
        self.results_index_path = os.path.join(self.workdir, "results_index.json")

        ensure_dir(self.workdir)
        ensure_dir(self.results_dir)

    # ---- computed snapshot properties (match old in-memory usage)

    @property
    def video_id(self):
        return _u(self._hget("video_id"), "")

    @property
    def state(self):
        return _i(self._hget("state"), 0)

    @property
    def percent(self):
        return _i(self._hget("percent"), 0)

    @property
    def message(self):
        return _u(self._hget("message"), "")

    @property
    def meta(self):
        return self._hget_json("meta_json", {})

    @property
    def error(self):
        e = self._hget_json("error_json", {})
        return e if e else None

    @property
    def variants(self):
        return self._hget_json("variants_json", [])

    @property
    def ready_variant_ids(self):
        return set(self._hget_json("ready_variant_ids_json", []))

    @property
    def results_by_variant_id(self):
        return self._hget_json("results_by_variant_id_json", {})

    @property
    def received_bytes(self):
        return _i(self._hget("received_bytes"), 0)

    def get_next_offset(self):
        return _i(self._hget("expected_offset"), 0)

    def is_terminal(self):
        return self.state in (STATE_DONE, STATE_FAILED, STATE_CANCELED)

    # ---- redis helpers

    def _key(self):
        return self.store._job_key(self.job_id)

    def _hget(self, field):
        try:
            return self.store.redis.hget(self._key(), field)
        except Exception:
            return None

    def _hset(self, mapping):
        try:
            self.store.redis.hset(self._key(), mapping=mapping)
            return True
        except Exception:
            return False

    def _hget_json(self, field, default):
        s = self._hget(field)
        if not s:
            return default
        try:
            return json.loads(_u(s, ""))
        except Exception:
            return default

    def _hset_json(self, field, obj):
        self._hset({field: _j(obj)})

    # ---- state transitions

    def _set_state(self, state, percent=None, message=None, meta=None):
        old_state = self.state
        mapping = {"state": str(int(state))}
        if percent is not None:
            mapping["percent"] = str(int(percent))
        if message is not None:
            mapping["message"] = str(message)
        if meta is not None:
            mapping["meta_json"] = _j(meta)

        if state == STATE_RUNNING:
            mapping["started_at_ts"] = str(_i(self._hget("started_at_ts"), 0) or now_ts())
        if state in (STATE_DONE, STATE_FAILED, STATE_CANCELED):
            mapping["finished_at_ts"] = str(now_ts())

        pipe = self.store.redis.pipeline()
        pipe.hset(self._key(), mapping=mapping)
        if old_state != state:
            pipe.srem(self.store._state_set_key(old_state), self.job_id)
            pipe.sadd(self.store._state_set_key(state), self.job_id)
        pipe.execute()

    def _fail(self, code, message, meta=None):
        self.logger.error("job %s failed: %s (%s)", self.job_id, message, code)
        err = {"code": code, "message": message, "meta": meta or {}}
        self._hset_json("error_json", err)
        self._set_state(STATE_FAILED, percent=max(self.percent, 0), message=message, meta=self.meta)

    def _done(self):
        self._set_state(STATE_DONE, percent=100, message="done", meta=self.meta)

    # ---- upload

    def append_upload_chunk(self, offset, data, last, filename="", content_type="", total_size_bytes=0):
        if self.is_terminal():
            return False, "job already finished", self.received_bytes, self.get_next_offset()

        expected = self.get_next_offset()
        if int(offset) != int(expected):
            return False, "bad offset", self.received_bytes, expected

        if data:
            max_bytes = int(self.cfg.get("max_upload_bytes") or 0)
            if max_bytes > 0 and (self.received_bytes + len(data)) > max_bytes:
                self._fail("UPLOAD_TOO_LARGE", "Upload exceeds max size", {"max_upload_bytes": max_bytes})
                return False, "upload too large", self.received_bytes, self.get_next_offset()

            with open(self.src_path, "ab") as f:
                f.write(data)

            new_received = self.received_bytes + len(data)
            new_expected = expected + len(data)

            self._hset({
                "received_bytes": str(int(new_received)),
                "expected_offset": str(int(new_expected)),
            })

            if not os.path.exists(self.src_meta_path):
                meta = {
                    "filename": filename or "",
                    "content_type": content_type or "",
                    "total_size_bytes": int(total_size_bytes or 0),
                }
                try:
                    with open(self.src_meta_path, "w", encoding="utf-8") as f:
                        json.dump(meta, f)
                except Exception:
                    pass

        if last:
            self._hset({"upload_done": "1"})
            if self.state == STATE_WAITING_UPLOAD:
                self._start_worker_async()

        return True, "ok", self.received_bytes, self.get_next_offset()

    # ---- conversion worker (best-effort single runner)

    def _start_worker_async(self):
        # Use SETNX-like lock in Redis to avoid parallel workers.
        lock_key = self.store._k(f"lock:jobrun:{self.job_id}")
        try:
            ok = self.store.redis.set(lock_key, "1", nx=True, ex=3600)
        except Exception:
            ok = None

        if not ok:
            return

        t = threading.Thread(target=self._run_convert, name=f"job-{self.job_id}", daemon=True)
        t.start()

    def _run_convert(self):
        self._set_state(STATE_RUNNING, percent=0, message="running", meta={"stage": "PROBE"})

        if not os.path.exists(self.src_path) or os.path.getsize(self.src_path) == 0:
            self._fail("NO_SOURCE", "No uploaded source", {})
            return

        ok = self._probe_source()
        if not ok:
            return

        variants = self.variants or []
        total = len(variants)

        for idx, v in enumerate(variants):
            variant_id = (v.get("variant_id") or "").strip()
            if not variant_id:
                self._fail("BAD_REQUEST", "Variant without variant_id", {})
                return

            # fallback parse
            parsed = parse_variant_id(variant_id)
            kind = int(v.get("kind") or 0)
            container = (v.get("container") or "").strip().lower()
            height = int(v.get("height") or 0)
            abr = int(v.get("audio_bitrate_kbps") or 0)
            vcodec = (v.get("vcodec") or "").strip().lower()
            acodec = (v.get("acodec") or "").strip().lower()

            if not container:
                container = parsed.get("container", "")
            if not vcodec:
                vcodec = parsed.get("vcodec", "")
            if not acodec:
                acodec = parsed.get("acodec", "")
            if height <= 0:
                height = int(parsed.get("height") or 0)
            if abr <= 0:
                abr = int(parsed.get("audio_bitrate_kbps") or 0)

            # If kind unspecified, infer from parsed
            if kind == 0:
                if parsed.get("kind") == "audio":
                    kind = 2
                elif parsed.get("kind") == "video":
                    kind = 1

            v2 = dict(v)
            v2["container"] = container or v.get("container")
            v2["vcodec"] = vcodec or v.get("vcodec")
            v2["acodec"] = acodec or v.get("acodec")
            v2["height"] = height
            v2["audio_bitrate_kbps"] = abr
            v2["kind"] = kind

            stage_meta = {"stage": "ENCODE", "variant": variant_id}
            self._set_state(
                STATE_RUNNING,
                percent=int((idx * 100) / max(total, 1)),
                message="encoding",
                meta=stage_meta,
            )

            try:
                if kind == 2 or (container in ("m4a", "mp3") and kind != 1):
                    self._encode_audio_variant(v2, variant_id)
                else:
                    self._encode_video_variant(v2, variant_id)

                self._mark_variant_ready(variant_id)
            except Exception as e:
                self._fail("FFMPEG_FAILED", "ffmpeg failed", {"exception": str(e), "variant_id": variant_id})
                return

        self._done()

    def _mark_variant_ready(self, variant_id):
        ready = sorted(list(self.ready_variant_ids.union({variant_id})))
        self._hset_json("ready_variant_ids_json", ready)
        self._persist_results_index()

    def _probe_source(self):
        ffprobe = self.cfg["ffprobe_bin"]
        args = [
            ffprobe,
            "-v", "error",
            "-show_format",
            "-show_streams",
            "-of", "json",
            self.src_path,
        ]
        code, out, err = run_cmd_capture(args, cwd=self.workdir)
        if code != 0:
            self._fail("FFPROBE_FAILED", "ffprobe failed", {"exit_code": code, "stderr": (err or "")[-4000:]})
            return False

        try:
            info = json.loads(out) if out else {}
        except Exception:
            info = {}

        self._set_state(self.state, meta={"stage": "PROBE", "ffprobe": info})
        return True

    def _encode_video_variant(self, v, variant_id):
        label = v.get("label") or ""
        height = int(v.get("height") or 0)
        vcodec = (v.get("vcodec") or "h264").lower() or "h264"
        acodec = (v.get("acodec") or "aac").lower() or "aac"
        container = (v.get("container") or "mp4").lower() or "mp4"

        if container != "mp4":
            container = "mp4"
        if vcodec in ("auto", ""):
            vcodec = "h264"
        if acodec in ("auto", ""):
            acodec = "aac"

        filename = f"v_{height}p.mp4" if height > 0 else "v_source.mp4"
        out_path = os.path.join(self.results_dir, filename)

        vf = f"scale=-2:{height}" if height > 0 else None

        ffmpeg = self.cfg["ffmpeg_bin"]
        args = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", self.src_path]
        if vf:
            args += ["-vf", vf]

        args += ["-c:v", "libx264"] if vcodec == "h264" else ["-c:v", "copy"] if vcodec == "copy" else ["-c:v", "libx264"]
        args += ["-c:a", "aac"] if acodec == "aac" else ["-c:a", "copy"] if acodec == "copy" else ["-c:a", "aac"]
        args += ["-movflags", "+faststart", out_path]

        code, out, err = run_cmd_capture(args, cwd=self.workdir)
        if code != 0:
            raise RuntimeError((err or "")[-4000:])

        art = self._make_artifact_ref("main", filename, "video/mp4", out_path, {"variant_id": variant_id})
        self._set_variant_result(variant_id, label, [art])

    def _encode_audio_variant(self, v, variant_id):
        label = v.get("label") or ""
        bitrate = int(v.get("audio_bitrate_kbps") or 128)
        container = (v.get("container") or "m4a").lower() or "m4a"
        if container != "m4a":
            container = "m4a"

        filename = f"a_{bitrate}k.m4a"
        out_path = os.path.join(self.results_dir, filename)

        ffmpeg = self.cfg["ffmpeg_bin"]
        args = [
            ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
            "-i", self.src_path,
            "-vn",
            "-c:a", "aac",
            "-b:a", f"{bitrate}k",
            out_path,
        ]

        code, out, err = run_cmd_capture(args, cwd=self.workdir)
        if code != 0:
            raise RuntimeError((err or "")[-4000:])

        art = self._make_artifact_ref("main", filename, "audio/mp4", out_path, {"bitrate_kbps": bitrate, "variant_id": variant_id})
        self._set_variant_result(variant_id, label, [art])

    def _make_artifact_ref(self, artifact_id, filename, mime, path, meta=None):
        st = os.stat(path)
        digest = sha256_file(path)
        return {
            "artifact_id": artifact_id,
            "filename": filename,
            "mime": mime,
            "size_bytes": int(st.st_size),
            "sha256": digest.hex(),
            "inline_bytes": "",
            "meta": meta or {},
            "path": path,
        }

    def _set_variant_result(self, variant_id, label, artifacts):
        results = self.results_by_variant_id
        results[variant_id] = {
            "variant_id": variant_id,
            "label": label or "",
            "artifacts": artifacts or [],
            "meta": {},
        }
        self._hset_json("results_by_variant_id_json", results)
        self._persist_results_index()

    def _persist_results_index(self):
        idx = {
            "job_id": self.job_id,
            "video_id": self.video_id,
            "results_by_variant_id": self.results_by_variant_id,
            "ready_variant_ids": sorted(list(self.ready_variant_ids)),
        }
        try:
            with open(self.results_index_path, "w", encoding="utf-8") as f:
                json.dump(idx, f)
        except Exception:
            pass