import json
import os
import threading
import uuid
import traceback
import subprocess
import time

import redis
import grpc

from utils.time_ut import now_ts
from utils.files_ut import ensure_dir, rm_tree
from utils.proc_ut import run_cmd_capture
from utils.hash_ut import sha256_file


STATE_QUEUED = 1
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


def clamp_int(v, lo, hi):
    try:
        v = int(v)
    except Exception:
        v = lo
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def parse_variant_id(variant_id):
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

    if len(parts) >= 3:
        c = parts[2].strip().lower()
        if "+" in c:
            v, a = c.split("+", 1)
            out["vcodec"] = v.strip()
            out["acodec"] = a.strip()
        else:
            if out.get("kind") == "audio":
                out["acodec"] = c
            else:
                out["vcodec"] = c

    if len(parts) >= 4:
        out["container"] = parts[3].strip().lower()

    return out


def _norm_rel(p: str) -> str:
    return (p or "").strip().replace("\\", "/").lstrip("/")


def _join_rel(a: str, b: str) -> str:
    a = _norm_rel(a)
    b = _norm_rel(b)
    if not a:
        return b
    if not b:
        return a
    return f"{a}/{b}"


def _auth_md(token: str):
    t = (token or "").strip()
    if not t:
        return []
    return [("authorization", f"Bearer {t}")]


def _grpc_opts():
    max_mb = int(os.getenv("YTCONVERT_YTSTORAGE_GRPC_MAX_MB", "64"))
    max_msg = max_mb * 1024 * 1024
    return [
        ("grpc.max_send_message_length", max_msg),
        ("grpc.max_receive_message_length", max_msg),
    ]


def _make_channel(addr: str, tls: bool):
    if tls:
        return grpc.secure_channel(addr, grpc.ssl_channel_credentials(), options=_grpc_opts())
    return grpc.insecure_channel(addr, options=_grpc_opts())


try:
    from proto import ytstorage_pb2 as spb  # type: ignore
    from proto import ytstorage_pb2_grpc as sgrpc  # type: ignore
except Exception:
    spb = None  # type: ignore
    sgrpc = None  # type: ignore


def _storage_addr(st: dict) -> str:
    if not st:
        return ""
    for k in ("address", "hostport", "target"):
        v = str(st.get(k) or "").strip()
        if v:
            return v
    host = str(st.get("host") or "").strip()
    port = str(st.get("port") or "").strip()
    if host and port:
        return f"{host}:{port}"
    return ""


def _download_from_storage(addr: str, tls: bool, token: str, rel_path: str, dst_path: str, logger, job_id: str):
    if spb is None or sgrpc is None:
        raise RuntimeError("ytstorage stubs not found (expected proto/ytstorage_pb2*.py)")

    rel_path = _norm_rel(rel_path)
    ensure_dir(os.path.dirname(dst_path))

    md = _auth_md(token)
    bytes_written = 0

    with _make_channel(addr, tls) as ch:
        stub = sgrpc.StorageServiceStub(ch)
        stream = stub.Read(
            spb.ReadRequest(
                path=spb.Path(rel_path=rel_path),
                offset=0,
                length=-1,
            ),
            metadata=md,
        )
        with open(dst_path, "wb") as f:
            for msg in stream:
                data = bytes(getattr(msg, "data", b"") or b"")
                if not data:
                    continue
                f.write(data)
                bytes_written += len(data)
                if bytes_written and (bytes_written % (256 * 1024 * 1024) == 0):
                    logger.info("job %s: DOWNLOAD %d MB...", job_id, bytes_written // (1024 * 1024))

    return bytes_written


def _mkdirs_storage(addr: str, tls: bool, token: str, rel_dir: str):
    if spb is None or sgrpc is None:
        raise RuntimeError("ytstorage stubs not found (expected proto/ytstorage_pb2*.py)")
    md = _auth_md(token)
    with _make_channel(addr, tls) as ch:
        stub = sgrpc.StorageServiceStub(ch)
        resp = stub.Mkdirs(
            spb.MkdirsRequest(path=spb.Path(rel_path=_norm_rel(rel_dir)), exist_ok=True),
            metadata=md,
        )
        if not getattr(resp, "ok", False):
            raise RuntimeError("ytstorage Mkdirs failed")


def _upload_file_to_storage(addr: str, tls: bool, token: str, rel_path: str, src_path: str):
    if spb is None or sgrpc is None:
        raise RuntimeError("ytstorage stubs not found (expected proto/ytstorage_pb2*.py)")
    md = _auth_md(token)
    rel_path = _norm_rel(rel_path)

    def gen():
        yield spb.WriteEnvelope(
            header=spb.WriteHeader(
                path=spb.Path(rel_path=rel_path),
                overwrite=True,
                append=False,
                expected_size=0,
                etag="",
            )
        )
        with open(src_path, "rb") as f:
            while True:
                b = f.read(1024 * 1024)
                if not b:
                    break
                yield spb.WriteEnvelope(data=spb.WriteData(data=b))

    last = None
    with _make_channel(addr, tls) as ch:
        stub = sgrpc.StorageServiceStub(ch)
        for ack in stub.Write(gen(), metadata=md):
            last = ack

    if last and getattr(last, "ok", False):
        return int(getattr(last, "bytes_written", 0) or 0)

    raise RuntimeError(f"ytstorage upload failed: {getattr(last, 'error', '')}")


def _ffmpeg_muxer_for_container(container: str) -> str:
    """
    Map "container" (extension-ish) to a valid ffmpeg muxer name.
    Important case: m4a is an extension, muxer is mp4.
    """
    c = (container or "").strip().lower()
    if c == "m4a":
        return "mp4"
    # usually fine:
    return c or "mp4"


class RedisJobStore:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.prefix = cfg.get("redis_prefix") or "ytconvert:"
        self.redis = redis.Redis.from_url(cfg.get("redis_url"), decode_responses=False)
        self._lock = threading.RLock()

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

    def metrics_snapshot(self):
        try:
            total = self.redis.scard(self._job_ids_set_key())
            queued = self.redis.scard(self._state_set_key(STATE_QUEUED))
            running = self.redis.scard(self._state_set_key(STATE_RUNNING))
            done = self.redis.scard(self._state_set_key(STATE_DONE))
            failed = self.redis.scard(self._state_set_key(STATE_FAILED))
            canceled = self.redis.scard(self._state_set_key(STATE_CANCELED))
        except Exception:
            return {}

        active = queued + running
        return {
            "jobs_total": float(total),
            "jobs_active": float(active),
            "jobs_queued": float(queued),
            "jobs_running": float(running),
            "jobs_done": float(done),
            "jobs_failed": float(failed),
            "jobs_canceled": float(canceled),
        }

    def create_job(self, video_id, variants, options, idempotency_key="", source=None, output=None):
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
                        self.logger.info("redis: idempotency hit key=%s -> job_id=%s", idempotency_key, jid)
                        return job, True

            job_id = new_job_id()
            job = RedisConvertJob(self.cfg, self.logger, self, job_id)

            now = now_ts()
            job_fields = {
                "job_id": job_id,
                "video_id": video_id,
                "state": str(STATE_QUEUED),
                "percent": "0",
                "message": "queued",
                "meta_json": _j({"stage": "QUEUED"}),
                "error_json": _j({}),
                "created_at_ts": str(now),
                "started_at_ts": "0",
                "finished_at_ts": "0",
                "variants_json": _j(variants or []),
                "options_json": _j(options or {}),
                "ready_variant_ids_json": _j([]),
                "results_by_variant_id_json": _j({}),
                "duration_sec": "0",
                "source_json": _j(source or {}),
                "output_json": _j(output or {}),
            }

            pipe = self.redis.pipeline()
            pipe.hset(self._job_key(job_id), mapping=job_fields)
            pipe.sadd(self._job_ids_set_key(), job_id)
            pipe.sadd(self._state_set_key(STATE_QUEUED), job_id)
            if idempotency_key:
                pipe.set(self._idem_key(idempotency_key), job_id)
            pipe.execute()

            self.logger.info("redis: created job_id=%s video_id=%s variants=%d", job_id, video_id, len(variants or []))

            job._start_worker_async()
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
                pipe = self.redis.pipeline()
                pipe.delete(k)
                pipe.srem(self._job_ids_set_key(), jid)
                for st in (STATE_QUEUED, STATE_RUNNING, STATE_DONE, STATE_FAILED, STATE_CANCELED):
                    pipe.srem(self._state_set_key(st), jid)
                pipe.execute()
            except Exception:
                continue

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

        self._last_progress_ts = 0.0
        self._last_job_percent = -1

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
    def source(self):
        return self._hget_json("source_json", {}) or {}

    @property
    def output(self):
        return self._hget_json("output_json", {}) or {}

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
        self.logger.error("job %s: FAILED code=%s message=%s meta=%s", self.job_id, code, message, meta or {})
        err = {"code": code, "message": message, "meta": meta or {}}
        self._hset_json("error_json", err)
        self._set_state(STATE_FAILED, percent=max(self.percent, 0), message=message, meta=self.meta)

    def _done(self):
        self._set_state(STATE_DONE, percent=100, message="done", meta=self.meta)
        self.logger.info("job %s: DONE", self.job_id)

    def _start_worker_async(self):
        lock_key = self.store._k(f"lock:jobrun:{self.job_id}")
        try:
            ok = self.store.redis.set(lock_key, "1", nx=True, ex=3600)
        except Exception:
            ok = None

        if not ok:
            self.logger.info("job %s: worker already running (lock exists)", self.job_id)
            return

        self.logger.info("job %s: worker starting", self.job_id)
        t = threading.Thread(target=self._run_convert, name=f"job-{self.job_id}", daemon=True)
        t.start()

    def _publish_progress(self, job_percent, meta):
        now_mono = time.monotonic()
        job_percent = clamp_int(job_percent, 0, 99) if self.state == STATE_RUNNING else clamp_int(job_percent, 0, 100)

        if (now_mono - self._last_progress_ts) < 0.8 and job_percent == self._last_job_percent:
            return

        self._last_progress_ts = now_mono
        self._last_job_percent = job_percent

        self._set_state(self.state, percent=job_percent, message=str(meta.get("stage") or "running"), meta=meta or {})

    def _calc_job_percent(self, progresses, weights):
        if not progresses:
            return 0
        if not weights or len(weights) != len(progresses):
            return clamp_int(sum(progresses) / max(len(progresses), 1), 0, 100)

        sw = float(sum(weights)) if weights else 1.0
        if sw <= 0:
            sw = 1.0
        acc = 0.0
        for p, w in zip(progresses, weights):
            acc += float(p) * float(w)
        return clamp_int(acc / sw, 0, 100)

    def _mark_variant_ready(self, variant_id):
        ready = sorted(list(self.ready_variant_ids.union({variant_id})))
        self._hset_json("ready_variant_ids_json", ready)
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

    def _probe_duration_sec(self):
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
            raise RuntimeError("ffprobe failed")

        try:
            info = json.loads(out) if out else {}
        except Exception:
            info = {}

        duration_sec = 0.0
        fmt = info.get("format") or {}
        duration_str = fmt.get("duration")
        duration_sec = _f(duration_str, 0.0)

        if duration_sec <= 0:
            streams = info.get("streams") or []
            for s in streams:
                d = _f(s.get("duration"), 0.0)
                if d > duration_sec:
                    duration_sec = d

        duration_sec = float(duration_sec or 0.0)
        if duration_sec <= 0:
            duration_sec = 1.0

        meta = {"stage": "PROBE", "duration_sec": float(duration_sec)}
        self._set_state(self.state, percent=self.percent, message=self.message, meta=meta)
        self.logger.info("job %s: PROBE ok duration_sec=%.3f", self.job_id, duration_sec)
        return duration_sec

    def _run_ffmpeg_with_progress(self, args, duration_sec, on_progress):
        p = subprocess.Popen(
            args,
            cwd=self.workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        out_time_ms = 0
        speed = ""
        last_vpct = -1

        def emit(vpct):
            nonlocal last_vpct
            vpct = clamp_int(vpct, 0, 100)
            if vpct == last_vpct:
                return
            last_vpct = vpct
            if on_progress:
                on_progress(vpct, {"current_out_time_sec": float(out_time_ms) / 1_000_000.0, "speed": speed})

        try:
            if not p.stdout:
                raise RuntimeError("ffmpeg stdout is not available")

            for line in p.stdout:
                line = (line or "").strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()

                if k == "out_time_ms":
                    try:
                        out_time_ms = int(v)
                    except Exception:
                        out_time_ms = 0
                    out_sec = float(out_time_ms) / 1_000_000.0
                    vpct = int((out_sec / float(duration_sec)) * 100.0)
                    vpct = clamp_int(vpct, 0, 99)
                    emit(vpct)

                elif k == "speed":
                    speed = v

                elif k == "progress" and v == "end":
                    emit(100)

        finally:
            rc = p.wait()
            stderr = ""
            try:
                if p.stderr:
                    stderr = p.stderr.read() or ""
            except Exception:
                stderr = ""
            if rc != 0:
                raise RuntimeError(stderr[-4000:])

    def _guess_mime_for_container(self, kind, container):
        c = (container or "").strip().lower()

        if kind == "video":
            if c == "mp4":
                return "video/mp4"
            if c == "webm":
                return "video/webm"
            if c == "mkv":
                return "video/x-matroska"
            if c == "mov":
                return "video/quicktime"
            return f"video/{c}" if c else "video/mp4"

        if c == "mp3":
            return "audio/mpeg"
        if c == "ogg":
            return "audio/ogg"
        if c == "m4a":
            return "audio/mp4"
        if c == "wav":
            return "audio/wav"
        if c == "flac":
            return "audio/flac"
        return f"audio/{c}" if c else "audio/mp4"

    def _ffmpeg_codecs_for_video_container(self, container, vcodec, acodec):
        c = (container or "").strip().lower()
        vcodec = (vcodec or "h264").strip().lower()
        acodec = (acodec or "aac").strip().lower()

        if vcodec in ("auto", ""):
            vcodec = "h264"
        if acodec in ("auto", ""):
            acodec = "aac"

        def map_v():
            if vcodec == "copy":
                return ["-c:v", "copy"]
            if c == "webm":
                if vcodec in ("vp9", "libvpx-vp9"):
                    return ["-c:v", "libvpx-vp9"]
                if vcodec in ("vp8", "libvpx"):
                    return ["-c:v", "libvpx"]
                return ["-c:v", "libvpx-vp9"]
            if vcodec in ("h264", "libx264"):
                return ["-c:v", "libx264"]
            return ["-c:v", "libx264"]

        def map_a():
            if acodec == "copy":
                return ["-c:a", "copy"]
            if c == "webm":
                if acodec in ("opus", "libopus"):
                    return ["-c:a", "libopus"]
                if acodec in ("vorbis", "libvorbis"):
                    return ["-c:a", "libvorbis"]
                return ["-c:a", "libopus"]
            if acodec in ("aac",):
                return ["-c:a", "aac"]
            return ["-c:a", "aac"]

        return map_v(), map_a()

    def _ffmpeg_args_for_audio_container(self, container, bitrate_kbps):
        c = (container or "").strip().lower()
        br = int(bitrate_kbps or 128)

        if c == "mp3":
            return ["-c:a", "libmp3lame", "-b:a", f"{br}k"]
        if c == "ogg":
            return ["-c:a", "libvorbis", "-b:a", f"{br}k"]
        # m4a is extension; codec args still fine
        return ["-c:a", "aac", "-b:a", f"{br}k"]

    def _encode_video_variant(self, v, variant_id, duration_sec, on_variant_progress):
        label = v.get("label") or ""
        height = int(v.get("height") or 0)
        vcodec = (v.get("vcodec") or "h264").lower() or "h264"
        acodec = (v.get("acodec") or "aac").lower() or "aac"
        container = (v.get("container") or "mp4").lower() or "mp4"

        filename = f"v_{height}p.{container}" if height > 0 else f"v_source.{container}"
        out_path = os.path.join(self.results_dir, filename)

        vf = f"scale=-2:{height}" if height > 0 else None

        ffmpeg = self.cfg["ffmpeg_bin"]
        args = [ffmpeg, "-y", "-hide_banner", "-nostats", "-loglevel", "error", "-i", self.src_path]

        if vf:
            args += ["-vf", vf]

        v_args, a_args = self._ffmpeg_codecs_for_video_container(container, vcodec, acodec)
        args += v_args
        args += a_args

        muxer = _ffmpeg_muxer_for_container(container)
        if muxer:
            args += ["-f", muxer]

        args += ["-progress", "pipe:1", out_path]

        self._run_ffmpeg_with_progress(args, duration_sec, on_variant_progress)

        mime = self._guess_mime_for_container("video", container)
        art = self._make_artifact_ref("main", filename, mime, out_path, {"variant_id": variant_id, "container": container, "muxer": muxer})
        self._set_variant_result(variant_id, label, [art])

    def _encode_audio_variant(self, v, variant_id, duration_sec, on_variant_progress):
        label = v.get("label") or ""
        bitrate = int(v.get("audio_bitrate_kbps") or 128)
        container = (v.get("container") or "m4a").lower() or "m4a"

        filename = f"a_{bitrate}k.{container}"
        out_path = os.path.join(self.results_dir, filename)

        ffmpeg = self.cfg["ffmpeg_bin"]
        args = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-nostats",
            "-loglevel",
            "error",
            "-i",
            self.src_path,
            "-vn",
        ]
        args += self._ffmpeg_args_for_audio_container(container, bitrate)

        # IMPORTANT FIX: m4a is not a muxer; use mp4 muxer (or omit -f).
        muxer = _ffmpeg_muxer_for_container(container)
        if muxer:
            args += ["-f", muxer]

        args += ["-progress", "pipe:1", out_path]

        self._run_ffmpeg_with_progress(args, duration_sec, on_variant_progress)

        mime = self._guess_mime_for_container("audio", container)
        art = self._make_artifact_ref(
            "main",
            filename,
            mime,
            out_path,
            {"bitrate_kbps": bitrate, "variant_id": variant_id, "container": container, "muxer": muxer},
        )
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
            "meta": meta or {},
            "path": path,
            "rel_path": "",
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

    def _run_convert(self):
        try:
            self._set_state(STATE_RUNNING, percent=0, message="running", meta={"stage": "DOWNLOAD"})

            src = self.source
            out = self.output
            src_storage = (src.get("storage") or {})
            out_storage = (out.get("storage") or {})

            src_addr = _storage_addr(src_storage)
            src_tls = bool(src_storage.get("tls") or False)
            src_token = str(src_storage.get("token") or "")
            src_rel = str(src.get("rel_path") or "")

            out_addr = _storage_addr(out_storage)
            out_tls = bool(out_storage.get("tls") or False)
            out_token = str(out_storage.get("token") or "")
            out_base = str(out.get("base_rel_dir") or "")

            self.logger.info(
                "job %s: STORAGE src_addr=%s src_rel=%s out_addr=%s out_base=%s tls=%s",
                self.job_id,
                src_addr,
                src_rel,
                out_addr,
                out_base,
                bool(src_tls or out_tls),
            )

            if not src_addr or not src_rel:
                self._fail("BAD_REQUEST", "missing source storage ref", {"source": src})
                return
            if not out_addr or not out_base:
                self._fail("BAD_REQUEST", "missing output storage ref", {"output": out})
                return

            self.logger.info("job %s: DOWNLOAD start rel=%s -> %s", self.job_id, src_rel, self.src_path)
            bytes_dl = _download_from_storage(src_addr, src_tls, src_token, src_rel, self.src_path, self.logger, self.job_id)
            self.logger.info("job %s: DOWNLOAD done bytes=%d", self.job_id, int(bytes_dl))

            self._publish_progress(2, {"stage": "PROBE"})
            duration_sec = self._probe_duration_sec()
            self._hset({"duration_sec": str(duration_sec)})

            variants = self.variants or []
            total = len(variants)

            task_weights = []
            task_progress = []
            for v in variants:
                kind = int(v.get("kind") or 0)
                w = 1.0
                if kind == 2:
                    w = 0.3
                task_weights.append(w)
                task_progress.append(0)

            for idx, v in enumerate(variants):
                variant_id = (v.get("variant_id") or "").strip()
                if not variant_id:
                    self._fail("BAD_REQUEST", "Variant without variant_id", {})
                    return

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

                stage_meta = {"stage": "ENCODE", "variant": variant_id, "total_variants": total}
                self._set_state(
                    STATE_RUNNING,
                    percent=self._calc_job_percent(task_progress, task_weights),
                    message="encoding",
                    meta=stage_meta,
                )

                self.logger.info(
                    "job %s: ENCODE start variant_id=%s kind=%d height=%d abr=%d container=%s",
                    self.job_id,
                    variant_id,
                    int(kind),
                    int(height),
                    int(abr),
                    container,
                )

                def on_variant_progress(vpct, extra_meta):
                    task_progress[idx] = vpct
                    jobpct = self._calc_job_percent(task_progress, task_weights)
                    meta2 = {"stage": "ENCODE", "variant": variant_id, "variant_percent": int(vpct), "duration_sec": float(duration_sec)}
                    meta2.update(extra_meta or {})
                    self._publish_progress(jobpct, meta2)

                if kind == 2:
                    self._encode_audio_variant(v2, variant_id, duration_sec, on_variant_progress)
                else:
                    self._encode_video_variant(v2, variant_id, duration_sec, on_variant_progress)

                task_progress[idx] = 100
                self._mark_variant_ready(variant_id)
                self._publish_progress(self._calc_job_percent(task_progress, task_weights), {"stage": "ENCODE", "variant": variant_id})
                self.logger.info("job %s: ENCODE done variant_id=%s", self.job_id, variant_id)

            self._publish_progress(99, {"stage": "UPLOAD"})
            self.logger.info("job %s: UPLOAD start base=%s", self.job_id, out_base)

            _mkdirs_storage(out_addr, out_tls, out_token, out_base)

            results = self.results_by_variant_id or {}
            for variant_id, vr in results.items():
                arts = vr.get("artifacts") or []
                for a in arts:
                    local_path = a.get("path")
                    fname = a.get("filename") or ""
                    if not local_path or not os.path.exists(local_path):
                        self._fail("INTERNAL", "local artifact missing before upload", {"variant_id": variant_id, "path": local_path})
                        return
                    if not fname:
                        fname = os.path.basename(local_path)

                    out_rel_path = _join_rel(out_base, fname)

                    self.logger.info(
                        "job %s: UPLOAD file variant=%s rel=%s size=%d",
                        self.job_id,
                        variant_id,
                        out_rel_path,
                        int(a.get("size_bytes") or 0),
                    )

                    bytes_up = _upload_file_to_storage(out_addr, out_tls, out_token, out_rel_path, local_path)
                    a["rel_path"] = out_rel_path
                    a["meta"] = dict(a.get("meta") or {})
                    a["meta"]["uploaded_bytes"] = int(bytes_up)

                self._set_variant_result(variant_id, vr.get("label") or "", arts)

            self.logger.info("job %s: UPLOAD done", self.job_id)
            self._done()

        except Exception as e:
            tb = traceback.format_exc()[-4000:]
            self._fail("INTERNAL", "Unhandled exception in worker", {"exception": str(e), "traceback": tb})
        finally:
            pass