import os
import threading
import uuid
import json

from utils.files_ut import ensure_dir, rm_tree
from utils.time_ut import now_ts
from utils.proc_ut import run_cmd_capture
from utils.hash_ut import sha256_file

from jobs.redis_job import (
    STATE_QUEUED,
    STATE_WAITING_UPLOAD,
    STATE_RUNNING,
    STATE_DONE,
    STATE_FAILED,
    STATE_CANCELED,
    RedisJobStore as JobStore,
)


def new_job_id():
    return uuid.uuid4().hex


class JobStore:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.lock = threading.RLock()
        self.jobs = {}
        self.idem_map = {}

    def create_job(self, video_id, variants, options, idempotency_key=""):
        with self.lock:
            if idempotency_key:
                existing = self.idem_map.get(idempotency_key)
                if existing and existing in self.jobs:
                    return self.jobs[existing], True

            job_id = new_job_id()
            job = ConvertJob(self.cfg, self.logger, job_id, video_id, variants, options)
            self.jobs[job_id] = job
            if idempotency_key:
                self.idem_map[idempotency_key] = job_id
            return job, False

    def get(self, job_id):
        with self.lock:
            return self.jobs.get(job_id)

    def cleanup_loop(self, stop_event):
        while not stop_event.is_set():
            stop_event.wait(30)
            self.cleanup_expired()

    def cleanup_expired(self):
        ttl = int(self.cfg.get("ttl_seconds") or 0)
        if ttl <= 0:
            return

        now = now_ts()
        to_delete = []
        with self.lock:
            for job_id, job in self.jobs.items():
                if not job.is_terminal():
                    continue
                done_at = job.finished_at_ts or 0
                if done_at <= 0:
                    continue
                if now - done_at >= ttl:
                    to_delete.append(job_id)

            for job_id in to_delete:
                job = self.jobs.pop(job_id, None)
                for k, v in list(self.idem_map.items()):
                    if v == job_id:
                        self.idem_map.pop(k, None)
                if job:
                    job.cleanup_workdir()

        if to_delete:
            self.logger.info("cleanup: removed %d expired jobs", len(to_delete))


class ConvertJob:
    def __init__(self, cfg, logger, job_id, video_id, variants, options):
        self.cfg = cfg
        self.logger = logger
        self.job_id = job_id
        self.video_id = video_id
        self.variants = variants or []
        self.options = options or {}

        self.state = STATE_QUEUED
        self.percent = 0
        self.message = "queued"
        self.meta = {}

        self.error = None

        self.created_at_ts = now_ts()
        self.started_at_ts = 0
        self.finished_at_ts = 0

        self.workdir = os.path.join(self.cfg["workdir"], self.job_id)
        self.src_path = os.path.join(self.workdir, "source.bin")
        self.src_meta_path = os.path.join(self.workdir, "source_meta.json")
        self.results_dir = os.path.join(self.workdir, "results")
        self.results_index_path = os.path.join(self.workdir, "results_index.json")

        self.received_bytes = 0
        self.expected_offset = 0
        self.upload_done = False

        self.ready_variant_ids = set()
        self.results_by_variant_id = {}

        self.cv = threading.Condition()

        ensure_dir(self.workdir)
        ensure_dir(self.results_dir)

        self.state = STATE_WAITING_UPLOAD
        self.message = "waiting upload"

        self.worker = None

    def is_terminal(self):
        return self.state in (STATE_DONE, STATE_FAILED, STATE_CANCELED)

    def cleanup_workdir(self):
        rm_tree(self.workdir)

    def get_next_offset(self):
        return self.expected_offset

    def append_upload_chunk(self, offset, data, last, filename="", content_type="", total_size_bytes=0):
        with self.cv:
            if self.is_terminal():
                return False, "job already finished", self.received_bytes, self.expected_offset

            if self.state not in (STATE_WAITING_UPLOAD, STATE_RUNNING):
                self.state = STATE_WAITING_UPLOAD

            if offset != self.expected_offset:
                return False, "bad offset", self.received_bytes, self.expected_offset

            if data:
                max_bytes = int(self.cfg.get("max_upload_bytes") or 0)
                if max_bytes > 0 and (self.received_bytes + len(data)) > max_bytes:
                    self._fail("UPLOAD_TOO_LARGE", "Upload exceeds max size", {"max_upload_bytes": max_bytes})
                    return False, "upload too large", self.received_bytes, self.expected_offset

                with open(self.src_path, "ab") as f:
                    f.write(data)

                self.received_bytes += len(data)
                self.expected_offset += len(data)

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
                self.upload_done = True
                if self.state == STATE_WAITING_UPLOAD:
                    self._start_worker_locked()
                self.cv.notify_all()

            return True, "ok", self.received_bytes, self.expected_offset

    def _start_worker_locked(self):
        if self.worker:
            return
        self.worker = threading.Thread(target=self._run_convert, name=f"job-{self.job_id}", daemon=True)
        self.worker.start()

    def _set_state(self, state, percent=None, message=None, meta=None):
        with self.cv:
            self.state = state
            if percent is not None:
                self.percent = int(percent)
            if message is not None:
                self.message = message
            if meta is not None:
                self.meta = meta
            self.cv.notify_all()

    def _fail(self, code, message, meta=None):
        self.logger.error("job %s failed: %s (%s)", self.job_id, message, code)
        self.error = {
            "code": code,
            "message": message,
            "meta": meta or {},
        }
        self._set_state(STATE_FAILED, percent=max(self.percent, 0), message=message, meta=self.meta)
        self.finished_at_ts = now_ts()

    def _done(self):
        self._set_state(STATE_DONE, percent=100, message="done", meta=self.meta)
        self.finished_at_ts = now_ts()

    def _run_convert(self):
        self.started_at_ts = now_ts()
        self._set_state(STATE_RUNNING, percent=0, message="running", meta={"stage": "PROBE"})

        if not os.path.exists(self.src_path) or os.path.getsize(self.src_path) == 0:
            self._fail("NO_SOURCE", "No uploaded source", {})
            return

        ok = self._probe_source()
        if not ok:
            return

        total = len(self.variants) if self.variants else 0
        for idx, v in enumerate(self.variants):
            if self.is_terminal():
                return

            variant_id = (v.get("variant_id") or "").strip()
            if not variant_id:
                self._fail("BAD_REQUEST", "Variant without variant_id", {})
                return

            label = v.get("label") or ""
            kind = int(v.get("kind") or 0)
            container = (v.get("container") or "").strip().lower()

            stage_meta = {"stage": "ENCODE", "variant": variant_id}
            self._set_state(STATE_RUNNING, percent=int((idx * 100) / max(total, 1)), message="encoding", meta=stage_meta)

            try:
                if kind == 2 or (container in ("m4a", "mp3") and kind != 1):
                    self._encode_audio_variant(v, variant_id, label)
                else:
                    self._encode_video_variant(v, variant_id, label)
                self.ready_variant_ids.add(variant_id)
                self._persist_results_index()
            except Exception as e:
                self._fail("FFMPEG_FAILED", "ffmpeg failed", {"exception": str(e), "variant_id": variant_id})
                return

        self._done()

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
            self._fail("FFPROBE_FAILED", "ffprobe failed", {"exit_code": code, "stderr": err[-4000:]})
            return False

        try:
            info = json.loads(out) if out else {}
        except Exception:
            info = {}

        self.meta = {"stage": "PROBE", "ffprobe": info}
        return True

    def _encode_video_variant(self, v, variant_id, label):
        height = int(v.get("height") or 0)
        vcodec = (v.get("vcodec") or "h264").lower()
        acodec = (v.get("acodec") or "aac").lower()
        container = (v.get("container") or "mp4").lower()

        if container != "mp4":
            container = "mp4"
        if vcodec in ("auto", ""):
            vcodec = "h264"
        if acodec in ("auto", ""):
            acodec = "aac"

        if height > 0:
            filename = f"v_{height}p.mp4"
        else:
            filename = "v_source.mp4"

        out_path = os.path.join(self.results_dir, filename)

        vf = None
        if height > 0:
            vf = f"scale=-2:{height}"

        ffmpeg = self.cfg["ffmpeg_bin"]
        args = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", self.src_path]

        if vf:
            args += ["-vf", vf]

        args += ["-c:v", "libx264"] if vcodec == "h264" else ["-c:v", "copy"] if vcodec == "copy" else ["-c:v", "libx264"]

        args += ["-c:a", "aac"] if acodec == "aac" else ["-c:a", "copy"] if acodec == "copy" else ["-c:a", "aac"]

        args += ["-movflags", "+faststart", out_path]

        code, out, err = run_cmd_capture(args, cwd=self.workdir)
        if code != 0:
            raise RuntimeError(err[-4000:])

        art = self._make_artifact_ref(
            artifact_id="main",
            filename=filename,
            mime="video/mp4",
            path=out_path,
            meta={"variant_id": variant_id},
        )

        self.results_by_variant_id[variant_id] = {
            "variant_id": variant_id,
            "label": label,
            "artifacts": [art],
            "meta": {},
        }

    def _encode_audio_variant(self, v, variant_id, label):
        bitrate = int(v.get("audio_bitrate_kbps") or 128)
        container = (v.get("container") or "m4a").lower()

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
            raise RuntimeError(err[-4000:])

        art = self._make_artifact_ref(
            artifact_id="main",
            filename=filename,
            mime="audio/mp4",
            path=out_path,
            meta={"bitrate_kbps": bitrate, "variant_id": variant_id},
        )

        self.results_by_variant_id[variant_id] = {
            "variant_id": variant_id,
            "label": label,
            "artifacts": [art],
            "meta": {},
        }

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