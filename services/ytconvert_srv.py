import time
import grpc

from jobs.redis_job import (
    STATE_QUEUED,
    STATE_RUNNING,
    STATE_DONE,
    STATE_FAILED,
    STATE_CANCELED,
)
from utils.grpc_ut import dict_to_struct


def _state_to_pb(state, pb2):
    m = {
        STATE_QUEUED: pb2.Status.QUEUED,
        STATE_RUNNING: pb2.Status.RUNNING,
        STATE_DONE: pb2.Status.DONE,
        STATE_FAILED: pb2.Status.FAILED,
        STATE_CANCELED: pb2.Status.CANCELED,
    }
    return m.get(state, pb2.Status.STATE_UNSPECIFIED)


def _make_error(pb2, err):
    if not err:
        return None
    return pb2.ErrorInfo(
        code=err.get("code", ""),
        message=err.get("message", ""),
        meta=dict_to_struct(err.get("meta", {})),
    )


def _peer(context):
    try:
        return context.peer()
    except Exception:
        return ""


def _md(context):
    try:
        return list(context.invocation_metadata() or [])
    except Exception:
        return []


def _short(s, n=200):
    s = "" if s is None else str(s)
    if len(s) <= n:
        return s
    return s[:n] + "..."


def make_converter_servicer(converter_pb2_grpc, converter_pb2, cfg, logger, job_store):
    class ConverterServicer(converter_pb2_grpc.ConverterServicer):
        def SubmitConvert(self, request, context):
            t0 = time.time()
            logger.info(
                "rpc SubmitConvert peer=%s video_id=%s idem=%s variants=%d md=%s",
                _peer(context),
                _short(request.video_id),
                _short(request.idempotency_key),
                len(request.variants),
                _md(context),
            )

            video_id = (request.video_id or "").strip()
            if not video_id:
                resp = converter_pb2.JobAck(
                    job_id="",
                    accepted=False,
                    message="video_id is required",
                    meta=dict_to_struct({"code": "BAD_REQUEST"}),
                )
                logger.info("rpc SubmitConvert -> accepted=%s msg=%s dt_ms=%d", resp.accepted, resp.message, int((time.time() - t0) * 1000))
                return resp

            # Validate storage-driven refs
            if not request.source or not request.source.storage or not (request.source.storage.address or "").strip():
                resp = converter_pb2.JobAck(job_id="", accepted=False, message="source.storage.address is required", meta=dict_to_struct({"code": "BAD_REQUEST"}))
                return resp
            if not (request.source.rel_path or "").strip():
                resp = converter_pb2.JobAck(job_id="", accepted=False, message="source.rel_path is required", meta=dict_to_struct({"code": "BAD_REQUEST"}))
                return resp

            if not request.output or not request.output.storage or not (request.output.storage.address or "").strip():
                resp = converter_pb2.JobAck(job_id="", accepted=False, message="output.storage.address is required", meta=dict_to_struct({"code": "BAD_REQUEST"}))
                return resp
            if not (request.output.base_rel_dir or "").strip():
                resp = converter_pb2.JobAck(job_id="", accepted=False, message="output.base_rel_dir is required", meta=dict_to_struct({"code": "BAD_REQUEST"}))
                return resp

            variants = []
            for v in request.variants:
                variants.append({
                    "variant_id": v.variant_id,
                    "label": v.label,
                    "kind": int(v.kind),
                    "container": v.container,
                    "height": int(v.height),
                    "vcodec": v.vcodec,
                    "acodec": v.acodec,
                    "audio_bitrate_kbps": int(v.audio_bitrate_kbps),
                    "options": dict(v.options) if v.options else {},
                })

            options = dict(request.options) if request.options else {}
            idem = (request.idempotency_key or "").strip()

            src = {
                "storage": {
                    "address": request.source.storage.address,
                    "tls": bool(request.source.storage.tls),
                    "token": request.source.storage.token,
                },
                "rel_path": request.source.rel_path,
            }
            out = {
                "storage": {
                    "address": request.output.storage.address,
                    "tls": bool(request.output.storage.tls),
                    "token": request.output.storage.token,
                },
                "base_rel_dir": request.output.base_rel_dir,
            }

            job, reused = job_store.create_job(video_id, variants, options, idempotency_key=idem, source=src, output=out)

            msg = "accepted"
            if reused:
                msg = "accepted (idempotent reuse)"

            resp = converter_pb2.JobAck(
                job_id=job.job_id,
                accepted=True,
                message=msg,
                meta=dict_to_struct({"queue": "redis"}),
            )

            logger.info(
                "rpc SubmitConvert -> job_id=%s accepted=%s reused=%s src=%s out=%s dt_ms=%d",
                resp.job_id,
                resp.accepted,
                reused,
                _short(request.source.rel_path),
                _short(request.output.base_rel_dir),
                int((time.time() - t0) * 1000),
            )
            return resp

        def GetStatus(self, request, context):
            t0 = time.time()
            logger.info("rpc GetStatus peer=%s job_id=%s", _peer(context), _short(request.job_id))

            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                logger.warning("rpc GetStatus -> NOT_FOUND job_id=%s dt_ms=%d", job_id, int((time.time() - t0) * 1000))
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            err = _make_error(converter_pb2, job.error)
            resp = converter_pb2.Status(
                job_id=job.job_id,
                video_id=job.video_id,
                state=_state_to_pb(job.state, converter_pb2),
                percent=int(job.percent),
                message=job.message or "",
                meta=dict_to_struct(job.meta or {}),
                error=err,
            )
            return resp

        def GetPartialResult(self, request, context):
            t0 = time.time()
            logger.info("rpc GetPartialResult peer=%s job_id=%s", _peer(context), _short(request.job_id))

            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                logger.warning("rpc GetPartialResult -> NOT_FOUND job_id=%s dt_ms=%d", job_id, int((time.time() - t0) * 1000))
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            err = _make_error(converter_pb2, job.error)
            resp = converter_pb2.PartialConvertResult(
                job_id=job.job_id,
                video_id=job.video_id,
                state=_state_to_pb(job.state, converter_pb2),
                percent=int(job.percent),
                message=job.message or "",
                ready_variant_ids=sorted(list(job.ready_variant_ids)),
                total_variants=len(job.variants),
                meta=dict_to_struct(job.meta or {}),
                error=err,
            )
            return resp

        def GetResult(self, request, context):
            t0 = time.time()
            logger.info("rpc GetResult peer=%s job_id=%s", _peer(context), _short(request.job_id))

            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                logger.warning("rpc GetResult -> NOT_FOUND job_id=%s dt_ms=%d", job_id, int((time.time() - t0) * 1000))
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            results_map = {}
            for variant_id, vr in (job.results_by_variant_id or {}).items():
                arts = []
                for a in vr.get("artifacts", []):
                    arts.append(converter_pb2.ArtifactRef(
                        artifact_id=a.get("artifact_id", ""),
                        filename=a.get("filename", ""),
                        mime=a.get("mime", ""),
                        size_bytes=int(a.get("size_bytes") or 0),
                        sha256=bytes.fromhex(a.get("sha256", "")) if a.get("sha256") else b"",
                        rel_path=a.get("rel_path", ""),
                        meta=dict_to_struct(a.get("meta", {})),
                    ))
                results_map[variant_id] = converter_pb2.VariantResult(
                    variant_id=variant_id,
                    label=vr.get("label", ""),
                    artifacts=arts,
                    meta=dict_to_struct(vr.get("meta", {})),
                )

            err = _make_error(converter_pb2, job.error)
            resp = converter_pb2.ConvertResult(
                job_id=job.job_id,
                video_id=job.video_id,
                state=_state_to_pb(job.state, converter_pb2),
                message=job.message or "",
                meta=dict_to_struct(job.meta or {}),
                error=err,
                results_by_variant_id=results_map,
            )

            logger.info(
                "rpc GetResult -> state=%s variants=%d dt_ms=%d",
                resp.state,
                len(resp.results_by_variant_id),
                int((time.time() - t0) * 1000),
            )
            return resp

        def WatchJob(self, request, context):
            logger.info(
                "rpc WatchJob peer=%s job_id=%s send_initial=%s",
                _peer(context),
                _short(request.job_id),
                bool(request.send_initial),
            )

            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            heartbeat = int(cfg.get("watch_heartbeat_sec") or 10)
            send_initial = bool(request.send_initial)

            last_state = None
            last_percent = None
            last_message = None
            last_meta_json = None
            last_ready = None

            def make_status():
                return converter_pb2.Status(
                    job_id=job.job_id,
                    video_id=job.video_id,
                    state=_state_to_pb(job.state, converter_pb2),
                    percent=int(job.percent),
                    message=job.message or "",
                    meta=dict_to_struct(job.meta or {}),
                    error=_make_error(converter_pb2, job.error),
                )

            def make_partial(ready):
                return converter_pb2.PartialConvertResult(
                    job_id=job.job_id,
                    video_id=job.video_id,
                    state=_state_to_pb(job.state, converter_pb2),
                    percent=int(job.percent),
                    message=job.message or "",
                    ready_variant_ids=list(ready),
                    total_variants=len(job.variants),
                    meta=dict_to_struct(job.meta or {}),
                    error=_make_error(converter_pb2, job.error),
                )

            if send_initial:
                st = make_status()
                yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.STATUS, status=st)
                last_state = job.state
                last_percent = job.percent
                last_message = job.message
                last_meta_json = str(job.meta or {})
                last_ready = tuple(sorted(list(job.ready_variant_ids)))

            while True:
                if context.is_active() is False:
                    return

                state = job.state
                percent = job.percent
                message = job.message
                meta_json = str(job.meta or {})
                ready = tuple(sorted(list(job.ready_variant_ids)))

                status_changed = (
                    state != last_state
                    or percent != last_percent
                    or message != last_message
                    or meta_json != last_meta_json
                )
                ready_changed = (ready != last_ready)

                if status_changed:
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.STATUS, status=make_status())
                    last_state = state
                    last_percent = percent
                    last_message = message
                    last_meta_json = meta_json

                if ready_changed or state in (STATE_DONE, STATE_FAILED, STATE_CANCELED):
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.PARTIAL, partial=make_partial(ready))
                    last_ready = ready

                if state == STATE_DONE:
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.DONE, partial=make_partial(ready))
                    return
                if state == STATE_FAILED:
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.FAILED, partial=make_partial(ready))
                    return

                if (not status_changed) and (not ready_changed):
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.HEARTBEAT)

                time.sleep(heartbeat)

    return ConverterServicer()