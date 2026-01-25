import os
import time

import grpc

from google.protobuf import struct_pb2

from jobs.convert_job import (
    STATE_QUEUED,
    STATE_WAITING_UPLOAD,
    STATE_RUNNING,
    STATE_DONE,
    STATE_FAILED,
    STATE_CANCELED,
)
from utils.grpc_ut import dict_to_struct


def _state_to_pb(state, pb2):
    m = {
        STATE_QUEUED: pb2.Status.QUEUED,
        STATE_WAITING_UPLOAD: pb2.Status.WAITING_UPLOAD,
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

            job, reused = job_store.create_job(video_id, variants, options, idempotency_key=idem)

            msg = "accepted"
            if reused:
                msg = "accepted (idempotent reuse)"

            meta = {"next_offset": job.get_next_offset()}
            resp = converter_pb2.JobAck(
                job_id=job.job_id,
                accepted=True,
                message=msg,
                meta=dict_to_struct(meta),
            )

            logger.info(
                "rpc SubmitConvert -> job_id=%s accepted=%s reused=%s next_offset=%s dt_ms=%d",
                resp.job_id,
                resp.accepted,
                reused,
                meta.get("next_offset"),
                int((time.time() - t0) * 1000),
            )
            return resp

        def UploadSource(self, request_iterator, context):
            t0 = time.time()
            peer = _peer(context)
            logger.info("rpc UploadSource peer=%s md=%s", peer, _md(context))

            first = True
            job = None
            chunks = 0
            bytes_in = 0

            for chunk in request_iterator:
                chunks += 1
                job_id = (chunk.job_id or "").strip()
                if first:
                    logger.info(
                        "rpc UploadSource first_chunk job_id=%s offset=%d last=%s filename=%s content_type=%s total=%d",
                        job_id,
                        int(chunk.offset),
                        bool(chunk.last),
                        _short(chunk.filename),
                        _short(chunk.content_type),
                        int(chunk.total_size_bytes or 0),
                    )

                    if not job_id:
                        resp = converter_pb2.UploadAck(
                            job_id="",
                            accepted=False,
                            message="job_id is required",
                            received_bytes=0,
                            meta=dict_to_struct({"code": "BAD_REQUEST"}),
                        )
                        logger.info("rpc UploadSource -> accepted=%s msg=%s dt_ms=%d", resp.accepted, resp.message, int((time.time() - t0) * 1000))
                        return resp

                    job = job_store.get(job_id)
                    if not job:
                        resp = converter_pb2.UploadAck(
                            job_id=job_id,
                            accepted=False,
                            message="job not found",
                            received_bytes=0,
                            meta=dict_to_struct({"code": "NOT_FOUND"}),
                        )
                        logger.info("rpc UploadSource -> accepted=%s msg=%s dt_ms=%d", resp.accepted, resp.message, int((time.time() - t0) * 1000))
                        return resp

                    first = False

                data_len = len(chunk.data) if chunk.data else 0
                bytes_in += data_len

                ok, msg, received, next_off = job.append_upload_chunk(
                    offset=int(chunk.offset),
                    data=bytes(chunk.data) if chunk.data else b"",
                    last=bool(chunk.last),
                    filename=chunk.filename or "",
                    content_type=chunk.content_type or "",
                    total_size_bytes=int(chunk.total_size_bytes or 0),
                )

                if not ok:
                    resp = converter_pb2.UploadAck(
                        job_id=job.job_id,
                        accepted=False,
                        message=msg,
                        received_bytes=int(received),
                        meta=dict_to_struct({"next_offset": int(next_off)}),
                    )
                    logger.warning(
                        "rpc UploadSource -> accepted=%s msg=%s received=%d next_offset=%d chunks=%d bytes_in=%d dt_ms=%d",
                        resp.accepted,
                        resp.message,
                        int(received),
                        int(next_off),
                        chunks,
                        bytes_in,
                        int((time.time() - t0) * 1000),
                    )
                    return resp

            if job is None:
                resp = converter_pb2.UploadAck(
                    job_id="",
                    accepted=False,
                    message="empty stream",
                    received_bytes=0,
                    meta=dict_to_struct({"code": "BAD_REQUEST"}),
                )
                logger.info("rpc UploadSource -> accepted=%s msg=%s dt_ms=%d", resp.accepted, resp.message, int((time.time() - t0) * 1000))
                return resp

            resp = converter_pb2.UploadAck(
                job_id=job.job_id,
                accepted=True,
                message="ok",
                received_bytes=int(job.received_bytes),
                meta=dict_to_struct({"next_offset": int(job.get_next_offset())}),
            )
            logger.info(
                "rpc UploadSource -> accepted=%s received=%d next_offset=%d chunks=%d bytes_in=%d dt_ms=%d",
                resp.accepted,
                resp.received_bytes,
                int(job.get_next_offset()),
                chunks,
                bytes_in,
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
            logger.info(
                "rpc GetStatus -> state=%s percent=%d msg=%s dt_ms=%d",
                resp.state,
                resp.percent,
                _short(resp.message),
                int((time.time() - t0) * 1000),
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
            logger.info(
                "rpc GetPartialResult -> state=%s percent=%d ready=%d/%d dt_ms=%d",
                resp.state,
                resp.percent,
                len(resp.ready_variant_ids),
                resp.total_variants,
                int((time.time() - t0) * 1000),
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
                        inline_bytes=b"",
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

        def DownloadResult(self, request, context):
            t0 = time.time()
            logger.info(
                "rpc DownloadResult peer=%s job_id=%s variant_id=%s artifact_id=%s offset=%d",
                _peer(context),
                _short(request.job_id),
                _short(request.variant_id),
                _short(request.artifact_id),
                int(request.offset or 0),
            )

            job_id = (request.job_id or "").strip()
            variant_id = (request.variant_id or "").strip()
            artifact_id = (request.artifact_id or "").strip()
            offset = int(request.offset or 0)

            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            vr = (job.results_by_variant_id or {}).get(variant_id)
            if not vr:
                context.abort(grpc.StatusCode.NOT_FOUND, "variant not found")

            artifact = None
            for a in vr.get("artifacts", []):
                if a.get("artifact_id") == artifact_id:
                    artifact = a
                    break
            if not artifact:
                context.abort(grpc.StatusCode.NOT_FOUND, "artifact not found")

            path = artifact.get("path")
            if not path or not os.path.exists(path):
                context.abort(grpc.StatusCode.NOT_FOUND, "artifact file missing")

            total = int(artifact.get("size_bytes") or 0)
            if offset < 0 or offset > total:
                context.abort(grpc.StatusCode.OUT_OF_RANGE, "bad offset")

            logger.info(
                "rpc DownloadResult serving filename=%s size=%d mime=%s",
                artifact.get("filename", ""),
                total,
                artifact.get("mime", ""),
            )

            chunk_size = int(cfg.get("download_chunk_bytes") or (1024 * 1024))

            sent = 0
            with open(path, "rb") as f:
                f.seek(offset)
                cur = offset
                first = True
                while True:
                    b = f.read(chunk_size)
                    if not b:
                        yield converter_pb2.DownloadChunk(
                            offset=int(cur),
                            data=b"",
                            last=True,
                            filename=artifact.get("filename", "") if first else "",
                            mime=artifact.get("mime", "") if first else "",
                            total_size_bytes=total if first else 0,
                            sha256_total=bytes.fromhex(artifact.get("sha256", "")) if (first and artifact.get("sha256")) else b"",
                        )
                        break

                    next_off = cur + len(b)
                    last = next_off >= total
                    sent += len(b)

                    yield converter_pb2.DownloadChunk(
                        offset=int(cur),
                        data=b,
                        last=bool(last),
                        filename=artifact.get("filename", "") if first else "",
                        mime=artifact.get("mime", "") if first else "",
                        total_size_bytes=total if first else 0,
                        sha256_total=bytes.fromhex(artifact.get("sha256", "")) if (first and artifact.get("sha256")) else b"",
                    )

                    first = False
                    cur = next_off
                    if last:
                        break

            logger.info("rpc DownloadResult done sent=%d dt_ms=%d", sent, int((time.time() - t0) * 1000))

        def WatchJob(self, request, context):
            logger.info("rpc WatchJob peer=%s job_id=%s send_initial=%s", _peer(context), _short(request.job_id), bool(request.send_initial))
            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            heartbeat = int(cfg.get("watch_heartbeat_sec") or 10)
            send_initial = bool(request.send_initial)

            last_state = None
            last_percent = None
            last_ready = None

            if send_initial:
                st = converter_pb2.Status(
                    job_id=job.job_id,
                    video_id=job.video_id,
                    state=_state_to_pb(job.state, converter_pb2),
                    percent=int(job.percent),
                    message=job.message or "",
                    meta=dict_to_struct(job.meta or {}),
                    error=_make_error(converter_pb2, job.error),
                )
                yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.STATUS, status=st)

            while True:
                if context.is_active() is False:
                    return

                state = job.state
                percent = job.percent
                ready = tuple(sorted(list(job.ready_variant_ids)))

                changed = (state != last_state) or (percent != last_percent) or (ready != last_ready)
                if changed:
                    partial = converter_pb2.PartialConvertResult(
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
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.PARTIAL, partial=partial)

                    if job.state == STATE_DONE:
                        logger.info("rpc WatchJob job_id=%s DONE", job_id)
                        yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.DONE, partial=partial)
                        return
                    if job.state == STATE_FAILED:
                        logger.info("rpc WatchJob job_id=%s FAILED", job_id)
                        yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.FAILED, partial=partial)
                        return

                    last_state = state
                    last_percent = percent
                    last_ready = ready
                else:
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.HEARTBEAT)

                time.sleep(heartbeat)

    return ConverterServicer()