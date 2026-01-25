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


def make_converter_servicer(converter_pb2_grpc, converter_pb2, cfg, logger, job_store):
    class ConverterServicer(converter_pb2_grpc.ConverterServicer):
        def SubmitConvert(self, request, context):
            video_id = (request.video_id or "").strip()
            if not video_id:
                return converter_pb2.JobAck(
                    job_id="",
                    accepted=False,
                    message="video_id is required",
                    meta=dict_to_struct({"code": "BAD_REQUEST"}),
                )

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
            return converter_pb2.JobAck(
                job_id=job.job_id,
                accepted=True,
                message=msg,
                meta=dict_to_struct(meta),
            )

        def UploadSource(self, request_iterator, context):
            first = True
            job = None

            for chunk in request_iterator:
                job_id = (chunk.job_id or "").strip()
                if first:
                    if not job_id:
                        return converter_pb2.UploadAck(
                            job_id="",
                            accepted=False,
                            message="job_id is required",
                            received_bytes=0,
                            meta=dict_to_struct({"code": "BAD_REQUEST"}),
                        )
                    job = job_store.get(job_id)
                    if not job:
                        return converter_pb2.UploadAck(
                            job_id=job_id,
                            accepted=False,
                            message="job not found",
                            received_bytes=0,
                            meta=dict_to_struct({"code": "NOT_FOUND"}),
                        )
                    first = False

                ok, msg, received, next_off = job.append_upload_chunk(
                    offset=int(chunk.offset),
                    data=bytes(chunk.data) if chunk.data else b"",
                    last=bool(chunk.last),
                    filename=chunk.filename or "",
                    content_type=chunk.content_type or "",
                    total_size_bytes=int(chunk.total_size_bytes or 0),
                )

                if not ok:
                    return converter_pb2.UploadAck(
                        job_id=job.job_id,
                        accepted=False,
                        message=msg,
                        received_bytes=int(received),
                        meta=dict_to_struct({"next_offset": int(next_off)}),
                    )

            if job is None:
                return converter_pb2.UploadAck(
                    job_id="",
                    accepted=False,
                    message="empty stream",
                    received_bytes=0,
                    meta=dict_to_struct({"code": "BAD_REQUEST"}),
                )

            return converter_pb2.UploadAck(
                job_id=job.job_id,
                accepted=True,
                message="ok",
                received_bytes=int(job.received_bytes),
                meta=dict_to_struct({"next_offset": int(job.get_next_offset())}),
            )

        def GetStatus(self, request, context):
            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            err = _make_error(converter_pb2, job.error)
            return converter_pb2.Status(
                job_id=job.job_id,
                video_id=job.video_id,
                state=_state_to_pb(job.state, converter_pb2),
                percent=int(job.percent),
                message=job.message or "",
                meta=dict_to_struct(job.meta or {}),
                error=err,
            )

        def GetPartialResult(self, request, context):
            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            err = _make_error(converter_pb2, job.error)
            return converter_pb2.PartialConvertResult(
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

        def GetResult(self, request, context):
            job_id = (request.job_id or "").strip()
            job = job_store.get(job_id)
            if not job:
                context.abort(grpc.StatusCode.NOT_FOUND, "job not found")

            if job.state not in (STATE_DONE, STATE_FAILED, STATE_CANCELED):
                # allow calling early, but return current snapshot
                pass

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
            return converter_pb2.ConvertResult(
                job_id=job.job_id,
                video_id=job.video_id,
                state=_state_to_pb(job.state, converter_pb2),
                message=job.message or "",
                meta=dict_to_struct(job.meta or {}),
                error=err,
                results_by_variant_id=results_map,
            )

        def DownloadResult(self, request, context):
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

            chunk_size = int(cfg.get("download_chunk_bytes") or (1024 * 1024))

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
                        return

                    next_off = cur + len(b)
                    last = next_off >= total

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
                        return

        def WatchJob(self, request, context):
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
                        yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.DONE, partial=partial)
                        return
                    if job.state == STATE_FAILED:
                        yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.FAILED, partial=partial)
                        return

                    last_state = state
                    last_percent = percent
                    last_ready = ready
                else:
                    yield converter_pb2.JobEvent(type=converter_pb2.JobEvent.HEARTBEAT)

                time.sleep(heartbeat)

    return ConverterServicer()