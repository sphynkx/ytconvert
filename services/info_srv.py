import socket
import time

from utils.grpc_ut import dict_to_struct  # not used but kept for future

## TODO: Rework to get predefined host from .env

def build_info_response(pb2, cfg, started_monotonic, metrics_provider=None):
    host = cfg.get("advertise_host") or cfg.get("host") or socket.gethostname()
    instance_id = cfg.get("instance_id") or host
    uptime = int(time.monotonic() - started_monotonic)

    labels = {
        "service": "ytconvert",
    }

    metrics = {}
    if metrics_provider:
        try:
            metrics.update(metrics_provider())
        except Exception:
            pass

    resp = pb2.InfoResponse(
        app_name=cfg.get("app_name", "ytconvert"),
        instance_id=instance_id,
        host=host,
        version=cfg.get("version", ""),
        uptime=uptime,
        labels=labels,
        metrics=metrics,
        build_hash=cfg.get("build_hash", ""),
        build_time=cfg.get("build_time", ""),
    )
    return resp


def make_info_servicer(info_pb2_grpc, info_pb2, cfg, started_monotonic, metrics_provider=None):
    class InfoServicer(info_pb2_grpc.InfoServicer):
        def All(self, request, context):
            return build_info_response(info_pb2, cfg, started_monotonic, metrics_provider=metrics_provider)

    return InfoServicer()