import logging
import threading
import time

from dotenv import load_dotenv

from config.app_cfg import load_app_cfg
from jobs.convert_job import JobStore
from services.grpc_srv import serve_grpc
from services.info_srv import make_info_servicer
from services.ytconvert_srv import make_converter_servicer
from utils.time_ut import monotonic
from utils.files_ut import ensure_dir

from proto import info_pb2, info_pb2_grpc
from proto import ytconvert_pb2, ytconvert_pb2_grpc


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("ytconvert")


def main():
    load_dotenv(".env")

    logger = _configure_logging()
    cfg = load_app_cfg()

    ensure_dir(cfg["workdir"])

    started_monotonic = monotonic()

    job_store = JobStore(cfg, logger)

    stop_event = threading.Event()
    cleanup_thread = threading.Thread(
        target=job_store.cleanup_loop,
        args=(stop_event,),
        name="cleanup",
        daemon=True,
    )
    cleanup_thread.start()

    def metrics_provider():
        try:
            return job_store.metrics_snapshot()
        except Exception:
            return {}

    def add_services(server):
        info_serv = make_info_servicer(
            info_pb2_grpc,
            info_pb2,
            cfg,
            started_monotonic,
            metrics_provider=metrics_provider,
        )
        info_pb2_grpc.add_InfoServicer_to_server(info_serv, server)

        conv_serv = make_converter_servicer(ytconvert_pb2_grpc, ytconvert_pb2, cfg, logger, job_store)
        ytconvert_pb2_grpc.add_ConverterServicer_to_server(conv_serv, server)

    server = serve_grpc(cfg, logger, add_services)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("shutdown requested")
    finally:
        stop_event.set()
        server.stop(grace=None)


if __name__ == "__main__":
    main()