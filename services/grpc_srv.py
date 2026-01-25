import grpc
from concurrent import futures

from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from grpc_reflection.v1alpha import reflection

from utils.auth_ut import extract_bearer_token


class TokenAuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, required_token, allow_methods=None):
        self.required_token = required_token or ""
        self.allow_methods = set(allow_methods or [])

    def intercept_service(self, continuation, handler_call_details):
        if not self.required_token:
            return continuation(handler_call_details)

        method = handler_call_details.method or ""
        if method in self.allow_methods:
            return continuation(handler_call_details)

        token = extract_bearer_token(handler_call_details.invocation_metadata)
        if token != self.required_token:
            def deny(request_or_iterator, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing token")
            return grpc.unary_unary_rpc_method_handler(deny)

        return continuation(handler_call_details)


def serve_grpc(cfg, logger, add_services_fn):
    port = int(cfg.get("grpc_port") or 9096)

    allow = set([
        "/grpc.health.v1.Health/Check",
        "/grpc.health.v1.Info/All",
    ])

    interceptor = TokenAuthInterceptor(cfg.get("token", ""), allow_methods=allow)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        interceptors=[interceptor],
        options=[
            ("grpc.max_receive_message_length", 128 * 1024 * 1024),
            ("grpc.max_send_message_length", 128 * 1024 * 1024),
        ],
    )

    hs = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(hs, server)
    hs.set("", health_pb2.HealthCheckResponse.SERVING)

    # Register app services (Info, Converter, etc.)
    add_services_fn(server)

    # Enable reflection after everything is registered.
    service_names = [
        health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
        "grpc.health.v1.Info",
        "ytconvert.v1.Converter",
        reflection.SERVICE_NAME,
    ]
    reflection.enable_server_reflection(service_names, server)

    server.add_insecure_port(f"[::]:{port}")

    logger.info("gRPC listening on 0.0.0.0:%d", port)
    server.start()
    return server