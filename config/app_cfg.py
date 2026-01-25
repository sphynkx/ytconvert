import os


def _env(name, default=None):
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    if v == "":
        return default
    return v


def _env_int(name, default):
    v = _env(name, None)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def load_app_cfg():
    cfg = {}

    cfg["app_name"] = _env("APP_NAME", "ytConvert")
    cfg["version"] = _env("APP_VERSION", "0.1.0")
    cfg["instance_id"] = _env("INSTANCE_ID", "")
    cfg["host"] = _env("HOSTNAME", "")
    cfg["grpc_port"] = _env_int("YTCONVERT_PORT", 9096)
    cfg["advertise_host"] = _env("YTCONVERT_HOST", "")
    cfg["token"] = _env("YTCONVERT_TOKEN", "")

    cfg["redis_url"] = _env("YTCONVERT_REDIS_URL", "")
    cfg["redis_prefix"] = _env("YTCONVERT_REDIS_PREFIX", "ytconvert:")

    p = cfg["redis_prefix"] or "ytconvert:"
    if not p.endswith(":"):
        p = p + ":"
    cfg["redis_prefix"] = p

    cfg["workdir"] = _env("YTCONVERT_WORKDIR", "/tmp/ytconvert/jobs")
    cfg["ttl_seconds"] = _env_int("YTCONVERT_TTL_SECONDS", 24 * 3600)

    cfg["ffmpeg_bin"] = _env("YTCONVERT_FFMPEG", "ffmpeg")
    cfg["ffprobe_bin"] = _env("YTCONVERT_FFPROBE", "ffprobe")

    cfg["max_upload_bytes"] = _env_int("YTCONVERT_MAX_UPLOAD_BYTES", 0)

    cfg["download_chunk_bytes"] = _env_int("YTCONVERT_DOWNLOAD_CHUNK_BYTES", 1024 * 1024)

    cfg["watch_heartbeat_sec"] = _env_int("YTCONVERT_WATCH_HEARTBEAT_SEC", 10)

    return cfg