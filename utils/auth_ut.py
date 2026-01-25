def extract_bearer_token(metadata):
    if not metadata:
        return ""
    for k, v in metadata:
        if (k or "").lower() != "authorization":
            continue
        vv = (v or "").strip()
        if vv.lower().startswith("bearer "):
            return vv[7:].strip()
        return vv
    return ""