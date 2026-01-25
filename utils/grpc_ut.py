from google.protobuf import struct_pb2


def dict_to_struct(d):
    s = struct_pb2.Struct()
    if not d:
        return s
    s.update(d)
    return s


def struct_to_dict(s):
    if s is None:
        return {}
    return dict(s)