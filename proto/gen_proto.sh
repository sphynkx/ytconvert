#!/usr/bin/env bash
set -euo pipefail

# Generate stubs from ytconvert.proto
# Make sure that ytconvert.proto is identical to one from yurtube service!!
# And ytstorage.proto is identical to one from ytstorage service!!

cd "$(dirname "$0")"

source ../.venv/bin/activate 

python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  ytconvert.proto

sed -i 's/^import ytconvert_pb2 as ytconvert__pb2/from . import ytconvert_pb2 as ytconvert__pb2/' ytconvert_pb2_grpc.py

echo "Generated: ytconvert_pb2.py ytconvert_pb2_grpc.py in $(pwd)"

##############
python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  ytstorage.proto

sed -i 's/^import ytstorage_pb2 as ytstorage__pb2/from . import ytstorage_pb2 as ytstorage__pb2/' ytstorage_pb2_grpc.py

echo "Generated: ytstorage_pb2.py ytstorage_pb2_grpc.py in $(pwd)"

##############
python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  info.proto

sed -i 's/^import info_pb2 as info__pb2/from . import info_pb2 as info__pb2/' info_pb2_grpc.py

echo "Generated: info_pb2.py info_pb2_grpc.py in $(pwd)"
