[ytconvert](https://github.com/sphynkx/ytconvert) is supplemental service for [yurtube app](https://github.com/sphynkx/yurtube), based on gRPC+protobuf and ffmpeg. It performs conversion videos into different video/audio formats. Service receives from app source video and list of formats to recode. Then sends to app results of conversion.


## Install and configure.
```bash
dnf install --nogpgcheck   https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm
dnf install -y ffmpeg ffmpeg-devel pkgconf-pkg-config redis python3 python3-devel gcc gcc-c++ grpcurl
cd /opt
git clone https://github.com/sphynkx/ytconvert
cd ytconvert
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r install/requirements.txt
deactivate
cp install/.env.example .env
```
Configure `.env` with your options - set `YTCONVERT_HOST` and `YTCONVERT_PORT` to your actual network parameters.


## Run service
Manually run:
```bash
chmod + x run.sh
./run.sh
```
Or configure and run as systems service:
```bash
cp install/ytconvert.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ytconvert
journalctl -u ytconvert -f
```


## Tests
Simple health test:
```bash
grpcurl -plaintext 192.168.7.3:9096 grpc.health.v1.Health/Check
```

Total service info:
```bash
grpcurl -plaintext 192.168.7.3:9096 grpc.health.v1.Info/All
```

Create job (2 variants: 720p video and 128k audio):
```bash
grpcurl -plaintext \
  -d '{
    "video_id":"vid_test_001",
    "idempotency_key":"idem-001",
    "source":{
      "storage":{"address":"192.168.7.3:9092","tls":false,"token":""},
      "rel_path":"test.mp4"
    },
    "output":{
      "storage":{"address":"192.168.7.3:9092","tls":false,"token":""},
      "base_rel_dir":"."
    },
    "variants":[
      {"variant_id":"v:720p:h264+aac:mp4", "label":"720p", "kind":"VIDEO", "container":"mp4"},
      {"variant_id":"a:128k:aac:m4a", "label":"Audio 128k", "kind":"AUDIO", "container":"m4a"}
    ]
  }' \
  192.168.7.3:9096 ytconvert.v1.Converter/SubmitConvert
```
Use received __JOB_ID__ for next commands.

Job status:
```bash
grpcurl -plaintext -d '{"job_id":"<JOB_ID>"}' 192.168.7.3:9096 ytconvert.v1.Converter/GetStatus
```

Monitor conversion process:
```bash
grpcurl -plaintext -d '{"job_id":"<JOB_ID>"}' 192.168.7.3:9096 ytconvert.v1.Converter/GetPartialResult
```

Job info and status (stream):
```bash
grpcurl -plaintext -d '{"job_id":"<JOB_ID>","send_initial":true}' 192.168.7.3:9096 ytconvert.v1.Converter/WatchJob
```

Final results:
```bash
grpcurl -plaintext -d '{"job_id":"<JOB_ID>"}' 192.168.7.3:9096 ytconvert.v1.Converter/GetResult
```

Download conversion result to file (for example for `v:720p:h264+aac:mp4`, artifact `main`):
```bash
grpcurl -plaintext -d '{"job_id":"<JOB_ID>","variant_id":"v:720p:h264+aac:mp4","artifact_id":"main","offset":0}' 192.168.7.3:9096 ytconvert.v1.Converter/DownloadResult > out.bin
```
