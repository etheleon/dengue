# Introduction

This readme details how you'll go about and running the predict function. By default the model chosen will be `sero_climate`.

# Build

We are currently storing the imager in dockerhub

```bash

IMG=etheleon/dengue_inla:latest
docker buildx build -t $IMG .
```

```bash
OUTPUT_DIR=/home/wesley/model_output
docker run --rm -v $OUTPUT_DIR:/workspace/output $IMG
```
