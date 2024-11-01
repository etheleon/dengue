# Introduction

This readme details how you'll go about and run the predict function. By default, the model chosen will be `sero_climate`.

# Build

We are currently storing the image in Docker Hub.

```bash
IMG=etheleon/dengue_inla:latest
docker buildx build -t $IMG .
```

```bash
OUTPUT_DIR=/home/wesley/model_output
docker run --rm -v $OUTPUT_DIR:/workspace/output $IMG
```

# DB Connection

You'll need to declare your database connection details in `settings.toml` and `.secrets.toml` using Dynaconf.

`settings.toml`:
```toml
[default]
DB_HOST = "your_database_host"
DB_PORT = "your_database_port"
DB_USER = "your_database_user"
DB_NAME = "your_database_name"
```

`.secrets.toml`:
```toml
[default]
DB_PASSWORD = "your_database_password"
```

Make sure to replace the placeholder values with your actual database connection details.