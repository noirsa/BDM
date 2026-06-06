#!/usr/bin/env bash
set -euo pipefail

has_nvidia_gpu() {
  if command -v nvidia-smi >/dev/null 2>&1; then
    if nvidia-smi >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

if has_nvidia_gpu; then
  echo "NVIDIA GPU detected. Starting full stack with GPU support and 3 Spark workers..."
  COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.gpu.yml)
  HAS_GPU=1
else
  echo "No NVIDIA GPU detected. Starting full stack without GPU and with 3 Spark workers..."
  COMPOSE_FILES=(-f docker-compose.yml)
  HAS_GPU=0
fi

docker compose "${COMPOSE_FILES[@]}" up --build -d --scale spark-worker=3

echo ""
echo "Full stack started."
echo ""
echo "Useful local URLs:"
echo "  Jupyter          http://localhost:8888    token: jupyter"
echo "  Airflow          http://localhost:8080    user/password: airflow / airflow"
echo "  MinIO Console    http://localhost:9001    credentials from .env"
echo "  Spark Master UI  http://localhost:8082    confirm Workers (3)"
echo "  Kafka UI         http://localhost:8081"
echo "  Mongo Express    http://localhost:8083"
echo "  Attu             http://localhost:3000"
echo "  Superset         http://localhost:8088"
echo ""

if [ "$HAS_GPU" -eq 1 ]; then
  echo "Verifying CUDA in Jupyter..."
  docker compose "${COMPOSE_FILES[@]}" exec jupyter python -c "import torch; print('CUDA available:', torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO GPU')"
else
  echo "GPU verification skipped because no NVIDIA GPU was detected."
fi

echo ""
echo "Check Spark workers:"
echo "  Open http://localhost:8082 and confirm Workers (3)"