#!/usr/bin/env bash
set -euo pipefail

PRUNE=0
DEEP_PRUNE=0

for arg in "$@"; do
  case "$arg" in
    --prune)
      PRUNE=1
      ;;
    --deep-prune)
      DEEP_PRUNE=1
      ;;
    *)
      echo "Unknown option: $arg"
      echo "Usage: ./run.sh [--prune] [--deep-prune]"
      exit 1
      ;;
  esac
done

has_nvidia_gpu() {
  if command -v nvidia-smi >/dev/null 2>&1; then
    if nvidia-smi >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

if [ "$PRUNE" -eq 1 ]; then
  echo "Cleaning Docker build cache..."
  docker builder prune -f

  echo "Cleaning dangling images..."
  docker image prune -f
fi

if [ "$DEEP_PRUNE" -eq 1 ]; then
  echo "Deep cleaning unused Docker images and build cache. Volumes will NOT be removed..."
  docker builder prune -a -f
  docker image prune -a -f
fi

if has_nvidia_gpu; then
  echo "NVIDIA GPU detected. Starting full stack with GPU support and 3 Spark workers..."
  COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.gpu.yml)
  HAS_GPU=1
else
  echo "No NVIDIA GPU detected. Starting full stack without GPU and with 3 Spark workers..."
  COMPOSE_FILES=(-f docker-compose.yml)
  HAS_GPU=0
fi

echo ""
echo "Step 1: Starting full stack without scaling Spark workers first..."
docker compose "${COMPOSE_FILES[@]}" up -d

echo ""
echo "Step 2: GPU verification..."

if [ "$HAS_GPU" -eq 1 ]; then
  echo "Verifying CUDA in Jupyter..."
  docker compose "${COMPOSE_FILES[@]}" exec jupyter python -c "import torch; print('CUDA available:', torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO GPU')"
else
  echo "GPU verification skipped because no NVIDIA GPU was detected."
fi

echo ""
echo "Step 3: Scaling Spark workers to 3..."
docker compose "${COMPOSE_FILES[@]}" up -d --scale spark-worker=3

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