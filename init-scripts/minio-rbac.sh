#!/usr/bin/env sh
set -eu

# Minimal MinIO RBAC bootstrap. This preserves the existing root/admin account
# and adds service users for normal pipeline access.

: "${MINIO_ENDPOINT:=http://minio:9000}"
: "${MINIO_ACCESS_KEY:=adminminio}"
: "${MINIO_SECRET_KEY:=adminminio}"
: "${MINIO_WRITER_ACCESS_KEY:=bdm_writer}"
: "${MINIO_WRITER_SECRET_KEY:=bdm_writer_password}"
: "${MINIO_READER_ACCESS_KEY:=bdm_reader}"
: "${MINIO_READER_SECRET_KEY:=bdm_reader_password}"

mc alias set bdm-minio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

for bucket in landing-zone trusted-zone exploitation-zone; do
  mc mb --ignore-existing "bdm-minio/${bucket}"
done

mc admin policy create bdm-minio landing-writer /config/minio_policies/landing_writer_policy.json || true
mc admin policy create bdm-minio landing-reader /config/minio_policies/landing_reader_policy.json || true

mc admin user add bdm-minio "${MINIO_WRITER_ACCESS_KEY}" "${MINIO_WRITER_SECRET_KEY}" || true
mc admin user add bdm-minio "${MINIO_READER_ACCESS_KEY}" "${MINIO_READER_SECRET_KEY}" || true

mc admin policy attach bdm-minio landing-writer --user "${MINIO_WRITER_ACCESS_KEY}"
mc admin policy attach bdm-minio landing-reader --user "${MINIO_READER_ACCESS_KEY}"

echo "MinIO RBAC bootstrap completed. Existing root/admin access is unchanged."
