#!/usr/bin/env bash
set -euo pipefail

AGENT_ID="${AGENT_ID:-binance-orderbook}"
SERVER_NAME="${SERVER_NAME:-_}"
CERT_CN="${CERT_CN:-localhost}"
LOCAL_API_PORT="${LOCAL_API_PORT:-18080}"
TLS_DIR="${TLS_DIR:-/etc/nginx/certs/${AGENT_ID}}"
TLS_CERT_PATH="${TLS_CERT_PATH:-${TLS_DIR}/fullchain.pem}"
TLS_KEY_PATH="${TLS_KEY_PATH:-${TLS_DIR}/privkey.pem}"
TEMPLATE_PATH="${TEMPLATE_PATH:-$(cd "$(dirname "$0")" && pwd)/nginx/binance-orderbook-api.conf.template}"
TARGET_PATH="${TARGET_PATH:-/etc/nginx/conf.d/${AGENT_ID}.conf}"

dnf install -y nginx openssl
mkdir -p "${TLS_DIR}"

if [[ ! -f "${TLS_CERT_PATH}" || ! -f "${TLS_KEY_PATH}" ]]; then
  openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "${TLS_KEY_PATH}" \
    -out "${TLS_CERT_PATH}" \
    -days 365 \
    -subj "/CN=${CERT_CN}"
fi

sed \
  -e "s|\${AGENT_ID}|${AGENT_ID}|g" \
  -e "s|\${SERVER_NAME}|${SERVER_NAME}|g" \
  -e "s|\${LOCAL_API_PORT}|${LOCAL_API_PORT}|g" \
  -e "s|\${TLS_CERT_PATH}|${TLS_CERT_PATH}|g" \
  -e "s|\${TLS_KEY_PATH}|${TLS_KEY_PATH}|g" \
  "${TEMPLATE_PATH}" > "${TARGET_PATH}"

nginx -t
systemctl enable --now nginx
systemctl reload nginx

echo "Nginx proxy installed for ${AGENT_ID}."
