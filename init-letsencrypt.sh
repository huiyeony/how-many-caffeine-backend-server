#!/bin/bash

# 임시 더미 인증서 생성 (nginx 최초 시작용)
mkdir -p /etc/letsencrypt/live/api.howmanycaffeine.com
docker compose run --rm --entrypoint "\
  openssl req -x509 -nodes -newkey rsa:4096 -days 1 \
  -keyout /etc/letsencrypt/live/api.howmanycaffeine.com/privkey.pem \
  -out /etc/letsencrypt/live/api.howmanycaffeine.com/fullchain.pem \
  -subj '/CN=localhost'" certbot

# nginx 시작
docker compose up -d nginx

# 더미 인증서 삭제
docker compose run --rm --entrypoint "\
  rm -rf /etc/letsencrypt/live/api.howmanycaffeine.com" certbot

# 실제 인증서 발급
docker compose run --rm --entrypoint "\
  certbot certonly --webroot \
  --webroot-path=/var/www/certbot \
  --email huiyeony888@gmail.com \
  --agree-tos \
  --no-eff-email \
  -d api.howmanycaffeine.com" certbot

# nginx 재시작
docker compose exec nginx nginx -s reload

echo "Done! HTTPS is ready."
