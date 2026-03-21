#!/bin/bash

DOMAIN=api.howmanycaffeine.com
EMAIL=huiyeony888@gmail.com
CONF_PATH=/home/ubuntu/caffeine-backend/nginx/nginx.conf

# 1단계: HTTP-only 임시 설정으로 nginx 시작
cat > $CONF_PATH << 'EOF'
server {
    listen 80;
    server_name api.howmanycaffeine.com;

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    location / {
        return 200 'ok';
    }
}
EOF

docker compose up -d nginx
sleep 3

# 2단계: 인증서 발급
docker compose run --rm --entrypoint "\
  certbot certonly --webroot \
  --webroot-path=/var/www/certbot \
  --email $EMAIL \
  --agree-tos \
  --no-eff-email \
  -d $DOMAIN" certbot

# 3단계: HTTPS 설정으로 복구
cat > $CONF_PATH << 'EOF'
server {
    listen 80;
    server_name api.howmanycaffeine.com;

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    location / {
        return 301 https://$host$request_uri;
    }
}

server {
    listen 443 ssl;
    server_name api.howmanycaffeine.com;

    ssl_certificate /etc/letsencrypt/live/api.howmanycaffeine.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.howmanycaffeine.com/privkey.pem;

    location / {
        proxy_pass http://api:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

docker compose exec nginx nginx -s reload

echo "Done! HTTPS is ready."
