#!/bin/sh
apk add jq
export nonce=$(jq -r .nonce /var/opentripplanner/otp-runner-server-only-manifest.json)
touch /etc/caddy/static/status-new.json
cat /etc/caddy/static/status.json | jq --arg n $nonce '.nonce = $n' > /etc/caddy/static/status-new.json
rm /etc/caddy/static/status.json
mv /etc/caddy/static/status-new.json /etc/caddy/static/status.json