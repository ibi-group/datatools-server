#!/bin/sh
apk add jq
export nonce=$(jq .nonce /var/opentripplanner/otp-runner-server-only-manifest.json)
touch /etc/caddy/static/status-new.json
jq '.nonce = \"$nonce\"' /etc/caddy/static/status.json > /etc/caddy/static/status-new.json
rm /etc/caddy/static/status.json
mv /etc/caddy/static/status-new.json /etc/caddy/static/status.json