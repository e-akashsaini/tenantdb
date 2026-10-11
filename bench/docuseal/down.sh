#!/usr/bin/env bash
docker rm -f docuseal >/dev/null 2>&1 || true
pkill -x tenantdb 2>/dev/null || true
echo stopped
