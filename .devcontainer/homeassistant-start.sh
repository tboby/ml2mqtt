#!/bin/sh
set -eu

rm -f /config/.ha_run.lock
exec /init
