#!/bin/bash
# Basic entrypoint script; parses passed arg and starts app
# by default
set -e

if [ -n "$1" ]; then
    ARG="$(echo "$1" | tr "[:upper:]" "[:lower:]")"
else
    ARG="start"
fi

case "$ARG" in
    "start")
        cd /app
        echo "I start my app here"
        ;;
    "shell")
        exec /bin/sh
        exit
        ;;
    *)
        exec "$ARG"
        exit
        ;;
esac
