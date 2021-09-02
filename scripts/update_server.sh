#!/usr/bin/env bash

set -e # stop on error
set -u # stop on not initialized var
set -x # add debug

#if [ -z "$SERVER" ]; then
#  echo "Please set the env var SERVER"
#  exit 1
#fi
#if [ -z "$USER" ]; then
#  echo "Please set the env var USER"
#  exit 1
#fi
#if [ -z "$PORT" ]; then
#  echo "Please set the env var PORT"
#  exit 1
#fi
#if [ -z "$SUDO_PASSWORD" ]; then
#  echo "Please set the env var SUDO_PASSWORD"
#  exit 1
#fi

ssh $USER@$SERVER -p$PORT '
cd /opt/arlo/core
git stash
git pull
git stash pop
/opt/arlo/core/arlo_venv/bin/pip install -r /opt/arlo/core/requirements.txt
echo '$SUDO_PASSWORD' | sudo -S systemctl restart arlo
'
