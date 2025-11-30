#!/bin/bash

wget -O aggregatebatteriesEss.zip https://github.com/gurkc006/dbus-aggregate-batteries/archive/refs/heads/main.zip
unzip -o aggregatebatteriesEss.zip
rm aggregatebatteriesEss.zip
/bin/cp -rf dbus-aggregate-batteries-main/* .
/bin/rm -R dbus-aggregate-batteries-main

# fix permissions, owner and group
if [ -d "/data/apps/dbus-aggregate-batteries" ]; then
    chmod +x /data/apps/dbus-aggregate-batteries/*.sh
    chmod +x /data/apps/dbus-aggregate-batteries/*.py
    chmod +x /data/apps/dbus-aggregate-batteries/service/run
    chmod +x /data/apps/dbus-aggregate-batteries/service/log/run
fi
