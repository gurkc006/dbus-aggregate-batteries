#!/bin/bash

wget -O aggregatebatteriesEss.zip https://github.com/gurkc006/dbus-aggregate-batteries/archive/refs/heads/main.zip
unzip -o aggregatebatteriesEss.zip
rm aggregatebatteriesEss.zip
/bin/cp -rf dbus-aggregate-batteries-main/* .
/bin/rm -R dbus-aggregate-batteries-main