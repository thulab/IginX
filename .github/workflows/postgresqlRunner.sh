#!/bin/sh

set -e
sudo sh -c "apt-get install postgresql postgresql-client"
sh -c "sleep 20"
sudo sh -c "/etc/init.d/postgresql start"