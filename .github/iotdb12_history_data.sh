#!/bin/sh

sed -i "s/has_data=false/has_data=true#data_prefix=ln.wf01/g" conf/config.properties
