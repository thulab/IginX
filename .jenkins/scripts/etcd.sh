#!/bin/sh

set -e

sh -c "wget -nv https://golang.org/dl/go1.16.5.linux-amd64.tar.gz"

sh -c "tar -zxvf go1.16.5.linux-amd64.tar.gz"

export GOROOT=$PWD

export GOPATH=$GOROOT/go

export GOBIN=$GOPATH/bin

export PATH=$PATH:$GOBIN

sh -c "wget -nv https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz"

sh -c "tar -zxvf etcd-v3.5.0-linux-amd64.tar.gz"

sh -c "mv etcd-v3.5.0-linux-amd64/etcd* $GOBIN"

sh -c "nohup etcd >> run.log 2>&1 &"