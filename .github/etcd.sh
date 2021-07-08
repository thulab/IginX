#!/bin/sh

set -e

sh -c "wget -nv https://golang.org/dl/go1.16.5.linux-amd64.tar.gz"

sh -c "tar -zxvf go1.16.5.linux-amd64.tar.gz"

sh -c "export GOROOT=$PWD/go"

sh -c "export GOPATH=$GOROOT/bin"

sh -c "export PATH=$PATH:$GOPATH"

sh -c "echo $PATH"

sh -c "echo $GOROOT"

sh -c "echo $GOPATH"

sh -c "wget -nv https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz"

sh -c "tar -zxvf etcd-v3.5.0-linux-amd64.tar.gz"

sh -c "mv etcd-v3.5.0-linux-amd64/etcd* $GOPATH/bin"

sh -c "etcd"