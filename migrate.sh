#!/usr/bin/env bash

mkdir -p ${3}data/sequence/root.${2}

mkdir -p ${3}data/unsequence/root.${2}

mv ${1}data/sequence/root.${2} ${3}data/sequence

mv ${1}data/unsequence/root.${2} ${3}data/unsequence

for file in `ls ${1}wal`;
do
  if [[ ${file} == root.${2}* ]];
  then
    mkdir -p ${3}wal/${file}
    mv ${1}wal/${file} ${3}wal
  fi
done

# TODO
rm -rf ${3}system

mv -f ${1}system ${3}

#for file in `ls ${3}system/storage_groups`;
#do
#  if [[ ${file} == root.${2}* ]];
#  then
#    echo "Skip it!"
#  else
#    rm -rf ${3}system/storage_groups/${file}
#  fi
#done