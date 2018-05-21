#!/bin/bash

CENTROID_FILE=centroid.txt
DATA_FILE=data.txt

rm $CENTROID_FILE
rm $DATA_FILE

for i in `seq 0 100000`;
do
    echo $i >> $DATA_FILE
done

for i in `seq 0 10000 100000`;
do
    echo $i >> $CENTROID_FILE
done
