#!/bin/sh

CLASSPATH=conf:tracker-api.jar
for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done
java -server -Dfile.encoding=UTF-8 -cp $CLASSPATH com.tracker.api.handler.WebStatsServiceServer > /dev/null 2>&1 &
