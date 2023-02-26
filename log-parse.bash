#!/bin/bash

LOG=$1
TEMPFILE=$(mktemp)

grep ' com.daml.lf.engine.trigger.Runner ' $LOG | while read line; do
  echo "$line" | sed -r 's/([0-9-]* [0-9:.]*) .*\] ([a-zA-Z]*) .* - (.*) , context: (.*)/{timestamp: "\1", level: "\2", message: "\3", context: \4}/' | yq -o=json . >>$TEMPFILE
done

jq 'select(.context.trigger) | { "@timestamp": .timestamp, level: .level, trigger: .context.trigger, message: .message }' $TEMPFILE | jq -s '.'
