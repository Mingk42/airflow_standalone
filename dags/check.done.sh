#!/bin/bash

echo "check.done"
           
#DONE_PATH="~/data/done/{{logical_date.strftime('%y%m%d')}}"
DONE_PATH=~/data/done/$1
DONE_PATH_FILE="$DONE_PATH/_DONE"
echo $DONE_PATH_FILE
cat $DONE_PATH_FILE
            
if [ -e "$DONE_PATH_FILE" ]; then
	figlet "Let's move on!"
	exit 0
else
	echo "I'll be back => $DONE_PATH_FILE"
	exit 1
fi
