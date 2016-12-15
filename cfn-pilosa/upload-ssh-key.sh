#!/bin/bash

for i in $(ls ~/.ssh/id_*.pub); do
    SSH_FILE=$i
done

read -p "SSH public key (default: \"$SSH_FILE\") "

if [ ! -z "$REPLY" ]; then
    SSH_FILE=$REPLY
fi

if [ ! -e "$SSH_FILE" ]; then
    echo File "$SSH_FILE" does not exist!
    exit 1
fi

KEY_NAME=$(grep -oE '[^ ]+$' $SSH_FILE)

read -p "Upload key \"$KEY_NAME\"? (y/n) " -n 1 -r
echo

if [ "$REPLY" == "y" ]; then
    echo Uploading key ${KEY_NAME}...
    ./env/bin/aws ec2 import-key-pair --key-name $KEY_NAME --public-key-material "$(cat $SSH_FILE)"
else
    echo "Aborting"
    exit 1
fi
