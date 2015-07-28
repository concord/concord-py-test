#!/bin/bash --login
set -ex
if [[ "$(which virtualenv)" == "" ]]; then
    if [[  "$(which apt-get)" != "" ]]; then
        echo "INSTALLING TOOLS: "
        sudo apt-get update
        sudo apt-get install software-properties-common python-dev python-pip -y
        sudo pip install virtualenv
    fi
fi

/usr/local/bin/virtualenv env
