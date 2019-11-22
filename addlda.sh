#!/bin/sh
if ps -Af | grep -v grep | grep -v log | grep addlda.py ; then
        echo "Addlda exists, not starting."
        exit 0
else
        echo "Addlda not found, starting."
        cd /srv/Data-Science/
        pwd
        /usr/local/bin/pipenv run ./addlda.py
        exit 0
fi


