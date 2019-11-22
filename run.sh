#!/bin/sh
if ps -Af | grep -v grep | grep -v log | grep run.py ; then
        echo "Run exists, not starting."
        exit 0
else
        echo "Run not found, starting."
        cd /srv/Data-Science/
        pwd
        /usr/local/bin/pipenv run ./run.py
        exit 0
fi

