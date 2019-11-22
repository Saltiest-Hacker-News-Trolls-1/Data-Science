#!/bin/sh
if ps -Af | grep -v grep | grep -v log | grep addusers.py ; then
        echo "Addusers exists, not starting."
        exit 0
else
        echo "Addusers not found, starting."
        cd /srv/Data-Science/
        pwd
        /usr/local/bin/pipenv run ./addusers.py
        exit 0
fi


