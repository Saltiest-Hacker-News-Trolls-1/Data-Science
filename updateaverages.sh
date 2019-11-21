#!/bin/sh
if ps -Af | grep -v grep | grep -v log | grep updateaverages.py ; then
        echo "Updateaverages exists, not starting."
        exit 0
else
        echo "Updateaverages not found, starting."
        cd /srv/Data-Science/
        pwd
        /usr/local/bin/pipenv run ./updateaverages.py
        exit 0
fi


