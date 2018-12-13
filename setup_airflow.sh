#!/bin/bash

# determine the absolute path of the passed resource
abs_path() {
    if [ -d $1 ]; then
        # is directory
        echo "$( cd "$1" && pwd )"
    else
        # is file
        echo "$( cd "$( dirname "$1" )" && pwd)/$( basename $1 )"
    fi
}

# determine the folder of this script ... 
script_location="$(abs_path "$( dirname "${BASH_SOURCE[0]}" )" )"

# ... which then will be assumed to be the $AIRFLOW_HOME location
export AIRFLOW_HOME="${script_location}"
echo "\$AIRFLOW_HOME was set to '${AIRFLOW_HOME}'..."
cd $AIRFLOW_HOME

if [[ "$1" == "scheduler" ]] || [[ "$1" == "s" ]]; then
    # additionally, start the Airflow scheduler
    echo "Starting Scheduler..."
    airflow scheduler
elif [[ "$1" == "webserver" ]] || [[ "$1" == "w" ]]; then
    # additionally, start the Airflow webserver
    echo "Starting webserver..."
    airflow webserver
fi
