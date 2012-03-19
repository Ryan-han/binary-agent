#!/usr/bin/env /bin/bash
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

usage="Usage: $0 (start|stop) (ftp) "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get arguments
startStop=$1
#shift
command=$2
#shift
agent_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${bin}/env.sh" ]; then
    source "${bin}/env.sh"
fi

if [ "$AGENT_HOME" = "" ]; then
  export AGENT_HOME=/usr/lib/agent
fi

if [ "$AGENT_LOG_DIR" = "" ]; then
  export AGENT_LOG_DIR="/var/log/agent"
fi

if [ "$AGENT_NICENESS" = "" ]; then
	export AGENT_NICENESS=0
fi 

if [ "$AGENT_PID_DIR" = "" ]; then
  AGENT_PID_DIR=/var/run/agent
fi

if [ "$AGENT_IDENT_STRING" = "" ]; then
  export AGENT_IDENT_STRING="$USER"
fi

# some variables
export AGENT_LOGFILE=agent-$AGENT_IDENT_STRING-$command-$HOSTNAME.log
export AGENT_ROOT_LOGGER="INFO,DRFA"
log=$AGENT_LOG_DIR/agent-$AGENT_IDENT_STRING-$command-$HOSTNAME.out
pid=$AGENT_PID_DIR/agent-$AGENT_IDENT_STRING-$command.pid

case $startStop in

  (start)
    mkdir -p "$AGENT_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    agent_rotate_log $log
    echo starting $command, logging to $log
    cd "$AGENT_HOME"
    nohup  nice -n ${AGENT_NICENESS} "${AGENT_HOME}"/bin/run $startStop $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac

