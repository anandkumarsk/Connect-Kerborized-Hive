#!/bin/bash

##### Constants

TITLE=" Java Program executed from machine  $HOSTNAME"

class_name="class name"
jar_name="jar file  name"
program_path=`pwd`

# variables required to start/stop/restart the process 
PIDFILE="$program_path/PID/$0.pid"
LOGFILE="$program_path/logs/shell/$0-`date "+%Y-%m-%d-%H:%M:%S"`.log"
JAVA_CMD="java -cp"

#run the init.d function script
. /etc/init.d/functions



cluster_entered=0
zookeeper_entered=0
broker_entered=0
role_entered=0
groupname_entered=0
topicname_entered=0
no_of_threads_entered=0
no_of_events_entered=0
action_entered=0
output_entered=0
email_entered=0

#default value
email=turnOn

#uncomment the following default values only at final deployment in Production 
cluster="cluster name"
cluster_entered=1
role="consumer"
role_entered=1



##### Functions

function usage
{
    echo " Message from Shell Script: usage: $0 ([--action start|stop|restart|status ][--role consumer|producer] [--cluster cluster name] - required ) [-z zookeeper:port] [-b broker:port] [-g groupname] [-t topic-name] [-n numberofThreads] [--events 100] [-m|--email turnOn|turnOff][--help]]"
}
	

##### Main

if [ "$1" = "" ]; then
				usage
				exit 1
fi
		

while [ "$1" != "" ]; do
    case $1 in
    -r | --role )           shift
        						role_entered=1
                                role=$1
                                ;;
	-c | --cluster )           shift
        						cluster_entered=1
                                cluster=$1
                                ;;
 	-z | --zookeeper )    shift
				      		  zookeeper_entered=1
				              zookeeper=$1
					          ;;
	-b | --broker )    shift
				      		  broker_entered=1
				              broker=$1
					          ;;
    
        
	-g | --groupname )    shift
                              groupname_entered=1
                              groupname_newvalue=$1
                              ;;

	-t | --topicname )    shift
                              topicname_entered=1
                              topicname=$1
                              ;;
	-n | --numberofThreads )    shift
            	                no_of_threads_entered=1
                	            no_of_threads=$1
                    	          ;;
	-e | --events )    		shift
                              	no_of_events_entered=1
                              	no_of_events=$1
                                	;;
	-a | --action )    		shift
                              	action_entered=1
                              	action=$1
                                ;;
	-o | --output )    		shift
                              	output_entered=1
                              	output=$1
                                ;;							
	-m | --email )    		shift
                              	email_entered=1
                              	email=$1
                                ;;
	                                
	-h | --help )               usage
                                exit
                                	;;
        * )                     usage
                                exit 1
    esac
    shift
done

if [ $action_entered -eq 0 ]
		then
			echo " Message from Shell Script: input for action is required"
			exit 1
fi

if [ $cluster_entered -eq 0 ]
		then
			echo " Message from Shell Script: cluster name is required"
			exit 1
fi

if [ $role_entered -eq 0 ]
		then
			echo " Message from Shell Script: Role is required"
			exit 1
fi


function cluster1 {


		
		
			if [ $zookeeper_entered -eq 0 ]
			then
			zookeeper="set zoo keeper"
			fi
			
			if [ $broker_entered -eq 0 ]
			then
			broker="set broker"
			fi
							
			if [ $groupname_entered -eq 0 ]
			then
			
			groupname="groupx"
			
			fi
			
			if [ $topicname_entered -eq 0 ]
			then
			topicname="topic_name"
			fi
			
			if [ $no_of_threads_entered -eq 0 ]
			then
			no_of_threads="1"
			fi
			
			if [ $no_of_events_entered -eq 0 ]
			then
			no_of_events="5"
			fi
			
			
}



start() {

		
		if [ $cluster_entered -eq 1 ]
		then
			#echo " Message from Shell Script: Cluster entered"
			if [ "$cluster"	= "Cluster1" ]
			then
			Cluster1
			fi	
			if [ "$cluster"	= "Cluster2" ]
			then
			Cluster2
			fi
	
		fi
	
		
        echo -n " Trying to start the Daemon: with following values" 
		
								echo " Message from Shell Script: role:$role"
								echo " Message from Shell Script: zookeeper:$zookeeper"
								echo " Message from Shell Script: broker:$broker"
								echo " Message from Shell Script: groupname:$groupname"
								echo " Message from Shell Script: topicname:$topicname"
								echo " Message from Shell Script: no_of_threads:$no_of_threads"
								echo " Message from Shell Script: no_of_events:$no_of_events"
								echo " Message from Shell Script: cluster:$cluster"
		
        
		if [ -f $PIDFILE$role ]; then
                PID=`cat $PIDFILE$role`
                echo "process is already running : $PID"
				exit 1
		else
			
			if [ $output_entered -eq 1 ] 
			then
			echo starting the program in Console
			echo "firing the command $JAVA_CMD $program_path/$jar_name -Denv=$cluster -DemailSwitch=turnON $class_name $role $zookeeper $groupname $topicname $no_of_threads $no_of_events $broker"
			$JAVA_CMD $program_path/$jar_name -Denv=$cluster -DemailSwitch=$email $class_name $role $zookeeper $groupname $topicname $no_of_threads $no_of_events $broker & echo $! > $PIDFILE$role
			#PID=`$JAVA_CMD $program_path/$jar_name -Denv=$cluster -DemailSwitch=turnON $class_name $role $zookeeper $groupname $topicname $no_of_threads $no_of_events $broker`
			PID=$!
			echo $PID > $PIDFILE$role
            echo `cat $PIDFILE$role`
			else
			PID=`$JAVA_CMD $program_path/$jar_name -Denv=$cluster -DemailSwitch=$email $class_name $role $zookeeper $groupname $topicname $no_of_threads $no_of_events $broker >$LOGFILE$role 2>&1 & echo $! > $PIDFILE$role`
            echo `cat $PIDFILE$role`
			
			fi


			
			
        fi
		

		}

stop() {
        echo -n "Shutting down process daemon: "
		if [ -f $PIDFILE$role ]; then
                PID=`cat $PIDFILE$role`
                echo Process ID is : $PID
				echo "Killing the process kill -9 $PID"
				kill -9 $PID
				echo "Removing PID file"
				rm -f $PIDFILE$role
				exit 1
        else
			echo "Could n't find the process, either the process is terminated or stopped"	
		fi

		
        return 0
			
		}
		
status() {
		
		if [ -f $PIDFILE$role ]; then
                PID=`cat $PIDFILE$role`
                #echo my file-status-email process is running: $PID
				ps -p $PID
				a=$(echo $?)

			if test $a -ne 0
			then
				echo "PID file is there but service is not running, so sending email and removing PID file"
				echo "file-status-email service down" | mail -s "Your Service is DOWN in `hostname -f` and restart it now" emailid@domain.com
				#rm -f $PIDFILE$role
				exit 1
			else
				sleep 0
			fi
		else
		echo "Process is being stopped by the script"
		
        fi
	
		}


if [ $action_entered -eq 1 ] 
then

case "$action" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
		status	
		;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage:  {start|stop|status|restart}"
        exit 1
        ;;
esac
exit $?

fi #[ action_entered -eq 1 ] 


