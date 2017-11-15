ps -ef|grep srunner|grep -v grep|cut -c 9-15|xargs kill -9
ps -ef|grep crunner|grep -v grep|cut -c 9-15|xargs kill -9
ps -ef|grep lrunner|grep -v grep|cut -c 9-15|xargs kill -9
ps -ef|grep trunner|grep -v grep|cut -c 9-15|xargs kill -9
