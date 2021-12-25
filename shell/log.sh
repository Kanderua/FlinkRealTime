#!/usr/bin/env bash

# 启动nginx
# 启动3个日志服务器
nginx=/opt/module/nginx/sbin/nginx
logger_home=/opt/gmall0726
jar=logger-0.0.1-SNAPSHOT.jar

case $1 in
"start")
if [[ -z "`pgrep -f nginx`" ]]; then
    echo "在 hadoop162 启动nginx"
    $nginx
else
    echo " nginx 已经启动, 无需重新启动"
fi
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 启动日志服务器"
    ssh $host "nohup java -jar $logger_home/$jar 1>$logger_home/log.log 2>&1 &"
done
;;
"stop")
echo "在 hadoop162 停止 nginx"
$nginx -s stop
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 停止日志服务器"
    ssh $host "jps | awk '/$jar/{print \$1}'| xargs kill -9 "
done

;;
*)
echo "你的启动姿势不对: "
echo " 启动日志采集: log.sh start "
echo " 停止日志采集: log.sh stop "

;;
esac