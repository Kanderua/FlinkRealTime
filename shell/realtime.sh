#!/usr/bin/env bash

flink=/opt/module/flink-1.13.1/bin/flink
jar=/opt/gmall0726/realtime-1.0-SNAPSHOT.jar
apps=(
com.atgugu.realtime.app.dwd.DwdLogApp
)

runnings=`$flink list -r 2>/dev/null | awk '/RUNNING/ {print \$(NF-1)}'`

for app in ${apps[*]} ; do
    job_name=`echo $app | awk -F. '{print \$NF}'`

    if [[ "${runnings[@]}" =~ $job_name ]]; then
        echo "$job_name 已经启动, 无需重新启动"
    else
        echo "启动应用: $job_name"
        $flink run -d -c $app $jar
    fi

done

