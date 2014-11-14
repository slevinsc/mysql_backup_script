#!/bin/bash
#set -x
send_mail() {
	curl  -H "Content-Type: application/json"  -H "Accept: application/json" \
	      -X POST  -d "{\"subject\":\"$1\",\"body\":\"$2\"}" http://172.17.17.133:5000
}
if [ $# -lt 1 ];then
	echo "please Enter backup or recover"
	exit 1
fi
cat <<EOF
################################
       在数据库里必须添加此用户
grant reload,lock tables,replication client on *.* to 'mysqlbackup'@'localhost' identified by '000000';
flush privileges;
#########################################
EOF
date=`date +%F--%T`
datadir='/data'
backupname='test'
cpu_num=`cat /proc/cpuinfo | grep processor | wc -l`
backupLog='/var/log/mysql/mysql_backup.log'
backup_target_path='192.168.1.3::backup'
if [ -x "/usr/bin/innobackupex" ];then
	proc='/usr/bin/innobackupex'
else
	echo "innobackupex not found"
	exit 1
fi
if [ ! -x "/usr/bin/pigz" ];then
	echo "pigz not found"
	exit 1
fi
mysql_conf='/etc/my.cnf'
#backup_database='snettv.*'
if [ -d "/backupdir" ];then
	backup_dir="/backupdir"
else
	mkdir /backupdir
	backup_dir="/backupdir"
fi
host='localhost'
user='mysqlbackup'
password='000000'
if [[ $1 == "backup" ]];then
	echo "###############Begin backup database#################################" >> $backupLog
	$proc --host=$host --user=$user --password=$password  --rsync --compress --compress-threads=$cpu_num --parallel=$cpu_num $backup_dir >> $backupLog 2>&1
	#$proc --host=$host --user=$user --password=$password  --rsync  --parallel=$cpu_num $backup_dir > $backupLog 2>&1
	tail -1 $backupLog | grep "completed OK"
	if [ $? -eq 0 ];then
		tar_file=`ls $backup_dir`
		#tar zcf $backup_dir/mysqlbackup_$tar_file.tar.gz $backup_dir/$tar_file
		cd $backup_dir
		tar cf - $tar_file | pigz -9 -p $cpu_num > ${backupname}backup_${tar_file}.tar.gz
		if [ $? -eq 0 ];then
			echo "$date tar $tar_file success" >> $backupLog
			rm -rf $backup_dir/$tar_file
			rsync -a $backup_dir/${backupname}backup_${tar_file}.tar.gz $backup_target_path 
			if [ $? -eq 0 ];then
				echo "rsync success" >> $backupLog
                                rm -rf $backup_dir/${backupname}backup_${tar_file}.tar.gz
			else
				echo "rsync fail" >> $backupLog
                        	subject="rsync fail"
 				body="$date rsync $tar_file fail"
                        	send_mail $subject $body 
				exit 1
			fi
		else
			echo "$date tar $tar_file fail" >> $backupLog
                        subject="tar fail"
 			body="$date tar $tar_file fail"
                        send_mail $subject $body 
		#	rm -rf $backup_dir/$tar_file
			exit 1
		fi
	else
		echo "$date backup fail" >> $backupLog
		rm -rf $backup_dir/$tar_file
                subject="backup fail"
 		body="$date backup high fail"
                send_mail $subject $body 
		exit 1
	fi
fi
if [[ $1 == "recover" ]];then
	if [  $# -ne 2 ];then
		echo "recover file not found"
		exit 1
	fi
	if [ ! -x "/usr/bin/qpress" ];then
		echo "qpress not found"
		exit 1
	fi
	#cd /etc/swd
	#cd $datadir
	cat << EOF
		########################################################
		PID文件不能放在datadir目录下，
		如果在，请更改配置文件里的pid文件路径，
		然后重启，再进行恢复
		########################################################
EOF
		read -p "Begin recovery(y/n):" value
		if [[ $value == "y" ]];then
			if [ "`ls -A $datadir`" = "" ]; then
				echo "$datadir is indeed empty"
			else
				echo "$datadir is not empty"
				exit 1
			fi
			echo "Begin unzip....."
			recover_dir=`basename $2  | awk -F . '{print $1}' | awk -F ${backupname}backup_ '{print $2}'`
			tar xf $2 -C /tmp
			# tmp_dir=`pwd`
			# recover_dir=`ls $tmp_dir`
			cd /tmp/$recover_dir
			for i in `find . -iname "*\.qp"`; do qpress -d $i  $(dirname $i) && rm $i; done
			cd ..
			echo "Begin prepare...."
                        pwd
			$proc --defaults-file=/etc/my.cnf --apply-log --redo-only /tmp/$recover_dir/
			read -p "Whether to continue recovery(y/n):" value
			if [[ $value == "y" ]];then
				echo "Begin recover..."
				$proc --copy-back $recover_dir/
				cd $datadir
				chown -R mysql:mysql *
				/etc/init.d/mysqld restart
			else
				echo "program exit "
				exit 1
			fi
		else
			exit 1
		fi
fi

