cd /data/pem && 
ssh -i 'grbds.pem' ec2-user@ec2-3-00-00-000.ap-northeast-2.compute.amazonaws.com /home/ec2-user/.local/bin/airflow trigger_dag delete_serviceKey $1