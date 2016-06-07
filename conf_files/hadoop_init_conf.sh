# this script executes all the haddop deamons
echo "registering key for secondary namenode"
ssh-keyscan localhost,0.0.0.0 >> /home/vagrant/.ssh/known_hosts;
echo "formating nodes and running deamons"
/home/vagrant/hadoop-2.7.2/bin/hdfs namenode -format
/home/vagrant/hadoop-2.7.2/bin/hdfs datanode -format
/home/vagrant/hadoop-2.7.2/sbin/start-dfs.sh
/home/vagrant/hadoop-2.7.2/sbin/start-yarn.sh


