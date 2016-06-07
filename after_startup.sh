#! /bin/bash

ssh-keyscan -t rsa master slave > /home/vagrant/.ssh/known_hosts

ssh-keyscan -t dsa master slave >> /home/vagrant/.ssh/known_hosts

for f in $( ls /vagrant/files/ssh/*.pub ); do cat $f >> /home/vagrant/.ssh/authorized_keys; done;
