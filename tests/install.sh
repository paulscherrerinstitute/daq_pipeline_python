mkdir -p /data/scylla/commitlog
mkdir -p /data/scylla/hints
mkdir -p /data/scylla/saved_caches
mkdir -p /data/scylla/view_hints

yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh

curl http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-3.1.repo > /etc/yum.repos.d/scylla-3.1.repo
yum -y install scylla

# vim /etc/scylla/scylla.yaml

chown -R scylla:scylla /data/scylla/
scylla_setup --no-raid-setup --nic eth0 --no-sysconfig-setup --no-version-check --no-node-exporter
systemctl start scylla-server.service