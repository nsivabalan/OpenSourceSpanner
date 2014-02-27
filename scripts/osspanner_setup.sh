yes | sudo apt-get install git
git clone https://github.com/nsivabalan/OpenSourceSpanner.git
git checkout -b mavenfix
git pull origin mavenfix
cd OpenSourceSpanner
Y | sh scripts/java_setup.sh
Y | sh scripts/set_up_hbase.sh
sh scripts/zmq_set_up.sh
sh scripts/jzmq_set_up.sh
