mkdir ~/temp
cd ~/temp
cp ~/OpenSourceSpanner/lib/zeromq-3.2.3.tar.gz .
tar xvzf zeromq-3.2.3.tar.gz
cd zeromq-3.2.3
sudo apt-get install libtool autoconf automake
sudo aptitude install build-essential
./configure
make
sudo make install
sudo ldconfig
