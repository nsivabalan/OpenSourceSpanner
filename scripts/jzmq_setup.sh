cd ~/temp
git clone https://github.com/zeromq/jzmq.git
cd jzmq
sudo apt-get install pkg-config
./autogen.sh
./configure
make
sudo make install
