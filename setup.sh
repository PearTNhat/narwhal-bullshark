sudo apt install python3.11 python3.11-venv -y
python3.11 -m venv venv
source venv/bin/activate
cd benchmark/
sudo apt install -y python3-dev python3-pip python3-venv \
                   build-essential pkg-config \
                   libfreetype6-dev libpng-dev
pip install matplotlib
pip install -r requirements.txt
pip install fabric==2.6.0 multiaddr toml requests
pip install multiaddr
pip install six lexicon decorator
pip install pyyaml


./venv/bin/fab local