sudo docker stop $(sudo docker ps -a -q)
sudo docker rm $(sudo docker ps -a -q)

cargo build --release
sudo docker build -t shawl . 