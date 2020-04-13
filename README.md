# Docker image
[kafka](https://hub.docker.com/r/wurstmeister/kafka)  
[kafka manager](https://hub.docker.com/r/sheepkiller/kafka-manager)

# Git
[https://github.com/wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker)

# Command
## start kafka & zk
1. go to kafka-docker folder
2. run `docker-compose up`

## start kafka manager
`docker run -it --rm  -p 9000:9000 -e ZK_HOSTS="192.168.99.100:2181" -e APPLICATION_SECRET=letmein sheepkiller/kafka-manager`  
ZK_HOSTS might be changed

## access kafka manager
[http://192.168.99.100:9000/](http://192.168.99.100:9000/)