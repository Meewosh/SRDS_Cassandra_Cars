docker run --name cassandra-node-0 --network cassandra-network -v $(pwd)/data-0:/var/lib/cassandra -d -p 9042:9042 --rm cassandra:latest 

docker run --name cassandra-node-1 -d --network cassandra-network -e CASSANDRA_SEEDS=cassandra-node-0 -v $(pwd)/data-1:/var/lib/cassandra -d -p 9043:9042 --rm cassandra:latest

docker run --name cassandra-node-2 -d --network cassandra-network -e CASSANDRA_SEEDS=cassandra-node-0 -v $(pwd)/data-2:/var/lib/cassandra -d -p 9044:9042 --rm cassandra:latest