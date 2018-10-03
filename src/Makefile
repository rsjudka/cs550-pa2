all: super_peer leaf_node logging env_dirs test_data

super_peer: super_peer.cpp
	g++ super_peer.cpp -std=c++11 -pthread -o super_peer

leaf_node: leaf_node.cpp
	g++ leaf_node.cpp -std=c++11 -pthread -o leaf_node

logging:
	mkdir logs/
	mkdir logs/leaf_nodes/
	mkdir logs/super_peers/

env_dirs:
	mkdir nodes/
	mkdir nodes/n1/
	mkdir nodes/n2/
	mkdir nodes/n3/
	mkdir nodes/n4/
	mkdir nodes/n5/
	mkdir nodes/n6/
	mkdir nodes/n7/
	mkdir nodes/n8/
	mkdir nodes/n9/
	mkdir nodes/n10/

test_data:
	cp ../data/n1/* nodes/n1/
	cp ../data/n2/* nodes/n2/
	cp ../data/n3/* nodes/n3/
	cp ../data/n4/* nodes/n4/
	cp ../data/n5/* nodes/n5/
	cp ../data/n6/* nodes/n6/
	cp ../data/n7/* nodes/n7/
	cp ../data/n8/* nodes/n8/
	cp ../data/n9/* nodes/n9/
	cp ../data/n10/* nodes/n10/

clean:
	rm super_peer leaf_node
	rm -rf nodes/
	rm -rf logs/