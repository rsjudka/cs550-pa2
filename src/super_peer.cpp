#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <thread>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <fstream>


#define ID 0 // used to identify as a super peer
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096


class SuperPeer {
    private:
        std::vector<int> neighbors;
        std::vector<int> leaf_nodes;
        
        std::unordered_map<std::string, std::vector<int>> files_index; // mapping between a filename and any peers associated with it
        std::ofstream server_log;

        std::mutex log_m;
        std::mutex files_index_m;

        // helper function for getting the current time to microsecond-accuracy as a string
        std::string time_now() {
            std::chrono::high_resolution_clock::duration now = std::chrono::high_resolution_clock::now().time_since_epoch();
            std::chrono::microseconds now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
            return std::to_string(now_ms.count());
        }

        // log message to specified log file
        void log(std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(log_m);
            server_log << '[' << time_now() << "] [" << type << "] " << msg  << '\n' << std::endl;
            std::cout << '[' << time_now() << "] [" << type << "] [" << msg  << "]\n" << std::endl;
        }
        
        void error(std::string type) {
            std::cerr << "\n[" << type << "] exiting program\n" << std::endl;
            exit(1);
        }

        //helper function for cleaning up the indexing server anytime a peer leaf_node is disconnected
        void remove_leaf_node(int leaf_node_socket_fd, int leaf_node_id, std::string type) {
            std::string msg = "closing connection for leaf_node ID '" + std::to_string(leaf_node_id) + "' and cleaning up index";
            log(type, msg);
            files_index_cleanup(leaf_node_id);
            close(leaf_node_socket_fd);
        }

        // handle all requests sent to the indexing server
        void handle_request(int leaf_node_socket_fd) {
            int leaf_node_id;
            //initialize connection with peer leaf_node with getting leaf_node id
            if (recv(leaf_node_socket_fd, &leaf_node_id, sizeof(leaf_node_id), 0) < 0) {
                log("leaf_node unidentified", "closing connection");
                close(leaf_node_socket_fd);
                return;
            }

            char request;
            while (1) {
                request = '0';
                // get request type from peer leaf_node
                if (recv(leaf_node_socket_fd, &request, sizeof(request), 0) < 0) {
                    remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                    return;
                }

                switch (request) {
                    case '1':
                        registry(leaf_node_socket_fd, leaf_node_id);
                        break;
                    case '2':
                        deregistry(leaf_node_socket_fd, leaf_node_id);
                        break;
                    case '3':
                        search(leaf_node_socket_fd, leaf_node_id);
                        break;
                    case '4':
                        print_files_map();
                        break;
                    case '0':
                        remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node disconnected");
                        return;
                    default:
                        remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "unexpected request");
                        return;
                }
            }
        }

        // handles communication with peer leaf_node for registering a single file 
        void registry(int leaf_node_socket_fd, int leaf_node_id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer leaf_node
            if (recv(leaf_node_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                return;
            }
            
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(files_index_m);
            // add peer's leaf_node id to file map if not already included
            if(!(std::find(files_index[filename].begin(), files_index[filename].end(), leaf_node_id) != files_index[filename].end()))
                files_index[filename].push_back(leaf_node_id);
        }

        // handles communication with peer leaf_node for deregistering a single file 
        void deregistry(int leaf_node_socket_fd, int leaf_node_id) {
            char buffer[MAX_FILENAME_SIZE];
            if (recv(leaf_node_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                return;
            }
 
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(files_index_m);
            // remove peer's leaf_node id from file
            files_index[filename].erase(std::remove(files_index[filename].begin(), files_index[filename].end(), leaf_node_id), files_index[filename].end());
            // remove filename from mapping if no more peers mapped to file
            if (files_index[filename].size() == 0)
                files_index.erase(filename);
        }

        // remove leaf_node id from all files in mapping
        void files_index_cleanup(int leaf_node_id) {
            std::unordered_map<std::string, std::vector<int>> tmp_files_index;
            
            std::lock_guard<std::mutex> guard(files_index_m);
            for (auto const &file_index : files_index) {
                std::vector<int> tmp_leaf_node_ids = file_index.second;
                tmp_leaf_node_ids.erase(std::remove(tmp_leaf_node_ids.begin(), tmp_leaf_node_ids.end(), leaf_node_id), tmp_leaf_node_ids.end());
                if (tmp_leaf_node_ids.size() > 0)
                    tmp_files_index[file_index.first] = tmp_leaf_node_ids;
            }
            files_index = tmp_files_index;
        }

        // handles communication with peer leaf_node for returning all leaf_node ids mapped to a filename
        void search(int leaf_node_socket_fd, int leaf_node_id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer leaf_node
            if (recv(leaf_node_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                return;
            }

            std::string filename = std::string(buffer);
            
            std::ostringstream leaf_node_ids;
            if (files_index.count(filename) > 0) {
                std::string delimiter;
                std::lock_guard<std::mutex> guard(files_index_m);
                for (auto &&leaf_node_id : files_index[filename]) {
                    // add leaf_node id to stream
                    leaf_node_ids << delimiter << leaf_node_id;
                    delimiter = ',';
                }
            }

            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, leaf_node_ids.str().c_str());
            // send comma delimited list of all leaf_node ids for a specific file to the peer leaf_node
            if (send(leaf_node_socket_fd, buffer_, sizeof(buffer_), 0) < 0) {
                remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                return;
            }
        }

        // helper function for displaying the entire files index
        void print_files_map() {
            std::lock_guard<std::mutex> guard(files_index_m);
            std::cout << "\n__________FILES INDEX__________" << std::endl;
            for (auto const &file_index : files_index) {
                std::cout << file_index.first << ':';
                std::string delimiter;
                for (auto &&leaf_node_id : file_index.second) {
                    std::cout << delimiter << leaf_node_id;
                    delimiter = ',';
                }
                std::cout << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
        }

        void get_network(std::string config_path) {
            std::ifstream config(config_path);
            int member_type;
            int id;
            int port;
            std::string neighbors_string;
            std::string leaf_nodes_string;

            std::string tmp;
            while(config >> member_type) {
                if (member_type == 0) {
                    config >> id >> port >> neighbors_string >> leaf_nodes_string;
                    if (id == peer_id) {
                        peer_port = port;
                        neighbors = comma_delim_ints_to_vector(neighbors_string);
                        leaf_nodes = comma_delim_ints_to_vector(leaf_nodes_string);
                        return;
                    }
                }
                else
                    std::getline(config, tmp); // ignore anything else in the line
            }
            error("invalid peer id");
        }

    public:
        int peer_id;
        int peer_port;
        int socket_fd;

        SuperPeer(int id, std::string config_path) {
            peer_id = id;
            get_network(config_path);

            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char*)&addr, addr_size);
            
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(peer_port);

            socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            // bind socket to port to be used for indexing server
            if (bind(socket_fd, (struct sockaddr*)&addr, addr_size) < 0)
                error("failed server binding");

            std::cout << "starting indexing server on port " << peer_port << '\n' << std::endl;

            // start logging
            server_log.open("logs/indexing_server/server.log");
        }

        std::vector<int> comma_delim_ints_to_vector(std::string s) {
            std::vector<int> result;

            std::stringstream ss(s);
            while(ss.good()) {
                std::string substr;
                std::getline(ss, substr, ',');
                result.push_back(atoi(substr.c_str()));
            }
            return result;
        }

        void run() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            int conn_socket_fd;

            std::ostringstream conn_identity;
            while (1) {
                // listen for any peer connections to start communication
                listen(socket_fd, 5);

                if ((conn_socket_fd = accept(socket_fd, (struct sockaddr*)&addr, &addr_size)) < 0) {
                    // ignore any failed connections from peer leaf_nodes
                    log("failed connection", "ignoring connection");
                    continue;
                }

                conn_identity << inet_ntoa(addr.sin_addr) << '@' << ntohs(addr.sin_port);
                log("conn established", conn_identity.str());
                
                // start thread for single client-server communication
                std::thread t(&SuperPeer::handle_request, this, conn_socket_fd);
                t.detach(); // detaches thread and allows for next connection to be made without waiting

                conn_identity.str("");
                conn_identity.clear();
            }
        }

        ~SuperPeer() {
            close(socket_fd);
            server_log.close();
        }
};


int main(int argc, char *argv[]) {
    // require peer id config path to be passed as arg
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " peer_id config_path" << std::endl;
        exit(0);
    }

    SuperPeer super_peer(atoi(argv[1]), argv[2]);
    super_peer.run();

    return 0;
}
