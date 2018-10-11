#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <thread>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <fstream>


#define HOST "localhost" // assume all connections happen on same machine
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096


class SuperPeer {
    private:
        std::vector<int> neighbors;
        std::vector<int> leaf_nodes;

        std::unordered_map<std::string, std::vector<int>> files_index; // mapping between a filename and any peers associated with it
        
        struct Message_ID_Hash {
            size_t operator()(const std::pair<int, int>& p) const {
                return p.first ^ p.second;
            }
        };
        
        typedef std::pair<int, int> Message_ID;
        std::unordered_map<Message_ID, std::chrono::system_clock::time_point, Message_ID_Hash> message_ids;
        
        std::ofstream server_log;

        std::mutex log_m;
        std::mutex files_index_m;
        std::mutex message_ids_m;

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
            std::string msg = "closing connection for leaf_node id '" + std::to_string(leaf_node_id) + "' and cleaning up index";
            log(type, msg);
            files_index_cleanup(leaf_node_id);
            close(leaf_node_socket_fd);
        }

        // create a connection to some server given a specific port
        int connect_server(int server_port) {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char *)&addr, addr_size);

            // open a socket for the new connection
            struct hostent *server = gethostbyname(HOST);
            int server_socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&addr.sin_addr.s_addr, server->h_length);
            addr.sin_port = htons(server_port);
            
            // connect to the server
            if (connect(server_socket_fd, (struct sockaddr *)&addr, addr_size) < 0) {
                return -1;
            }
            
            return server_socket_fd;
        }

        // handle all requests sent to the indexing server
        void handle_connection(int conn_socket_fd) {
            char id;
            //initialize connection with conn by getting conn id
            if (recv(conn_socket_fd, &id, sizeof(id), 0) < 0) {
                log("conn unidentified", "closing connection");
                close(conn_socket_fd);
                return;
            }

            switch (id) {
                case '0':
                    handle_super_peer_request(conn_socket_fd);
                    break;
                case '1':
                    handle_leaf_node_requests(conn_socket_fd);
                    break;
                default:
                    log("conn unidentified", "closing connection");
                    close(conn_socket_fd);
                    return;
            }
        }

        void handle_super_peer_request(int super_peer_socket_fd) {
            char request;
            // get request type from peer leaf_node
            if (recv(super_peer_socket_fd, &request, sizeof(request), 0) < 0) {
                log("super_peer unidentified", "closing connection");
                close(super_peer_socket_fd);
                return;
            }

            switch (request) {
                case '1':
                    query(super_peer_socket_fd);
                    break;
                default:
                    log("unexpected request", "closing connection");
            }
            close(super_peer_socket_fd);
        }

        // handle all requests sent to the indexing server
        void handle_leaf_node_requests(int leaf_node_socket_fd) {
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
                        node_search(leaf_node_socket_fd, leaf_node_id);
                        break;
                    case '4':
                        print_files_map();
                        break;
                    case '5':
                        print_message_ids_list();
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

        std::string get_ids_from_filename(std::string filename) {
            std::ostringstream ids;
            if (files_index.count(filename) > 0) {
                std::string delimiter;
                std::lock_guard<std::mutex> guard(files_index_m);
                for (auto &&id : files_index[filename]) {
                    // add leaf_node id to stream
                    ids << delimiter << id;
                    delimiter = ',';
                }
            }
            return ids.str();
        }

        std::string get_neighbor_ids_from_filename(std::string filename, int leaf_node_id, int seq_id_, int ttl) {
            std::string filename_ids;
            std::string delimiter;
            for (auto&& neighbor : neighbors) {
                int super_peer_socket_fd = connect_server(neighbor);
                if (super_peer_socket_fd < 0) {
                    log("failed super peer connection", "ignoring connection");
                    continue;
                }
                if (send(super_peer_socket_fd, "0", sizeof(char), 0) < 0)
                    log("super peer unresponsive", "ignoring request");
                else {
                    if (send(super_peer_socket_fd, "1", sizeof(char), 0) < 0) 
                        log("super peer unresponsive", "ignoring request");
                    else {
                        if (send(super_peer_socket_fd, &ttl, sizeof(ttl), 0) < 0)
                           log("super peer unresponsive", "ignoring request");
                        else {
                            if (send(super_peer_socket_fd, &leaf_node_id, sizeof(leaf_node_id), 0) < 0)
                                log("super peer unresponsive", "ignoring request");
                            else {
                                if (send(super_peer_socket_fd, &seq_id_, sizeof(seq_id_), 0) < 0)
                                    log("super peer unresponsive", "ignoring request");
                                else {
                                    char buffer[MAX_FILENAME_SIZE];
                                    strcpy(buffer, filename.c_str());
                                    if (send(super_peer_socket_fd, buffer, sizeof(buffer), 0) < 0)
                                        log("super peer unresponsive", "ignoring request");
                                    else {
                                        if (!send_message_id(super_peer_socket_fd, leaf_node_id, seq_id_))
                                            log("super peer unresponsive", "ignoring request");
                                        else {
                                            std::string msg = "msg id [" + std::to_string(leaf_node_id) + "," + std::to_string(seq_id_) + "] to peer " + std::to_string(neighbor);
                                            log("forwarding message", msg);
                                            char buffer_[MAX_MSG_SIZE];
                                            if (recv(super_peer_socket_fd, buffer_, sizeof(buffer_), 0) < 0)
                                                log("super peer unresponsive", "ignoring request");
                                            else if (buffer_[0]) {
                                                filename_ids += delimiter + std::string(buffer_);
                                                delimiter = ',';
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                close(super_peer_socket_fd);
            }
            return filename_ids;
        }

        bool send_message_id(int super_peer_socket_fd, int leaf_node_id, int sequence_number) {
            if (send(super_peer_socket_fd, &leaf_node_id, sizeof(leaf_node_id), 0) < 0)
                return false;

            if (send(super_peer_socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0)
                return false;
            
            std::lock_guard<std::mutex> guard(message_ids_m);
            message_ids[{leaf_node_id, sequence_number}] = std::chrono::system_clock::now();
            return true;
        }

        bool check_message_id(int super_peer_socket_fd) {
            int leaf_node_id;
            if (recv(super_peer_socket_fd, &leaf_node_id, sizeof(leaf_node_id), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return false;
            }

            int sequence_number;
            // recieve filename from peer leaf_node
            if (recv(super_peer_socket_fd, &sequence_number, sizeof(sequence_number), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return false;
            }

            Message_ID msg_id = {leaf_node_id, sequence_number};
            std::lock_guard<std::mutex> guard(message_ids_m);
            if (message_ids.find(msg_id) == message_ids.end()) {
                message_ids[msg_id] = std::chrono::system_clock::now();
                return true;
            }
            log("message already seen", "rerouting message back to sender");
            return false;
        }

        void query(int super_peer_socket_fd) {
            int ttl;
            if (recv(super_peer_socket_fd, &ttl, sizeof(ttl), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return;
            }

            int leaf_node_id;
            if (recv(super_peer_socket_fd, &leaf_node_id, sizeof(leaf_node_id), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return;
            }

            int seq_id_;
            if (recv(super_peer_socket_fd, &seq_id_, sizeof(seq_id_), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return;
            }            

            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer leaf_node
            if (recv(super_peer_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return;
            }

            std::string node_ids;
            if (check_message_id(super_peer_socket_fd)) {
                node_ids = get_ids_from_filename(buffer);
                if (ttl-- > 0) {
                    std::string neighbor_leaf_node_ids = get_neighbor_ids_from_filename(buffer, leaf_node_id, seq_id_, ttl);
                    if (!neighbor_leaf_node_ids.empty())
                        node_ids += ((!node_ids.empty()) ? "," : "") + neighbor_leaf_node_ids;
                }
            }

            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, node_ids.c_str());
            // send comma delimited list of all leaf_node ids for a specific file to the peer leaf_node
            if (send(super_peer_socket_fd, buffer_, sizeof(buffer_), 0) < 0) {
                log("super peer unresponsive", "ignoring request");
                return;
            }
        }
        
        // handles communication with peer leaf_node for returning all leaf_node ids mapped to a filename
        void node_search(int leaf_node_socket_fd, int leaf_node_id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer leaf_node
            if (recv(leaf_node_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_leaf_node(leaf_node_socket_fd, leaf_node_id, "leaf_node unresponsive");
                return;
            }

            seq_id += 1;
            std::string leaf_node_ids = get_ids_from_filename(buffer);
            std::string neighbor_leaf_node_ids = get_neighbor_ids_from_filename(buffer, leaf_node_id, seq_id, TTL);
            if (!neighbor_leaf_node_ids.empty())
                leaf_node_ids += ((!leaf_node_ids.empty()) ? "," : "") + neighbor_leaf_node_ids;
            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, leaf_node_ids.c_str());
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

        void print_message_ids_list() {
            std::lock_guard<std::mutex> guard(files_index_m);
            std::cout << "\n__________MESSAGE IDS__________" << std::endl;
            for (auto const &message_id : message_ids) {
                std::cout << '[' << message_id.first.first << ',' << message_id.first.second << "]" << std::endl;
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

            config >> TTL;
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

        void maintain_message_ids() {
            while (1) {
                sleep(60);
                std::lock_guard<std::mutex> guard(message_ids_m);
                for (auto itr = message_ids.cbegin(); itr != message_ids.cend();) {
                    itr = (std::chrono::duration_cast<std::chrono::minutes>(std::chrono::system_clock::now() - itr->second).count() > 1) ? message_ids.erase(itr++) : ++itr;
                }
            }
        }

    public:
        int TTL;
        int peer_id;
        int peer_port;
        int socket_fd;
        int seq_id = 0;

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
            server_log.open("logs/super_peers/" + std::to_string(peer_port));
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

            std::thread t(&SuperPeer::maintain_message_ids, this);
            t.detach();

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
                std::thread t(&SuperPeer::handle_connection, this, conn_socket_fd);
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
