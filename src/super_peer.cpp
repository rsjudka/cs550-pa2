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


#define PORT 9999 // default chosen for server
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096


class SuperPeer {
    private:
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

        //helper function for cleaning up the indexing server anytime a peer client is disconnected
        void remove_client(int client_socket_fd, int client_id, std::string type) {
            std::string msg = "closing connection for client ID '" + std::to_string(client_id) + "' and cleaning up index";
            log(type, msg);
            files_index_cleanup(client_id);
            close(client_socket_fd);
        }

        // handle all requests sent to the indexing server
        void handle_client_requests(int client_socket_fd) {
            int client_id;
            //initialize connection with peer client with getting client id
            if (recv(client_socket_fd, &client_id, sizeof(client_id), 0) < 0) {
                log("client unidentified", "closing connection");
                close(client_socket_fd);
                return;
            }

            char request;
            while (1) {
                request = '0';
                // get request type from peer client
                if (recv(client_socket_fd, &request, sizeof(request), 0) < 0) {
                    remove_client(client_socket_fd, client_id, "client unresponsive");
                    return;
                }

                switch (request) {
                    case '1':
                        registry(client_socket_fd, client_id);
                        break;
                    case '2':
                        deregistry(client_socket_fd, client_id);
                        break;
                    case '3':
                        search(client_socket_fd, client_id);
                        break;
                    case '4':
                        print_files_map();
                        break;
                    case '0':
                        remove_client(client_socket_fd, client_id, "client disconnected");
                        return;
                    default:
                        remove_client(client_socket_fd, client_id, "unexpected request");
                        return;
                }
            }
        }

        // handles communication with peer client for registering a single file 
        void registry(int client_socket_fd, int client_id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer client
            if (recv(client_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_client(client_socket_fd, client_id, "client unresponsive");
                return;
            }
            
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(files_index_m);
            // add peer's client id to file map if not already included
            if(!(std::find(files_index[filename].begin(), files_index[filename].end(), client_id) != files_index[filename].end()))
                files_index[filename].push_back(client_id);
        }

        // handles communication with peer client for deregistering a single file 
        void deregistry(int client_socket_fd, int client_id) {
            char buffer[MAX_FILENAME_SIZE];
            if (recv(client_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_client(client_socket_fd, client_id, "client unresponsive");
                return;
            }
 
            std::string filename = std::string(buffer);
            std::lock_guard<std::mutex> guard(files_index_m);
            // remove peer's client id from file
            files_index[filename].erase(std::remove(files_index[filename].begin(), files_index[filename].end(), client_id), files_index[filename].end());
            // remove filename from mapping if no more peers mapped to file
            if (files_index[filename].size() == 0)
                files_index.erase(filename);
        }

        // remove client id from all files in mapping
        void files_index_cleanup(int client_id) {
            std::unordered_map<std::string, std::vector<int>> tmp_files_index;
            
            std::lock_guard<std::mutex> guard(files_index_m);
            for (auto const &file_index : files_index) {
                std::vector<int> tmp_client_ids = file_index.second;
                tmp_client_ids.erase(std::remove(tmp_client_ids.begin(), tmp_client_ids.end(), client_id), tmp_client_ids.end());
                if (tmp_client_ids.size() > 0)
                    tmp_files_index[file_index.first] = tmp_client_ids;
            }
            files_index = tmp_files_index;
        }

        // handles communication with peer client for returning all client ids mapped to a filename
        void search(int client_socket_fd, int client_id) {
            char buffer[MAX_FILENAME_SIZE];
            // recieve filename from peer client
            if (recv(client_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                remove_client(client_socket_fd, client_id, "client unresponsive");
                return;
            }

            std::string filename = std::string(buffer);
            
            std::ostringstream client_ids;
            if (files_index.count(filename) > 0) {
                std::string delimiter;
                std::lock_guard<std::mutex> guard(files_index_m);
                for (auto &&client_id : files_index[filename]) {
                    // add client id to stream
                    client_ids << delimiter << client_id;
                    delimiter = ',';
                }
            }

            char buffer_[MAX_MSG_SIZE];
            strcpy(buffer_, client_ids.str().c_str());
            // send comma delimited list of all client ids for a specific file to the peer client
            if (send(client_socket_fd, buffer_, sizeof(buffer_), 0) < 0) {
                remove_client(client_socket_fd, client_id, "client unresponsive");
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
                for (auto &&client_id : file_index.second) {
                    std::cout << delimiter << client_id;
                    delimiter = ',';
                }
                std::cout << std::endl;
            }
            std::cout << "_______________________________\n" << std::endl;
        }

    public:
        int socket_fd;

        SuperPeer() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char*)&addr, addr_size);
            
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(PORT);

            socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            // bind socket to port to be used for indexing server
            if (bind(socket_fd, (struct sockaddr*)&addr, addr_size) < 0)
                error("failed server binding");

            std::cout << "starting indexing server on port " << PORT << '\n' << std::endl;

            // start logging
            server_log.open("logs/indexing_server/server.log");
        }

        void run() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            int client_socket_fd;

            std::ostringstream client_identity;
            while (1) {
                // listen for any peer connections to start communication
                listen(socket_fd, 5);

                if ((client_socket_fd = accept(socket_fd, (struct sockaddr*)&addr, &addr_size)) < 0) {
                    // ignore any failed connections from peer clients
                    log("failed client connection", "ignoring connection");
                    continue;
                }

                client_identity << inet_ntoa(addr.sin_addr) << '@' << ntohs(addr.sin_port);
                log("client connected", client_identity.str());
                
                // start thread for single client-server communication
                std::thread t(&SuperPeer::handle_client_requests, this, client_socket_fd);
                t.detach(); // detaches thread and allows for next connection to be made without waiting

                client_identity.str("");
                client_identity.clear();
            }
        }

        ~SuperPeer() {
            close(socket_fd);
            server_log.close();
        }
};


int main() {
    SuperPeer super_peer;
    super_peer.run();

    return 0;
}
