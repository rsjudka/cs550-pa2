#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>

#include <thread>
#include <mutex>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <fstream>


#define HOST "localhost" // assume all connections happen on same machine
#define INDEXING_SERVER_PORT 9999 // default chosen from indexing_server source code
#define MAX_FILENAME_SIZE 256 // assume the maximum file size is 256 characters
#define MAX_MSG_SIZE 4096
#define MAX_STAT_MSG_SIZE 16


//global counters used only for logging special messages used for later anlaysis
int search_request_counter = 0;
int retrieve_request_counter = 0;


class LeafNode {
    private:
        std::vector<std::pair<std::string, time_t>> files; // vector of all files within a peer's directory
        std::ofstream server_log;
        std::ofstream client_log;

        std::mutex log_m;

        // helper function for getting the current time to microsecond-accuracy as a string
        std::string time_now() {
            std::chrono::high_resolution_clock::duration now = std::chrono::high_resolution_clock::now().time_since_epoch();
            std::chrono::microseconds now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
            return std::to_string(now_ms.count());
        }

        // log message to specified log file
        void log(std::ofstream &log_stream, std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(log_m);
            log_stream << '[' << time_now() << "] [" << type << "] " << msg  << '\n' << std::endl;
        }

        //special log messages used for later analysis
        void eval_log(std::ofstream &log_stream, int key, std::string type, std::string msg) {
            std::lock_guard<std::mutex> guard(log_m);
            log_stream << '!' << key << " [" << time_now() << "] [" << type << "] [" << msg  << "]\n" << std::endl;
        }
        
        void error(std::string type) {
            std::cerr << "\n[" << type << "] exiting program\n" << std::endl;
            exit(1);
        }

        // handles a peer server's file retrieval request
        // only performs single retrieval
        void handle_client_request(int client_socket_fd) {
            retrieve(client_socket_fd);

            close(client_socket_fd);
            log(server_log, "client disconnected", "closed connection");
        }

        void retrieve(int client_socket_fd) {
            // recieve filename to download from peer client
            char buffer[MAX_FILENAME_SIZE];
            if (recv(client_socket_fd, buffer, MAX_FILENAME_SIZE, 0) < 0) {
                log(server_log, "client unresponsive", "closing connection");
                return;
            }
            
            // create full file path of peer server to send
            std::ostringstream filename;
            filename << std::string(files_directory_path);
            filename << std::string(buffer);

            int fd = open(filename.str().c_str(), O_RDONLY);
            if (fd == -1) {
                // send message to peer client if file cannot be opened
                if (send(client_socket_fd, "-1", MAX_STAT_MSG_SIZE, 0) < 0)
                    log(server_log, "client unresponsive", "closing connection");
            }
            else {
                struct stat file_stat;
                if (fstat(fd, &file_stat) < 0) {
                    // send message to peer client if file size cannot be determined
                    if (send(client_socket_fd, "-2", MAX_STAT_MSG_SIZE, 0) < 0)
                        log(server_log, "client unresponsive", "closing connection");
                }
                else {
                    char file_size[MAX_STAT_MSG_SIZE];
                    sprintf(file_size, "%ld", file_stat.st_size);

                    //send file size to peer client
                    if (send(client_socket_fd, file_size, sizeof(file_size), 0) < 0) {
                        log(server_log, "client unresponsive", "closing connection");
                        return;
                    }

                    off_t offset = 0;
                    int remaining_size = file_stat.st_size;
                    int sent_size = 0;
                    //send file in 4096 byte blocks until entire file sent
                    while (((sent_size = sendfile(client_socket_fd, fd, &offset, MAX_MSG_SIZE)) > 0) && (remaining_size > 0))
                        remaining_size -= sent_size;
                }
            }
            close(fd);
        }

        // read all files in peer's directory and save to files vector
        std::vector<std::pair<std::string, time_t>> get_files() {
            std::vector<std::pair<std::string, time_t>> tmp_files;
            
            if (auto directory = opendir(files_directory_path.c_str())) {
                while (auto file = readdir(directory)) {
                    //skip . and .. files and any directories
                    if (!file->d_name || strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0 || file->d_type == DT_DIR)
                        continue;
                    
                    // get full file path
                    std::ostringstream file_path;
                    file_path << files_directory_path;
                    file_path << file->d_name;
                    
                    int fd = open(file_path.str().c_str(), O_RDONLY);
                    if (fd == -1) {
                        //ignore file if unable to open
                        log(client_log, "failed file open", "ignoring \"" + file_path.str() + '\"');
                        continue;
                    }
                    
                    struct stat file_stat;
                    if (fstat(fd, &file_stat) < 0) {
                        // ignore file if unable to file stats
                        log(client_log, "failed file stat", "ignoring \"" + file_path.str() + '\"');
                        continue;
                    }
                    close(fd);

                    // save a pair of the filename and last modified date to the files vector if not already in there
                    time_t modified_time = file_stat.st_mtim.tv_sec;
                    std::pair<std::string, time_t> file_info = std::make_pair(file->d_name, modified_time);
                    if(!(std::find(tmp_files.begin(), tmp_files.end(), file_info) != tmp_files.end()))
                        tmp_files.push_back(file_info);
                }
                closedir(directory);
            }
            else {
                error("invalid directory");
            }
            return tmp_files;
        }

        // create a connection to some server given a specific port
        // index_server flag used for knowing which type of server to connect
        int connect_server(int server_port, bool index_server=true) {
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
                // only exit program if failed to connect to indexing server
                if (index_server)
                    error("failed indexing server connection");
                else {
                    return -1;
                }
            }
            
            return server_socket_fd;
        }

        void register_files(int server_socket_fd) {
            int n;
            char buffer[MAX_FILENAME_SIZE];

            while (1) {
                std::vector<std::pair<std::string, time_t>> tmp_files = get_files();
                for(auto&& x: files) {
                    char request = '1';
                    // send a deregister request if a file is no longer in the files vector (or has been modified)
                    if(!(std::find(tmp_files.begin(), tmp_files.end(), x) != tmp_files.end()))
                        request = '2';
                    
                    // send a register request for any other files
                    if (send(server_socket_fd, &request, sizeof(request), 0) < 0) {
                        log(client_log, "server unresponsive", "ignoring request");
                    }
                    else {
                        bzero(buffer, MAX_FILENAME_SIZE);
                        strcpy(buffer, x.first.c_str());
                        // register file with the indexing server
                        if (send(server_socket_fd, buffer, sizeof(buffer), 0) < 0)
                            log(client_log, "server unresponsive", "ignoring request");
                    }
                }
                // replace the old files vector with the new one
                files = tmp_files;
                // wait 5 seconds to update files list 
                sleep(5);
            }
        }
        
        // handle user interface for sending a search request to the indexing server
        void search_request(int server_socket_fd) {
            std::cout << "filename: ";
            char filename[MAX_FILENAME_SIZE];
            std::cin >> filename;
            eval_log(client_log, search_request_counter, "search request", "start");
            // send a search request to the indexing server
            if (send(server_socket_fd, "3", sizeof(char), 0) < 0) {
                std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                log(client_log, "server unresponsive", "ignoring request");
            }
            else {
                // send the filename to search to the indexing server
                if (send(server_socket_fd, filename, sizeof(filename), 0) < 0) {
                    std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                    log(client_log, "server unresponsive", "ignoring request");
                }
                else {
                    // recieve list of peers with file from indexing server
                    // output appropriate message to peer client
                    char buffer[MAX_MSG_SIZE];
                    if (recv(server_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                        std::cout << "\nunexpected connection issue: no search performed\n" << std::endl;
                        log(client_log, "server unresponsive", "ignoring request");
                    }
                    else if (!buffer[0])
                        std::cout << "\nfile \"" << filename << "\" not found\n" << std::endl;
                    else
                        std::cout << "\npeer(s) with file \"" << filename << "\": " << buffer << '\n' << std::endl;
                }
            }
            eval_log(client_log, search_request_counter++, "search request", "end");
        }

        //helper function for creating the filename of a downloaded file
        std::string resolve_filename(std::string filename, std::string(peer)) {
            std::ostringstream local_filename;
            local_filename << files_directory_path;
            size_t extension_idx = filename.find_last_of('.');
            local_filename << filename.substr(0, extension_idx);
            // add the file origin if the file already exists in the local directory
            if ((std::find_if(files.begin(), files.end(), [filename](const std::pair<std::string, int> &element){return element.first == filename;}) != files.end()))
                local_filename << "-origin-" << peer;
            local_filename << filename.substr(extension_idx, filename.size() - extension_idx);

            return local_filename.str();
        }
        
        // handle user interface for sending a retrieve request to a peer server
        void retrieve_request(int server_socket_fd) {
            std::cout << "peer: ";
            char peer[6];
            std::cin >> peer;
            eval_log(client_log, retrieve_request_counter, "retrieve request", "start");
            // check if the passed-in peer is the current client
            if (atoi(peer) == port) {
                std::cout << "\npeer '" << peer << "' is current client: no retreival performed\n" << std::endl;
                return;
            }
            // connect to the given peer server
            int peer_socket_fd = connect_server(atoi(peer), false);
            if (peer_socket_fd < 0) {
                std::cout << "\npeer '" << peer << "' is not valid: no retreival performed\n" << std::endl;
                log(client_log, "failed peer server connection", "ignoring request");
                return;
            }
            
            eval_log(client_log, retrieve_request_counter, "retrieve request", "pause");
            std::cout << "filename: ";
            char filename[MAX_FILENAME_SIZE];
            std::cin >> filename;
            eval_log(client_log, retrieve_request_counter, "retrieve request", "unpause");
            if (send(peer_socket_fd, filename, sizeof(filename), 0) < 0) {
                std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                log(client_log, "peer unresponsive", "ignoring request");
            }
            else {
                char buffer[MAX_STAT_MSG_SIZE];
                // get the file size from the peer server
                if (recv(peer_socket_fd, buffer, sizeof(buffer), 0) < 0) {
                    std::cout << "\nunexpected connection issue: no retreival performed\n" << std::endl;
                    log(client_log, "peer unresponsive", "ignoring request");
                }
                else {
                    int file_size = atoi(buffer);
                    // handle message from peer server
                    if (file_size == -1)
                        std::cout << "\npeer '" << peer << "' does not have file \"" << filename << "\": no retreival performed\n" << std::endl;
                    else if (file_size == -2)
                        std::cout << "\ncould not read file \"" << filename << "\"'s stats: no retreival performed\n" << std::endl;
                    else {
                        // create pretty filename for outputting results to peer client
                        std::string local_filename_path = resolve_filename(filename, peer);
                        size_t filename_idx = local_filename_path.find_last_of('/');
                        std::string local_filename = local_filename_path.substr(filename_idx+1, local_filename_path.size() - filename_idx);
                        FILE *file = fopen(local_filename_path.c_str(), "w");
                        if (file == NULL) {
                            std::cout << "\nunable to create new file \"" << local_filename << "\": no retreival performed\n" << std::endl;
                            log(client_log, "failed file open", "ignoring file");
                        }
                        else {
                            char buffer_[MAX_MSG_SIZE];
                            int remaining_size = file_size;
                            int received_size;
                            // write blocks recieved from peer server to new file
                            while (((received_size = recv(peer_socket_fd, buffer_, sizeof(buffer_), 0)) > 0) && (remaining_size > 0)) {
                                fwrite(buffer_, sizeof(char), received_size, file);
                                remaining_size -= received_size;
                            }
                            fclose(file);
                            std::cout << "\nfile \"" << filename << "\" downloaded as \"" << local_filename << "\"\n" << std::endl;
                            std::cout << "\ndislpay file '" << local_filename << "'\n. . .\n" << std::endl;
                            log(client_log, "file download", "file download successful");
                        }
                    }
                }
            }
            eval_log(client_log, retrieve_request_counter++, "retrieve request", "end");
            close(peer_socket_fd);
        }

    public:
        std::string files_directory_path;
        int port;
        int socket_fd;

        LeafNode(std::string path, int custom_port) {
            files_directory_path = path;
            // add ending '/' if missing in path argument
            if (files_directory_path.back() != '/')
                files_directory_path += '/';
            files = get_files();

            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            bzero((char*)&addr, addr_size);
            
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            // if passing custom port as arg, use that instead of a random one (used for running analysis)
            if (custom_port > 0) {
                addr.sin_port = htons(custom_port);
            }

            socket_fd = socket(AF_INET, SOCK_STREAM, 0);

            // bind socket to port to be used for peer server
            if (bind(socket_fd, (struct sockaddr*)&addr, addr_size) < 0)
                error("failed to start peer server");
            
            // get port number to use as client id for indexing server
            getsockname(socket_fd, (struct sockaddr *)&addr, &addr_size);
            port = ntohs(addr.sin_port);

            std::cout << "current client id: " << port << '\n' << std::endl;

            // start logging for both peer client and peer server
            std::string log_name_prefix = "logs/peers/" + std::to_string(port);
            server_log.open(log_name_prefix + "_server.log");
            client_log.open(log_name_prefix + "_client.log");
        }
        
        void run_client() {
            int server_socket_fd = connect_server(INDEXING_SERVER_PORT);

            //send peer server port number to be used as client id in indexing server
            if (send(server_socket_fd, &port, sizeof(port), 0) < 0)
                error("server unreachable");

            //start thread for automatic files updater
            std::thread t(&LeafNode::register_files, this, server_socket_fd);
            t.detach();

            //continously prompt user for request
            while (1) {
                std::string request;
                std::cout << "request [(s)earch|(r)etrieve|(q)uit]: ";
                std::cin >> request;

                switch (request[0]) {
                    case 's':
                    case 'S':
                        search_request(server_socket_fd);
                        break;
                    case 'r':
                    case 'R':
                        retrieve_request(server_socket_fd);
                        break;
                    case 'q':
                    case 'Q':
                        close(server_socket_fd);
                        exit(0);
                        break;
                    case 'l':
                    case 'L':
                        // used for testing to see all registered files
                        send(server_socket_fd, "4", sizeof(char), 0);
                        break;
                    default:
                        std::cout << "\nunexpected request\n" << std::endl;
                        break;
                }
            }
        }

        void run_server() {
            struct sockaddr_in addr;
            socklen_t addr_size = sizeof(addr);
            int client_socket_fd;

            std::ostringstream client_identity;
            while (1) {
                // listen for any peer connections to start file download
                listen(socket_fd, 5);

                if ((client_socket_fd = accept(socket_fd, (struct sockaddr*)&addr, &addr_size)) < 0) {
                    // ignore any failed connections from peer clients
                    log(server_log, "failed client connection", "ignoring connection");
                    continue;
                }

                client_identity << inet_ntoa(addr.sin_addr) << '@' << ntohs(addr.sin_port);
                log(server_log, "client connected", client_identity.str());

                // start thread for performing file download
                std::thread t(&LeafNode::handle_client_request, this, client_socket_fd);
                t.detach(); // detaches thread and allows for next connection to be made without waiting

                client_identity.str("");
                client_identity.clear();
            }
        }

        void run() {
            // start independent threads for both client and server
            std::thread c_t(&LeafNode::run_client, this);
            std::thread s_t(&LeafNode::run_server, this);

            c_t.join();
            s_t.join();
        }

        ~LeafNode() {
            close(socket_fd);
            server_log.close();
            client_log.close();
        }
};


int main(int argc, char *argv[]) {
    // require directory path to be passed as arg
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " path" << std::endl;
        exit(0);
    }
    
    int port = 0;
    if (argc == 3) {
        port = atoi(argv[2]);
    }

    LeafNode leaf_node(argv[1], port);
    leaf_node.run();

    return 0;
}
