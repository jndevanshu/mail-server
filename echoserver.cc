#include <iostream>
#include <map>
#include <algorithm>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

using namespace std;

map<int, pthread_t> allThreads; // Mapping fd to thread id
int sockfd; // socket fd
bool v_flag; // verbose flag

void signalSIGINTHandler(int signum){
    /*
     * Signal Handler for Ctrl-C.
     * 1. Closes all fds.
     * 2. Joins all threads.
     * 3. Closes socket fd.
     */
    string msg = "-ERR Server shutting down\n";
    for(map<int, pthread_t>::iterator mapIterator = allThreads.begin() ; mapIterator != allThreads.end() ; mapIterator ++){
        send(mapIterator->first, msg.c_str(), strlen(msg.c_str()), 0);
        if(v_flag){
            fprintf(stderr, "[%d] Connection closed\n", mapIterator->first);
        }
        close(mapIterator->first);
    }
    for(map<int, pthread_t>::iterator mapIterator = allThreads.begin() ; mapIterator != allThreads.end() ; mapIterator ++){
        if(v_flag){
            fprintf(stderr, "Joining thread!!!\n");
        }
        pthread_cancel(mapIterator->second);
        pthread_join(mapIterator->second, NULL);
    }
    if(v_flag){
        fprintf(stderr, "Closing socket fd\n");
    }
    close(sockfd);
    exit(0);
}

string runCommand(int fd, string command, int *status){
    /*
     * Parses and Runs the command.
     * Status becomes 1 when quit command run.
     * Return value: string response to the command.
     */
    if(v_flag){
        fprintf(stderr, "[%d] C:%s\n", fd, command.c_str());
    }
    string response = "";
    int pos = command.find(" ");
    string cmd = "";
    if(pos == string::npos){
        cmd = command;
    }
    else{
        cmd = command.substr(0, pos);
    }
    if(strcasecmp(cmd.c_str(), "ECHO") == 0){
        *status = 0;
        if(pos == string::npos){
            response = "+OK \n";
        }
        else{
            response = "+OK " + command.substr(pos + 1) + "\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "QUIT") == 0){
        *status = 1;
        response = "+OK Goodbye!\r\n";
    }
    else{
        *status = 0;
        response = "-ERR Unknown command\r\n";
    }
    if(v_flag){
        fprintf(stderr, "[%d] S:%s", fd, response.c_str());
    }
    return response;
}

void *threadWorker(void *arg){
    /*
     * Thread worker to handle one particular connection
     * Receives the string from connection until LF or CRLF encountered. 
     *        when encountered, runs the command
     * Sends the appropriate response back to the client.
     */
    int fd = *(int *)arg;
    string msg = "+OK Server ready (Author: Devanshu Jain / devjain)\r\n";
    send(fd, msg.c_str(), strlen(msg.c_str()), 0);
    
    string command = "";
    string response = "";

    int num_bytes;
    int MAX_LEN = 10;
    char cmd[MAX_LEN + 1];

    while(true){
        num_bytes = recv(fd, cmd, MAX_LEN, 0);
        cmd[num_bytes] = '\0';
        if(v_flag){
            fprintf(stderr, "[%d] Received %d bytes: %s\n", fd, num_bytes, cmd);
        }
        
        if(num_bytes <= 0){
            // client has closed the connection or some error has happened
            break;
        }
        else{
            command += cmd;
            int CRLF_pos = command.find("\r\n");
            string command_to_run = "";
            if(CRLF_pos == string::npos){
                // in interest of robustness
                CRLF_pos = command.find("\n");
                if(CRLF_pos == string::npos){
                    // not found
                    continue;
                }
                else{
                    command_to_run = string(command.begin(), command.begin() + CRLF_pos);
                    command = string(command.begin() + CRLF_pos + 1, command.end());
                }
            }
            else{
                command_to_run = string(command.begin(), command.begin() + CRLF_pos);
                command = string(command.begin() + CRLF_pos + 2, command.end());
            }
            // found
            int status = 0;
            response = runCommand(fd, command_to_run, &status);
            send(fd, response.c_str(), strlen(response.c_str()), 0);
            if(status == 1){
                // QUIT command
                break;
            }
        }
    }
    
    if(v_flag){
        fprintf(stderr, "[%d] Connection closed\n", fd);
    }
    close(fd);
    pthread_exit(0);
}

int startServer(int port_no){
    /*
     * Starts the server, accepts connections and creates threads to handle those connections.
     */
    if(v_flag){
        fprintf(stderr, "Creating the socket\n");
    }
    sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if(sockfd < 0){
        if(v_flag){
            fprintf(stderr, "Socket Error: Could not start the server\n");
        }
        exit(1);
    }
    
    struct sockaddr_in servaddr;
    memset(&servaddr, '\0', sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(port_no);
    if(v_flag){
        fprintf(stderr, "Binding the socket to port %d\n", port_no);
    }
    if(bind(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0){
        if(v_flag){
            fprintf(stderr, "Bind Error: Could not bind the port\n");
        }
        exit(1);
    }
    
    if(v_flag){
        fprintf(stderr, "Starting to listen...\n");
    }
    if(listen(sockfd, 10) < 0){
        if(v_flag){
            fprintf(stderr, "Listen Error: Could not listen\n");
        }
        exit(1);
    }

    while(true){
        struct sockaddr_in clientaddr;
        socklen_t clientaddrlen = sizeof(clientaddr);
        int *fd = (int *) malloc(sizeof(int)); 
        *fd = accept(sockfd, (struct sockaddr *) &clientaddr, &clientaddrlen);
        if(*fd < 0){
            if(v_flag){
                fprintf(stderr, "Accept Error: Could not accept\n");
            }
            exit(1);
        }
        else{
            if(v_flag){
                fprintf(stderr, "[%d] New connection\n", *fd);
            }
        }
        pthread_t thread;
        pthread_create(&thread, NULL, &threadWorker, fd);
        allThreads[*fd] = thread;
    }
    return 0;
}

int main(int argc, char *argv[]){

    int port_no = 10000; // Port Number ; default is 10000
    
    int c;
    char *val, *temp;
    while((c = getopt(argc, argv, "p:av")) != -1){
        switch(c){
            case 'p': 
                val = optarg;
                port_no = strtol(val, &temp, 0);
                if(*temp == '\0'){
                    if(port_no <= 0){
                        fprintf(stderr, "Port Number can not be less than 1.\n");
                        exit(1);
                    }
                }
                else{
                    fprintf(stderr, "Port Number is not an integer.\n");
                    exit(1);
                }
                break;
            case 'a':
                fprintf(stderr, "Devanshu Jain (devjain)\n");
                exit(1);
            case 'v':
                v_flag = true;
                break;
            case '?':
                if(optopt == 'p'){
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                }
                else{
                    fprintf(stderr, "Unknown option `-%c`.\n", optopt);
                }
                exit(1);
            default:
                abort();
        }
    }
    if(v_flag){
        fprintf(stderr, "Registering signal handler.\n");    
    }
    // Signal registered
    if(signal(SIGINT, signalSIGINTHandler) == SIG_ERR){
        if(v_flag){
            fprintf(stderr, "Signal Error: Could not register signal handler.\n");
        }
        exit(1);
    }

    startServer(port_no);

    return 0;
}