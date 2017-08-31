#include <iostream>
#include <map>
#include <algorithm>
#include <fstream>
#include <vector>
#include <string>
#include <deque>

#include <experimental/filesystem>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <openssl/md5.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

using namespace std;
namespace fs = std::experimental::filesystem;

enum STATE {AUTH, TRAN, UPDT};

struct mail_state{
    STATE state;
    string username;
    string password;
    map<int, int> messageMap; // maps message number to size (in octet)
    struct flock fileLock;
    int fileDescriptor;
    vector<int> markedMessages;
};

map<int, pthread_t> allThreads;
int sockfd;
bool v_flag;
string dirName = "";

void signalSIGINTHandler(int signum){
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

void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer){
    /* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */
    MD5_CTX c;
    MD5_Init(&c);
    MD5_Update(&c, data, dataLengthBytes);
    MD5_Final(digestBuffer, &c);
}

string authenticate(mail_state *mailState){
    /* TODO:
     * 1. Acquire lock on mbox (maildrop)
     * 2. populate the state map
     * 3. Give apt response
     */
    string response = "";
    bool is_auth = false;
    fs::path file = (fs::path)dirName / (fs::path) (mailState->username + ".mbox");
    if(fs::exists(file)){
        // valid username
        if(strcmp(mailState->password.c_str(), "cis505") == 0){
            // valid user
            is_auth = true;
        }
    }
    if(!is_auth){
        response = "-ERR invalid password\r\n";
    }

    struct flock fl;
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    fl.l_pid = getpid();
    int fd = open(file.c_str(), O_RDWR);
    if(fd < 0){
        if(v_flag){
            fprintf(stderr, "Could not open %s file\n", file.c_str());
            perror("hi");
        }
        response = "-ERR could not open mbox file\r\n";
    }
    else{
        int res = fcntl(fd, F_SETLK, &fl);
        if(res){
            response = "-ERR lock could not be obtained\r\n";
            close(fd);
        }
        else{
            mailState->fileLock = fl;
            mailState->fileDescriptor = fd;

            ifstream infile;
            infile.open(file.c_str());
            string content = "";
            string line = "";
            int msgNum = 0;
            while(getline(infile, line)){
                if(strcasecmp(line.substr(0, 5).c_str(), "FROM ") == 0){
                    mailState->messageMap[msgNum] = content.size();
                    content = "";
                    msgNum ++;
                }
                else{
                    content += line + "\n";
                }
            }
            mailState->messageMap[msgNum] = content.size();
            infile.close();

            if(v_flag){
                for(map<int, int>::iterator it = mailState->messageMap.begin() ; it != mailState->messageMap.end() ; it ++){
                    fprintf(stderr, "%d\t%d\n", it->first, it->second);
                }
            }

            response = "+OK\r\n";
            mailState->state = TRAN;
        }
    }
    return response;
}

map<int, vector<string>> getMessage(fs::path filePath, int msgNumber, mail_state *mailState){
    // retrives the message(s)
    map<int, vector<string>> result;
    ifstream infile;
    infile.open(filePath);
    int offset = 0;
    for(map<int, int>::iterator it = mailState->messageMap.begin() ; it != mailState->messageMap.end() ; it ++){
        string line = "";
        if(it->first == 0){
            offset += it->second;
            continue;
        }
        infile.seekg(offset);
        getline(infile, line);
        offset += line.size() + 1;
        char buffer[it->second + 1];
        infile.read(buffer, it->second);
        buffer[it->second] = '\0';
        offset += strlen(buffer);
        if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), it->first) == mailState->markedMessages.end()){
            // message not marked for deletion
            if(msgNumber == -1){
                if(result.find(it->first) == result.end()){
                    result[it->first] = {};
                }
                result[it->first].push_back(line + "\n");
                result[it->first].push_back(buffer);
            }
            else{
                if(it->first == msgNumber){
                    if(result.find(it->first) == result.end()){
                        result[it->first] = {};
                    }
                    result[it->first].push_back(line);
                    result[it->first].push_back(buffer);
                    break;
                }
            }
        }
    }
    infile.close();
    return result;
}

string update(mail_state *mailState){
    /* TODO:
     * 1. delete marked messages
     * 2. Release lock on mbox (maildrop)
     * 3. Give apt response
     */
    string response = "";

    fs::path oldfile = (fs::path)dirName / (fs::path) (mailState->username + ".mbox");
    map<int, vector<string>> messages = getMessage(oldfile, -1, mailState);
    string content = "";
    for(map<int, vector<string>>::iterator it = messages.begin() ; it != messages.end() ; it ++){
        fprintf(stderr, "Msg Number: %d\n", it->first);
        content += (it->second)[0];
        content += (it->second)[1];
        fprintf(stderr, "Content: %s\n", content.c_str());
    }

    fs::path newfile = (fs::path)dirName / (fs::path) (mailState->username + ".mbox.tmp");
    if(fs::exists(newfile)){
        remove(newfile.c_str());
    }
    ofstream outfile;
    outfile.open(newfile.c_str());
    outfile.write(content.c_str(), content.size());
    outfile.close();

    remove(oldfile.c_str());
    rename(newfile.c_str(), oldfile.c_str());

    mailState->fileLock.l_type = F_UNLCK;
    fcntl(mailState->fileDescriptor, F_SETLK, &mailState->fileLock);
    close(mailState->fileDescriptor);
    
    response = "+OK\r\n";
    return response;
}

string runCommand(int fd, string command, int *status, mail_state *mailState){
    if(v_flag){
        fprintf(stderr, "[%d] C:%s\n", fd, command.c_str());
    }
    string response = "";
    int pos = command.find(" ");
    string cmd = "";
    if(pos == string::npos){
        cmd = command;
        command = "";
    }
    else{
        cmd = command.substr(0, pos);
        command = command.substr(pos + 1);
        if(v_flag){
            fprintf(stderr, "Command: %s\n", command.c_str());
        }
    }
    if(strcasecmp(cmd.c_str(), "USER") == 0){
        if(mailState->state == AUTH){
            // valid
            mailState->username = command;
            response = "+OK\r\n";
        }
        else{
            // invalid
            response = "-ERR Server not in Authorization state\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "PASS") == 0){
        if(mailState->state == AUTH){
            // valid
            if(strcasecmp(mailState->username.c_str(), "") == 0){
                // username not provided
                response = "-ERR username not provided\r\n";
            }
            else{
                mailState->password = command;
                response = authenticate(mailState);
            }
        }
        else{
            // invalid
            response = "-ERR Server not in Authorization state\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "STAT") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. +ve: Respond with +OK <number of messages> <size of messages (in octet)> NOTE: msg marked as deleted not be processed
             * 2. -ve: Respond with -ERR
             */
            int numberOfMessages = 0;
            int sizeOfMessages = 0;
            for(map<int, int>::iterator it = mailState->messageMap.begin() ; it != mailState->messageMap.end() ; it ++){
                if(it->first == 0){
                    continue;
                }
                if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), it->first) != mailState->markedMessages.end()){
                    // message marked as deleted
                }
                else{
                    numberOfMessages ++;
                    sizeOfMessages += it->second;
                }
            }
            response = "+OK " + to_string(numberOfMessages) + " " + to_string(sizeOfMessages) + "\r\n";
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
            return response;
        }
    }
    else if(strcasecmp(cmd.c_str(), "UIDL") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. Optional argument: message number
             *       - can not correspond to a message marked as deleted
             * 2. +ve: Gives unique id listing for all messages or the message whose number was passed
             *       - <msg number> <unique id - hash>
             */
            stringstream ss;
            ss << "+OK\r\n";
            int msgNumber = -1;
            if(command != ""){
                msgNumber = stoi(command);
                if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), msgNumber) != mailState->markedMessages.end()){
                    // already deleted
                    response = "-ERR [" + to_string(msgNumber) + "] has been deleted\r\n";
                    return response;
                }
                else if(mailState->messageMap.find(msgNumber) == mailState->messageMap.end() || msgNumber <= 0){
                    // not a valid msg number
                    response = "-ERR Invalid message number\r\n";
                    return response;
                }
            }
            fs::path file = (fs::path)dirName / (fs::path) (mailState->username + ".mbox");
            map<int, vector<string>> messages = getMessage(file, msgNumber, mailState);
            for(map<int, vector<string>>::iterator it = messages.begin() ; it != messages.end() ; it ++){
                unsigned char result[1000 + 1];
                string message = it->second[1] + ".\r\n";
                if(v_flag){
                    fprintf(stderr, "Message is: >>>>>%s<<<<<\n%d\n", message.c_str(), message.size());
                }
                computeDigest(const_cast<char *> (message.c_str()), message.size(), result);
                if(v_flag){
                    fprintf(stderr, "Length: %d\n", message.size());
                    fprintf(stderr, "UIDL: %s\n", result);
                }
                ss << it->first;
                ss << " ";
                ss << result;
                ss << "\r\n";
            }
            ss << ".\r\n";
            response = ss.str();
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
            return response;
        }
    }
    else if(strcasecmp(cmd.c_str(), "RETR") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. Required argument: message number
             *       - can not correspond to a message marked as deleted
             * 2. +ve: multi line response ; 1st line is +OK message follows; [Sends the message corresponding to the msg number]
             *       - byte stuff if the first character of a line is a termmination octet
             */
            if(command == ""){
                response = "-Err Message Number missing\r\n";
                return response;
            }
            else{
                response = "+OK\r\n";
                int msgNumber = stoi(command);
                if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), msgNumber) != mailState->markedMessages.end()){
                    // already deleted
                    response = "-ERR [" + to_string(msgNumber) + "] has been deleted\r\n";
                    return response;
                }
                else if(mailState->messageMap.find(msgNumber) == mailState->messageMap.end() || msgNumber <= 0){
                    // not a valid msg number
                    response = "-ERR Invalid message number\r\n";
                    return response;
                }
                else{
                    response = "+OK\r\n";
                    fs::path file = (fs::path)dirName / (fs::path) (mailState->username + ".mbox");
                    map<int, vector<string>> messages = getMessage(file, msgNumber, mailState);
                    response += messages[msgNumber][1];
                    response += ".\r\n";
                    unsigned char result[1000 + 1];
                    computeDigest(const_cast<char *> ((messages[msgNumber][1] + ".\r\n").c_str()), (messages[msgNumber][1] + ".\r\n").size(), result);
                    if(v_flag){
                        fprintf(stderr, "Length: %d\n", (messages[msgNumber][1] + ".\r\n").size());
                        fprintf(stderr, "UIDL: %s\n", result);
                    }
                }
            }
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
            return response;
        }
    }
    else if(strcasecmp(cmd.c_str(), "DELE") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. Required argument: message number
             *       - can not correspond to a message marked as deleted
             * 2. +ve: mark the msg as deleted
             */
            if(command == ""){
                response = "-ERR Message Number missing\r\n";
                return response;
            }
            else{
                int msgNumber = stoi(command);
                if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), msgNumber) != mailState->markedMessages.end()){
                    // already deleted
                    response = "-ERR [" + to_string(msgNumber) + "] already deleted\r\n";
                    return response;
                }
                else if(mailState->messageMap.find(msgNumber) == mailState->messageMap.end()){
                    // not a valid msg number
                    response = "-ERR Invalid message number\r\n";
                    return response;
                }
                else{
                    mailState->markedMessages.push_back(msgNumber);
                    response = "+OK\r\n";
                }
            }
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "QUIT") == 0){
        /* TODO:
         * 1. If passed in AUTH state, session terminates
         * 2. If passed in TRAN state, transfers to UPDT state
         */
        if(mailState->state == AUTH){
            *status = -1;
            response = "+OK\r\n";
        }
        else if(mailState->state == TRAN){
            mailState->state = UPDT;
            response = update(mailState);
            *status = -1;
        }
        else{
            // should not come here ; no command in update state
        }
    }
    else if(strcasecmp(cmd.c_str(), "LIST") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. If option given, that is the message number (it is optional) ; respond with line containing info for that msg
             * 2. If no argument, then multi line response ; give info for all msgs after the initial +OK response
             * 3. Each line (msg info) is called: Scan listing : <msg no> <size of msg (in octets)>
             *        - end with termination octet ()
             * 4. The msg number can't correspond to a msg marked as deleted, if so, response: -ERR no such message
             * 5. 1st OK line: +OK scan listing follows
             */
            stringstream ss;
            ss << "+OK\r\n";
            int msgNumber = -1;
            if(command != ""){
                msgNumber = stoi(command);
                if(find(mailState->markedMessages.begin(), mailState->markedMessages.end(), msgNumber) != mailState->markedMessages.end()){
                    // already deleted
                    response = "-ERR [" + to_string(msgNumber) + "] has been deleted\r\n";
                    return response;
                }
                else if(mailState->messageMap.find(msgNumber) == mailState->messageMap.end() || msgNumber <= 0){
                    // not a valid msg number
                    response = "-ERR Invalid message number\r\n";
                    return response;
                }
            }
            int offset = 0;
            for(map<int, int>::iterator it = mailState->messageMap.begin() ; it != mailState->messageMap.end() ; it ++){
                if(it->first != 0 && (it->first == msgNumber || msgNumber == -1) && find(mailState->markedMessages.begin(), mailState->markedMessages.end(), it->first) == mailState->markedMessages.end()){
                    // not marked
                    ss << it->first;
                    ss << " ";
                    ss << it->second;
                    ss << "\r\n";
                    if(msgNumber != -1){
                        break;
                    }
                }
                offset += it->second;
            }
            ss << ".\r\n";
            response = ss.str();
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "RSET") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO:
             * 1. Any messages marked as deleted are unmarked, then send +OK
             */
            mailState->markedMessages = {};
            response = "+OK\r\n";
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "NOOP") == 0){
        if(mailState->state == TRAN){
            // valid
            /*
             * TODO: Nothing
             */
            response = "+OK\r\n";
        }
        else{
            // invalid
            response = "-ERR Server not in Transaction state\r\n";
        }
    }
    else{
        *status = 0;
        response = "-ERR Not Supported\r\n";
    }
    if(v_flag){
        fprintf(stderr, "[%d] S:%s", fd, response.c_str());
    }
    return response;
}

void *threadWorker(void *arg){
    int fd = *(int *)arg;
    string msg = "+OK POP3 Ready [localhost]\r\n";
    send(fd, msg.c_str(), strlen(msg.c_str()), 0);
    
    string command = "";
    string response = "";
    mail_state mailState = {
        AUTH,

    };

    int num_bytes;
    int MAX_LEN = 10;
    char cmd[MAX_LEN + 1];

    while(true){
        if(command.find("\r\n") == string::npos){
            num_bytes = recv(fd, cmd, MAX_LEN, 0);
        }
        else{
            num_bytes = 0;
        }
        cmd[num_bytes] = '\0';
        if(v_flag){
            fprintf(stderr, "[%d] Received %d bytes: %s\n", fd, num_bytes, cmd);
        }
        
        if(num_bytes <= 0 && command.size() == 0){
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
            response = runCommand(fd, command_to_run, &status, &mailState);
            send(fd, response.c_str(), strlen(response.c_str()), 0);
            if(status == -1){
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

    int port_no = 11000; // Port Number ; default is 11000
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

    dirName = argv[optind];

    if(fs::exists(dirName)){
        // is a directory
        if(v_flag){
            fprintf(stderr, "Directory: %s exists\n", dirName.c_str());
        }
    }
    else{
        if(v_flag){
            fprintf(stderr, "[%s]: Not a valid directory\n", dirName.c_str());
            exit(1);
        }
    }

    if(v_flag){
        fprintf(stderr, "Registering signal handler.\n");    
    }
    if(signal(SIGINT, signalSIGINTHandler) == SIG_ERR){
        if(v_flag){
            fprintf(stderr, "Signal Error: Could not register signal handler.\n");
        }
        exit(1);
    }

    startServer(port_no);

    return 0;
}