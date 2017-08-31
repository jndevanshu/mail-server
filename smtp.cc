#include <iostream>
#include <map>
#include <algorithm>
#include <fstream>
#include <vector>
#include <string>

#include <experimental/filesystem>

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

namespace fs = std::experimental::filesystem;

struct mail_state{
    string heloHost;
    string mailFrom;
    vector<string> recipients;
    string data;
    string previousCommand;
};

map<int, pthread_t> allThreads;
int sockfd;
bool v_flag;
bool r_flag;
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

int sendMail(mail_state *mailState){
    /*
     * Sends the mails:
     * 1. If to be relayed, added to mqueue
     * 2. Else put in the recipient's mailbox
     */    
    if(v_flag){
        fprintf(stderr, "Number of recipients: %d\n", mailState->recipients.size());
    }

    int result = 0;
    time_t rawtime;
    struct tm *timeinfo;

    time(&rawtime);
    timeinfo = localtime(&rawtime);

    char *time = asctime(timeinfo);
    time[strlen(time) - 1] = '\0';

    string content = "From " + mailState->mailFrom + " " + time + "\r\n";

    for(vector<string>::iterator it = mailState->recipients.begin() ; it != mailState->recipients.end() ; it ++){
        if(v_flag){
            fprintf(stderr, "Sending to: %s\n", *it);
        }

        string recipient = *it;
        int senderStart = recipient.find(":");
        if(senderStart == string::npos){
            senderStart = recipient.find("<");
        }
        int senderEnd = recipient.find("@");
        int angularEnd = recipient.find(">");
        
        string host = recipient.substr(senderEnd + 1, angularEnd - senderEnd - 1);
        if(v_flag){
            fprintf(stderr, "Host is %s\n", host.c_str());
        }
        if(strcasecmp(host.c_str(), "localhost") != 0 && !r_flag){
            // wrong host ; will not relay
            // should have already checked ; SHOULD NOT OCCUR
        }
        else if(strcasecmp(host.c_str(), "localhost") != 0 && r_flag){
            // wrong host ; have to relay
            string mailFrom = mailState->mailFrom;
            string rcptTo = recipient;
            string data = content + mailState->data;

            fs::path mqueueFile = ((fs::path) dirName) / ((fs::path) "mqueue");
            ofstream writer(mqueueFile, ofstream::app);
            writer << "FROM " << mailFrom << "\r\n";
            writer << "RCPT TO:" << rcptTo << "\r\n";
            writer << "DATA\r\n";
            writer << data << ".\r\n";
            writer.close();
        }
        else{
            string msg = content + mailState->data;
            string rcpt = recipient.substr(senderStart + 1, senderEnd - senderStart - 1) + ".mbox";
            fs::path mboxFile = ((fs::path) dirName) / ((fs::path) rcpt);
            if(fs::is_regular_file(mboxFile)){
                if(v_flag){
                    fprintf(stderr, "Writing to the file: %s\n", ((string)mboxFile).c_str());
                }
                // If a regular file, write to it
                ofstream writer(mboxFile, ofstream::app);
                writer << msg;
                writer.close();
            }
            else{
                if(v_flag){
                    fprintf(stderr, "Can not write to the file: %s\n", ((string)mboxFile).c_str());
                }
                // not the correct file: should not happen
                // have already checked the recipients
            }
        }
    }
    return result;
}

bool checkSender(string sender){
    // Checks if sender contains a @ or not
    if(sender.find("<") == string::npos){
        return false;
    }
    else if(sender.find(">") == string::npos){
        return false;
    }
    if(sender.find("@") == string::npos){
        if(sender.find(">") - sender.find("<") == 1){
            return true;
        }
        else{
            return false;
        }
    }
    else{
        return true;
    }
}

bool checkRecipient(string recipient){
    /*
     * Checks if recipient is correct
     * 1. If relay enabled, domain can be anything
     * 2. Else, it needs to be localhost
     */
    if(v_flag){
        fprintf(stderr, "Recipient: %s\n", recipient.c_str());
    }
    int start = recipient.find("<");
    int end = recipient.find(">");
    if(v_flag){
        fprintf(stderr, "Start: %d\nEnd: %d\n", start, end);
    }
    recipient = recipient.substr(start + 1, end - start - 1);

    if(v_flag){
        fprintf(stderr, "Recipient: %s\n", recipient.c_str());
    }
    int senderStart = recipient.find(":");
    if(senderStart == string::npos){
        senderStart = recipient.find("<");
    }
    int senderEnd = recipient.find("@");
    if(senderEnd == string::npos){
        return false;
    }
    int angularEnd = recipient.find(">");
    
    string host = recipient.substr(senderEnd + 1, angularEnd - senderEnd + 1);
    if(strcasecmp(host.c_str(), "localhost") != 0 && !r_flag){
        // wrong host ; will not relay
        return false;
    }
    else if(strcasecmp(host.c_str(), "localhost") != 0 && r_flag){
        // wrong host ; will relay
        return true;
    }
    else{
        string rcpt = recipient.substr(senderStart + 1, senderEnd - senderStart - 1) + ".mbox";
        if(v_flag){
            fprintf(stderr, "Sender: %s\n", rcpt.c_str());
        }
        fs::path mboxFile = ((fs::path) dirName) / ((fs::path) rcpt);
        if(v_flag){
            fprintf(stderr, "mBoxFile: %s\n", ((string)mboxFile).c_str());
        }
        if(fs::is_regular_file(mboxFile)){
            // Correct file exists
            return true;
        }
        else{
            // no such file exists
            return false;
        }
    }
}

string runCommand(int fd, string command, int *status, mail_state *mailState){
    /*
     * Run commands: HELO, MAIL, RCPT, DATA, RSET, NOOP, QUIT
     */
    if(v_flag){
        fprintf(stderr, "[%d] C:>%s<\n", fd, command.c_str());
    }
    string response = "";

    if(strcasecmp(mailState->previousCommand.c_str(), "DATA") == 0){
        // just store the data
        if(strcasecmp(command.c_str(), ".") == 0){
            // end of data
            int result = sendMail(mailState);
            response = "250 OK\r\n";
            *status = 1;
        }
        else{
            mailState->data += command + "\r\n";
            *status = 3;
        }
        return response;
    }

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

    if(strcasecmp(cmd.c_str(), "HELO") == 0){
        if(strcasecmp(mailState->previousCommand.c_str(), "HELO") == 0 || strcasecmp(mailState->previousCommand.c_str(), "") == 0){
            if(command.length() == 0){
                response = "501 Empty HELO not allowed\r\n";
                *status = -1; // Quit
            }
            else{
                mailState->heloHost = command;
                mailState->previousCommand = "HELO";
                response = "250 localhost\r\n";
            }
        }
        else{
            response = "503 Out of Order\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "MAIL") == 0){
        if(strcasecmp(mailState->previousCommand.c_str(), "HELO") == 0 || strcasecmp(mailState->previousCommand.c_str(), "MAIL") == 0){
            // run the command
            int fromPos = command.find(":");
            if(fromPos == string::npos){
                // ':' not found
                response = "501 Syntax Error\r\n";
            }
            else if(strcasecmp(command.substr(0, fromPos).c_str(), "FROM") != 0){
                response = "500 Command Unrecognized\r\n";
            }
            else{
                string sender = command.substr(fromPos + 1);
                if(checkSender(sender)){
                    // localhost recipient
                    mailState->mailFrom = sender;
                    mailState->previousCommand = "MAIL";
                    response = "250 OK\r\n";
                }
                else{
                    // something wrong with from address
                    response = "553 Requested action not taken\r\n";
                }
            }
        }
        else{
            response = "503 Out of Order\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "RCPT") == 0){
        if(strcasecmp(mailState->previousCommand.c_str(), "MAIL") == 0 || strcasecmp(mailState->previousCommand.c_str(), "RCPT") == 0){
            // run the command
            int toPos = command.find(":");
            if(toPos == string::npos){
                // ':' not found
                response = "501 Syntax Error\r\n";
            }
            else if(strcasecmp(command.substr(0, toPos).c_str(), "TO") != 0){
                response = "500 Command Unrecognized\r\n";
            }
            else{
                string rcpt = command.substr(toPos + 1);
                if(checkRecipient(rcpt)){
                    // localhost recipient
                    mailState->recipients.push_back(rcpt);
                    mailState->previousCommand = "RCPT";
                    response = "250 OK\r\n";
                }
                else{
                    // not localhost recipient
                    response = "550 Requested action not taken\r\n";
                }
            }
        }
        else{
            response = "503 Out of Order\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "DATA") == 0){
        if(strcasecmp(mailState->previousCommand.c_str(), "RCPT") == 0){
            // run the command
            mailState->previousCommand = "DATA";
            response = "354 Please send the data. End with a <DOT>.\r\n";
        }
        else{
            response = "503 Out of Order\r\n";
        }
    }
    else if(strcasecmp(cmd.c_str(), "RSET") == 0){
        response = "250 OK\r\n";
        *status = 1;
    }
    else if(strcasecmp(cmd.c_str(), "NOOP") == 0){
        // do nothing
        response = "250 OK\r\n";
    }
    else if(strcasecmp(cmd.c_str(), "QUIT") == 0){
        response = "221 OK\r\n";
        *status = -1;
    }
    else{
        // not a valid command
        response = "500 -ERR Unknown command\r\n";
    }
    if(v_flag){
        fprintf(stderr, "[%d] S:%s", fd, response.c_str());
    }
    return response;
}

void *threadWorker(void *arg){
    int fd = *(int *)arg;
    string msg = "220 localhost Server ready (Author: Devanshu Jain / devjain)\r\n";
    send(fd, msg.c_str(), strlen(msg.c_str()), 0);
    
    string command = "";
    string response = "";
    mail_state mailState = {
        "",
        "",
        {},
        "",
        ""
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
        if(num_bytes <= 0 && command != ""){
            if(v_flag){
                fprintf(stderr, ">%s<\n", command.c_str());
            }
        }
        
        if(num_bytes <= 0 && command == ""){
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
                    // crlf or lf not found
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
            fprintf(stderr, "Command to run: %s\n", command_to_run.c_str());
            // found
            int status = 0;
            response = runCommand(fd, command_to_run, &status, &mailState);
            if(status != 3){
                send(fd, response.c_str(), strlen(response.c_str()), 0);
            }
            if(status == -1){
                // QUIT command
                break;
            }
            else if(status == 1){
                // RSET command
                command = "";
                response = "";
                mailState.mailFrom = "";
                mailState.recipients = {};
                mailState.data = "";
                mailState.previousCommand = "HELO";
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

    int port_no = 2500; // Port Number ; default is 2500
    int c;
    char *val, *temp;
    while((c = getopt(argc, argv, "p:avr")) != -1){
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
            case 'r':
                r_flag = true;
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