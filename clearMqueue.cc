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
#include <sys/utsname.h>
#include <netinet/in.h>

#include <netdb.h>
#include <arpa/nameser.h>
#include <arpa/inet.h>
#include <resolv.h>

using namespace std;

namespace fs = std::experimental::filesystem;

struct mail_state{
    string mailFrom;
    string rcptTo;
    string data;
};

string dirName = "";
bool v_flag = false;

int printMail(mail_state mailState){
    if(v_flag){
        fprintf(stderr, "###%s###\n", mailState.mailFrom.c_str());
        fprintf(stderr, "###%s###\n", mailState.rcptTo.c_str());
        fprintf(stderr, "###%s###\n", mailState.data.c_str());
    }
}

vector<mail_state> readData(){
    if(v_flag){
        fprintf(stderr, "Reading data\n");
    }
    vector<mail_state> mails;
    fs::path mqueueFile = ((fs::path) dirName) / ((fs::path) "mqueue");
    ifstream istream;
    istream.open(mqueueFile);
    string line = "";
    while(getline(istream, line)){
        if(strcasecmp(line.substr(0, 5).c_str(), "FROM ") == 0){
            mail_state mailState;
            line = line.substr(5, line.size() - 6); // take from < till \r encountered (excluded)
            mailState.mailFrom = line;
            getline(istream, line);
            if(v_flag){
                fprintf(stderr, "Read: %s\n", line.c_str());
            }
            int rcptStartPos = line.find("<");
            if(rcptStartPos == string::npos || line.find(":") == string::npos){
                if(v_flag){
                    fprintf(stderr, "Could not parse recipient properly. Please Check\n");
                    exit(1);
                }
            }
            line = line.substr(rcptStartPos, line.size() - rcptStartPos - 1); // take from < till \r encountered (excluded)
            mailState.rcptTo = line;
            getline(istream, line);
            while(strcasecmp(line.substr(0, 4).c_str(), "DATA") != 0){
                getline(istream, line);
            }
            mailState.data = "";
            while(getline(istream, line) && strcasecmp(line.substr(0, 2).c_str(), ".\r") != 0){
                mailState.data += line + "\n";
            }
            mails.push_back(mailState);
            printMail(mailState);
        }
    }
    istream.close();
    return mails;
}

int writeRemainingMails(vector<mail_state> mails){
    fs::path newFileName = ((fs::path) dirName) / ((fs::path) "mqueue.tmp");
    if(fs::exists(newFileName)){
        remove(newFileName.c_str());
    }
    ofstream writer;
    writer.open(newFileName, ofstream::app);
    for(vector<mail_state>::iterator it = mails.begin() ; it != mails.end() ; it ++){
        writer << "FROM " << it->mailFrom << "\r\n";
        writer << "RCPT TO:" << it->rcptTo << "\r\n";
        writer << "DATA\r\n";
        writer << it->data;
        writer << ".\r\n";
    }
    writer.close();

    fs::path oldFileName = ((fs::path) dirName) / ((fs::path) "mqueue");
    if(fs::exists(oldFileName)){
        remove(oldFileName.c_str());
    }
    rename(newFileName.c_str(), oldFileName.c_str());
    return 0;
}

map<int, vector<string>> getMXRecords(string host){
    int LENGTH = 4096;
    unsigned char answer[LENGTH + 1];
    ns_msg msg;
    int cnt_an;
    ns_rr rr;
    char buffer[LENGTH + 1];

    map<int, vector<string>> mxRecords;
    
    int resLen = 0;
    if((resLen = res_query(host.c_str(), ns_c_in, ns_t_mx, answer, LENGTH)) < 0){
        herror("res_query error: ");
        return mxRecords;
    }
    answer[resLen] = '\0';
    if(v_flag){
        fprintf(stdout, "Response Length: %d\n", resLen);
    }
    if(ns_initparse(answer, resLen, &msg) < 0){
        herror("parse error: ");
        return mxRecords;
    }
    if((cnt_an = ns_msg_count(msg, ns_s_an)) <= 0){
        herror("count error: ");
        return mxRecords;
    }
    for(int i = 0 ; i < cnt_an ; i ++){
        if(ns_parserr(&msg, ns_s_an, i, &rr) < 0){
            herror("parserr error: ");
            return mxRecords;
        }
        ns_sprintrr(&msg, &rr, NULL, NULL, buffer, LENGTH);
        
        const u_char *rdata = ns_rr_rdata(rr);
        char exchange[4096];
        int priority = ns_get16(rdata);
        int len = dn_expand(answer, answer + resLen, rdata + 2, exchange, sizeof(exchange));
        if(v_flag){
            fprintf(stdout, "Priority: %d\n", priority);
            fprintf(stdout, "Exchange: %s\n", exchange);    
        }
        if(mxRecords.find(priority) == mxRecords.end()){
            mxRecords[priority] = {};
        }
        mxRecords[priority].push_back(exchange);
    }
    return mxRecords;
}

string sendCommand(int sockfd, string command, int expectedResponseCode){
    int LENGTH = 4096;
    char buffer[LENGTH + 1];

    if(send(sockfd, command.c_str(), command.size(), 0) < 0){
        perror("send error (Could not send mail from command): ");
        return "";
    }
    if(v_flag){
        fprintf(stderr, "ME: %s\n", command.c_str());
    }
    int resLen = 0;
    if((resLen = recv(sockfd, buffer, LENGTH, 0)) < 0){
        perror("recv error (Could not recv the reply for mail from command): ");
        return "";
    }
    buffer[resLen] = '\0';
    if(v_flag){
        fprintf(stderr, "THEY: %s\n", buffer);
    }
    if(atoi(buffer) != expectedResponseCode){
        return "";
    }
    string retValue = buffer;
    return retValue;
}

int _sendMail(string host, int portNo, string mailFrom, string rcptTo, string data){
    struct addrinfo hints, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int rv = 0;
    if((rv = getaddrinfo(host.c_str(), to_string(portNo).c_str(), &hints, &res)) != 0){
        perror("getaddrinfo error: ");
        fprintf(stderr, "Return value: %d\n%s\n", rv, gai_strerror(rv));
        return 1;
    }
    else{
        if(v_flag){
            fprintf(stderr, "Connected to: %s:%s\n", host.c_str(), to_string(portNo).c_str());
        }
    }

    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if(sockfd < 0){
        perror("Socket Error: ");
        return 1;
    }
    else{
        if(v_flag){
            fprintf(stderr, "Socket created\n");
        }
    }

    if(connect(sockfd, res->ai_addr, res->ai_addrlen) != 0){
        perror("connect error: ");
        return 1;
    }
    else{
        if(v_flag){
            fprintf(stderr, "Connection created\n");
        }
        int LENGTH = 4096;
        char buffer[LENGTH + 1];
        int resLen = 0;
        if((resLen = recv(sockfd, buffer, LENGTH, 0)) < 0){
            perror("recv error (Could not recv the reply for connection): ");
            return 1;
        }
        buffer[resLen] = '\0';
        if(v_flag){
            fprintf(stderr, "THEY: %s\n", buffer);
        }
    }
    
    string command = "HELO localhost\r\n";
    sendCommand(sockfd, command, 250);

    command = "MAIL FROM:" + mailFrom + "\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    command = "RCPT TO:" + rcptTo + "\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    command = "DATA\r\n";
    if(sendCommand(sockfd, command, 354).size() == 0){
        return 1;
    }

    command = data + "\r\n.\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    close(sockfd);

    return 0;
}

int sendMail(mail_state mailState){
    string recipient = mailState.rcptTo;
    int ATPos = recipient.find("@");
    int endAnglePos = recipient.find(">");
    string host = recipient.substr(ATPos + 1, endAnglePos - ATPos - 1);
    if(v_flag){
        fprintf(stderr, "Processing host: %s\n", host.c_str());
    }
    map<int, vector<string>> mxRecords = getMXRecords(host);
    for(map<int, vector<string>>::iterator it = mxRecords.begin() ; it != mxRecords.end() ; it ++){
        if(v_flag){
            fprintf(stderr, "With Priority: %d\n", it->first);
        }
        for(vector<string>::iterator exchange_it = it->second.begin() ; exchange_it != it->second.end() ; exchange_it ++){
            if(v_flag){
                fprintf(stderr, "Trying Exchange: %s\n", (*exchange_it).c_str());
            }
            string smtpServer = *exchange_it;
            int portNo = 25;
            int result = _sendMail(smtpServer, portNo, mailState.mailFrom, mailState.rcptTo, mailState.data);
            if(result == 0){
                // mail sent successfully
                return 0;
            }
        }
    }
    return 1;
}

int clearMQueue(){
    vector<mail_state> remaining;
    vector<mail_state> mails = readData();
    for(vector<mail_state>::iterator it = mails.begin() ; it != mails.end() ; it ++){
        int result = sendMail(*it);
        if(result != 0){
            // mail was not sent successfully
            remaining.push_back(*it);
        }
    }
    writeRemainingMails(remaining);
}

int main(int argc, char *argv[]){
    int c;
    char *val, *temp;
    while((c = getopt(argc, argv, "va")) != -1){
        switch(c){
            case 'v':
                v_flag = true;
                break;
            case 'a':
                fprintf(stderr, "Devanshu Jain (devjain)\n");
                exit(1);
            case '?':
                fprintf(stderr, "Unknown option `-%c`.\n", optopt);
                exit(1);
            default:
                abort();
        }
    }

    dirName = argv[optind];

    clearMQueue();
}