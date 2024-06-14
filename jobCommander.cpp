// File jobCommander.c
// Supports:
// # issueJob <job>: issues a job to the job scheduler
// # setConcurrency <N>: sets the concurrency level to N
// # stop <jobID>: stops a job with the given jobID
// # poll [running,queued]:
// # exit: exits the job commander

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <vector>
#include <string>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "Util.h"

using namespace std;

int main(int argc, char *argv[]) 
{
    // Parse the command line arguments.
    if (argc < 4) {
        printf("Usage: jobCommander <server_name> <port_number> <command>\n");
        exit(1);
    }

    // The server name and port number are the first two arguments.
    string server_name = string(argv[1]);
    string port_number = string(argv[2]);

    // The command is the third argument.
    string command = string(argv[3]);

    // Connect to the server via a socket.
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Error opening socket");
        exit(1);
    }

    struct hostent *server = gethostbyname(server_name.c_str());
    if (server == NULL) {
        fprintf(stderr, "Error, no such host\n");
        exit(1);
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(atoi(port_number.c_str()));
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Error connecting");
        exit(1);
    }

    // We have now connected to the server.
    // We are ready to send and receive messages.

    // Issue a job to the job scheduler.
    if (command == "issueJob") 
    {
        if (argc < 5) {
            printf("Usage: jobCommander <server_name> <port_number> issueJob <job>\n");
            exit(1);
        }

        // The command is "issueJob" and the arguments is the concatenation of 
        // the rest of the command line arguments.
        string issue_job_command = "issueJob ";
        for (int i = 4; i < argc; i++) {
            issue_job_command += string(argv[i]) + " ";
        }

        printf("Issuing job: %s\n", issue_job_command.c_str());

        // Send the command to the server.
        int n = write(sockfd, issue_job_command.c_str(), issue_job_command.length());
        if (n < 0) {
            perror("Error writing to socket");
            exit(1);
        }
    }
    else if(command == "setConcurrency")
    {
        if (argc < 5) {
            printf("Usage: jobCommander setConcurrency <N>\n");
            exit(1);
        }
        printf("Setting concurrency to: %s\n", argv[2]);

        string set_concurrency_command = "setConcurrency " + string(argv[4]);

        // Send the command to the server.
        int n = write(sockfd, set_concurrency_command.c_str(), set_concurrency_command.length());
        if (n < 0) {
            perror("Error writing to socket");
            exit(1);
        }
    }
    else if(command == "stop")
    {
        if (argc < 3) {
            printf("Usage: jobCommander stop <jobID>\n");
            exit(1);
        }
        printf("Stopping job: %s\n", argv[2]);

        string stop_job_command = "stop " + string(argv[4]);

        // Send the command to the server.
        int n = write(sockfd, stop_job_command.c_str(), stop_job_command.length());
        if (n < 0) {
            perror("Error writing to socket");
            exit(1);
        }
    }
    else if(command == "poll")
    {
        if (argc < 5) {
            printf("Usage: jobCommander poll [running,queued]\n");
            exit(1);
        }
        printf("Polling: %s\n", argv[2]);

        string poll_command = "poll " + string(argv[4]);

        // Send the command to the server.
        int n = write(sockfd, poll_command.c_str(), poll_command.length());
        if (n < 0) {
            perror("Error writing to socket");
            exit(1);
        }
    }
    else if(command == "exit")
    {
        printf("Exiting jobExecutorServer\n");

        string exit_command = "exit";

        // Send the command to the server.
        int n = write(sockfd, exit_command.c_str(), exit_command.length());
        if (n < 0) {
            perror("Error writing to socket");
            exit(1);
        }
    }
    else
    {
        printf("Invalid command\n");
    }

    // Read the server's response. Keep reading until the server sends "Done."
    char buffer[256];
    bzero(buffer, 256);
    while (true) {
        int n = read(sockfd, buffer, 255);
        if (n < 0) {
            perror("Error reading from socket");
            exit(1);
        }

        printf("%s", buffer);

        if (strstr(buffer, "Done.") != NULL) {
            break;
        }

        bzero(buffer, 256);
    }

    // Close the socket.
    close(sockfd);

    return 0;
}