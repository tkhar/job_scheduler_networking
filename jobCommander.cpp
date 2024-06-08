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

#include "Util.h"

using namespace std;

int main(int argc, char *argv[]) 
{
    char *command;

    // Parse the command line arguments.
    if (argc < 2) {
        printf("Usage: jobCommander <command>\n");
        exit(1);
    }

    // Check if the jobExecutorServer.txt file exists.
    // If not, run the jobExecutorServer program.
    FILE *file = fopen("jobExecutorServer.txt", "r");
    if (file == NULL) {
        printf("Error: jobExecutorServer is not running.\n");
    
        // Run the jobExecutorServer program via fork and exec.
        pid_t pid = fork();
        if (pid == 0) {
            execl("./jobExecutorServer", "jobExecutorServer", NULL);
        }
        else
        {
            // Wait for the jobExecutorServer to start.
            sleep(1);
        }
    }

    // Close the file.
    fclose(file);

    command = argv[1];

    // Create a named pipe to communicate with the jobExecutorServer.
    int fd;
    const char *pipe = "/tmp/jobCommanderPipe";

    // Give the output pipe the name /tmp/jobCommanderPipeOutput + PID.
    int fd_output;
    char pipe_output[100];
    snprintf(pipe_output, sizeof(pipe_output), "/tmp/jobCommanderPipeOutput%d", getpid());

    // Create the named pipe.
    mkfifo(pipe_output, 0666);

    // Open the pipe for reading.
    fd_output = open(pipe_output, O_RDONLY | O_NONBLOCK);

    // Issue a job to the job scheduler.
    if (strcmp(command, "issueJob") == 0) 
    {
        if (argc < 3) {
            printf("Usage: jobCommander issueJob <job>\n");
            exit(1);
        }

        // The command starts with the output pipe name
        string issue_job_command = pipe_output;

        // The command is issueJob and the arguments is the concatenation of 
        // the remaining strings argv[2], argv[3], ...
        issue_job_command += " issueJob " + string(argv[2]);
        for (int i = 3; i < argc; i++) {
            issue_job_command += " ";
            issue_job_command += string(argv[i]);
        }

        printf("Issuing job: %s\n", issue_job_command.c_str());

        // Open the pipe and write the command to it.
        fd = open(pipe, O_WRONLY);
        write(fd, issue_job_command.c_str(), issue_job_command.length() + 1);
        close(fd);
    }
    else if(strcmp(command, "setConcurrency") == 0)
    {
        if (argc < 3) {
            printf("Usage: jobCommander setConcurrency <N>\n");
            exit(1);
        }
        printf("Setting concurrency to: %s\n", argv[2]);

        // Concatenate the command and the concurrency level.
        string concurrency = string(argv[2]);
        string set_concurrency_command;
        set_concurrency_command.assign(pipe_output);
        set_concurrency_command += " setConcurrency " + concurrency;

        // Open the pipe and write the command to it.
        fd = open(pipe, O_WRONLY);
        write(fd, set_concurrency_command.c_str(), set_concurrency_command.length() + 1);
        close(fd);
    }
    else if(strcmp(command, "stop") == 0)
    {
        if (argc < 3) {
            printf("Usage: jobCommander stop <jobID>\n");
            exit(1);
        }
        printf("Stopping job: %s\n", argv[2]);

        // Concatenate the command and the job ID.
        string jobID = string(argv[2]);

        string stop_job_command;
        stop_job_command.assign(pipe_output);
        stop_job_command += " stop " + jobID;

        // Open the pipe and write the command to it.
        fd = open(pipe, O_WRONLY);
        write(fd, stop_job_command.c_str(), stop_job_command.length() + 1);
        close(fd);
    }
    else if(strcmp(command, "poll") == 0)
    {
        if (argc < 3) {
            printf("Usage: jobCommander poll [running,queued]\n");
            exit(1);
        }
        printf("Polling: %s\n", argv[2]);

        // Concatenate the command and the status.
        string status = string(argv[2]);

        // The poll command first contains the output pipe 
        // and then the poll command and the status.
        string poll_command;
        poll_command.assign(pipe_output);
        poll_command += " poll " + status;

        // Open the pipe and write the command to it.
        fd = open(pipe, O_WRONLY);
        write(fd, poll_command.c_str(), poll_command.length() + 1);
        close(fd);
    }
    else if(strcmp(command, "exit") == 0)
    {
        printf("Exiting jobExecutorServer\n");

        string exit_command;
        exit_command.assign(pipe_output);
        exit_command += " exit";

        // Open the pipe and write the command to it.
        fd = open(pipe, O_WRONLY);
        write(fd, exit_command.c_str(), exit_command.length() + 1);
        close(fd);
    }
    else
    {
        printf("Invalid command\n");
    }

    // Keep reading from the output pipe until we get the message "Done"
    while(true)
    {
        // Read the output from the pipe.
        char buffer[1024 * 1024];
        while (read(fd_output, buffer, sizeof(buffer)) <= 0) {
            sleep(1);
        }

        // If buffer contains "Done" as a substring, break.
        string output(buffer);
        if (output.find("Done") != string::npos) {
            break;
        }


        // Print the output.
        printf("%s", output.c_str());
    }

    // Close the pipe.
    close(fd_output);

    // Remove the named pipe.
    unlink(pipe_output);

    return 0;
}