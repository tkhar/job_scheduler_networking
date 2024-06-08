// File: jobExecutorServer.cpp
// 
// This is the server side. It remains open and receives queries from the jobCommander.
//
// Supports:
// # issueJob <job>: issues a job to the job scheduler
// # setConcurrency <N>: sets the concurrency level to N
// # stop <jobID>: stops a job with the given jobID
// # poll [running,queued]:
// # exit: exits the job commander

#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <vector>
#include <string>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <cassert>
#include <sys/stat.h>
#include <thread>
#include <signal.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "Util.h"

void runJob(Job job);

/*
    This function dequeues a job from the jobQueue and runs it.
*/
void dequeueAndRunJob(      
    bool lock = true
    )
{
    // Acquire the jobMutex.
    if (lock)
    {
        jobMutex.lock();
    }

    // If there are jobs in the queue, run the first job.
    // Only do so if the concurrency level is not reached.
    if (!jobQueue.empty() && runningJobs.size() < concurrencyLevel)
    {
        Job nextJob = jobQueue.front();
        jobQueue.pop();

        nextJob.status = "running";
        runningJobs.push_back(nextJob);

        // We don't need to hold the lock to run the job.
        if(lock)
        {
            jobMutex.unlock();
        }

        // Run the job in a separate thread.
        std::thread t(runJob, nextJob);

        // Detach the thread.
        t.detach();
    }
    else
    {
        if (lock)
        {
            jobMutex.unlock();
        }
    }
}

void runJob(
    int newsockfd,
    Job job)
{
    printf("Running job %d: %s\n", job.jobID, job.command.c_str());

    // Verify that the job is running.
    assert(job.status == "running");

    // Child process. This process waits for the job to be completed.
    pid_t pid = fork();
    if (pid == 0) 
    {
        // Child process. This process runs the job.
        printf("Executing job %d: %s\n", job.jobID, job.command.c_str());

        // Redirect stdout to the socket.
        int o = dup2(newsockfd, STDOUT_FILENO);

        if(o == -1)
        {
            printf("Error: dup2() failed.\n");
            exit(1);
        }

        // Execute the command line.
        execl("/bin/sh", "sh", "-c", job.command.c_str(), NULL);

        // If execl() fails, print an error message.
        printf("Error: execl() failed.\n");
        exit(1);
    }
    else if (pid > 0)
    {
        // Parent process. This process waits for the child process to finish.

        // Close the socket in the parent process.
        close(newsockfd);

        for (int i = 0; i < runningJobs.size(); i++)
        {
            if (runningJobs[i].jobID == job.jobID)
            {
                runningJobs[i].running_pid = pid;
                break;
            }
        }

        // Wait for the child process to finish.
        int status;
        waitpid(pid, &status, 0);

        // Remove the job from the running jobs vector.
        jobMutex.lock();

        // Erase the job from the running jobs vector.
        for (int i = 0; i < runningJobs.size(); i++)
        {
            if (runningJobs[i].jobID == job.jobID)
            {
                runningJobs.erase(runningJobs.begin() + i);
                break;
            }
        }

        jobMutex.unlock();

        printf("Erased job %d from running jobs.\n", job.jobID);
        printf("Now running %d jobs.\n", (int)runningJobs.size());

        // Queue the next job if there is one.
        dequeueAndRunJob();

        if (status != 0)
        {
            // The child process exited abnormally.
            printf("Error: command %s exited abnormally.\n", job.command.c_str());
        }
    }
}

/*
    This function issues a job to the job scheduler.
    It either runs the job immediately or queues it.
    It runs in a worker thread that will join the client's controller thread. 
*/
void issueJob(
    int newsockfd,
    string arguments) 
{
    int n;

    // The argument is a string: the command line of the job.
    string command_line = arguments;

    // Acquire the jobMutex.
    jobMutex.lock();

    // Create the job
    Job job;
    job.jobID = jobID++;
    job.command.assign(command_line);

    printf("Running jobs: %d\n", (int)runningJobs.size());

    // If there are at least concurrencyLevel jobs running, add the job to the queue.
    if (runningJobs.size() >= concurrencyLevel) 
    {
        job.status = "queued";
        job.queuePosition = jobQueue.size();
        jobQueue.push(job);

        printf("Job %d queued.\n", job.jobID);
        printf("Queue position: %d\n", job.queuePosition);
        printf("Job command: %s\n", job.command.c_str());

        jobMutex.unlock();
    }
    else
    {
        job.status = "running";
        job.queuePosition = -1;
        runningJobs.push_back(job);

        // We don't need to hold the lock to run the job.
        jobMutex.unlock();

        // Run the job in a separate thread.
        runJob(newsockfd, job);
    }
}

void setConcurrency(string arguments) 
{
    // The argument is a string: the concurrency level.
    int concurrency = stoi(arguments);

    // Acquire the jobMutex.
    jobMutex.lock();

    // Set the concurrency level.
    concurrencyLevel = concurrency;

    printf("Concurrency level set to: %d\n", concurrencyLevel);

    // If the concurrency level is increased, run the next job until
    // the new concurrency level is reached.
    dequeueAndRunJob(false);

    // Release the jobMutex.
    jobMutex.unlock();
}

void stopJob(
    int newsockfd,
    string arguments) 
{
    int n;

    // The argument is a string: the job ID.
    int jobID = stoi(arguments);

    // Acquire the jobMutex.
    jobMutex.lock();

    // Search for the job in the running jobs vector.
    for (int i = 0; i < runningJobs.size(); i++)
    {
        if (runningJobs[i].jobID == jobID)
        {
            // Send a signal to the process to stop.
            kill(runningJobs[i].running_pid, SIGKILL);

            // Remove the job from the running jobs vector.
            runningJobs.erase(runningJobs.begin() + i);

            printf("Job %d stopped.\n", jobID);

            // Queue the next job if there is one.
            dequeueAndRunJob(false);

            // Release the jobMutex.
            jobMutex.unlock();

            return;
        }
    }

    // Search for the job in the job queue and remove it if found.
    queue<Job> newJobQueue;
    while (!jobQueue.empty())
    {
        Job job = jobQueue.front();
        jobQueue.pop();

        if (job.jobID != jobID)
        {
            newJobQueue.push(job);
            break;
        }
    }

    jobQueue = newJobQueue;

    // Release the jobMutex.
    jobMutex.unlock();
}

void poll(
    int newsockfd,
    string arguments) 
{
    int n;

    // If the argument is "running", print the running jobs.
    if(arguments == "running")
    {
        printf("Polling running jobs.\n");
        jobMutex.lock();

        string output = "Number of running jobs: " + to_string(runningJobs.size()) + "\n";
        n = write(newsockfd, output.c_str(), output.length());
        if (n < 0) 
        {
            printf("Error: writing to socket.\n");
            exit(1);
        }

        jobMutex.unlock();
    }
    else if(arguments == "queued")
    {
        printf("Polling queued jobs.\n");
        jobMutex.lock();

        string output = "Number of queued jobs: " + to_string(jobQueue.size()) + "\n";

        queue<Job> jobQueueCopy = jobQueue;

        while (!jobQueueCopy.empty())
        {
            Job job = jobQueueCopy.front();
            jobQueueCopy.pop();

            output += "Job " + to_string(job.jobID) + ": " + job.command + "\n";
        }

        n = write(newsockfd, output.c_str(), output.length());
        if (n < 0) 
        {
            printf("Error: writing to socket.\n");
            exit(1);
        }

        jobMutex.unlock();
    }
    else
    {
        printf("Invalid argument: %s\n", arguments.c_str());
    
    }
}

/*
    This is run by a controller thread to handle a command.
*/
void handleCommand(int *newsockfd_ptr, int buffer_size) 
{
    int newsockfd = *newsockfd_ptr;
    free(newsockfd_ptr);

    int n;
    char buffer[buffer_size];

    bzero(buffer, buffer_size);
    n = read(newsockfd, buffer, buffer_size - 1);
    if (n < 0) 
    {
        printf("Error: reading from socket.\n");
        exit(1);
    }

    printf("Command received: %s\n", buffer);

    // Put the buffer into a string.
    string buffer_str(buffer);

    // Extract the command and arguments from the buffer.
    string command;
    string arguments;
    size_t pos = buffer_str.find(" ");
    if (pos != string::npos) 
    {
        command = buffer_str.substr(0, pos);
        arguments = buffer_str.substr(pos + 1);
    } 
    else 
    {
        command = buffer_str;
    }

    // Now we have the command and arguments.
    if (command == "issueJob") 
    {
        std::thread t(issueJob, newsockfd, arguments);

        // Wait for the thread to finish.
        t.join();
    }
    else if (command == "setConcurrency") 
    {
        setConcurrency(arguments);
    }
    else if (command == "stop") 
    {
        stopJob(newsockfd, arguments);
    }
    else if (command == "poll") 
    {
        poll(newsockfd, arguments);
    }
    else if (strcmp(command.c_str(), "exit") == 0) 
    {
        printf("Exiting.\n");
        exiting = true;
    }
    else
    {
        assert(false); // We should never get here.
        printf("Invalid command: %s\n", command.c_str());
    }

    //
    // At this point the command is done.
    //

    // Before closing the socket, send a message to the jobCommander that the command is done.
    // This is necessary because the jobCommander is blocked on the read() call.
    n = write(newsockfd, "Done", 4);
    if (n < 0) 
    {
        printf("Error: writing to socket.\n");
        exit(1);
    }

    close(newsockfd);
}

int main(int argc, char *argv[]) 
{
    printf("Job Executor Server started.\n");
    printf("PID: %d\n", getpid());

    // Arguments: prompt> jobExecutorServer [portnum] [bufferSize]  [threadPoolSize]
    if (argc != 4) 
    {
        printf("Usage: %s [portnum] [bufferSize] [threadPoolSize]\n", argv[0]);
        exit(1);
    }

    // Extract the port number, buffer size, and thread pool size from the arguments.
    // Do some parameter checking.
    int portnum = atoi(argv[1]);
    if (portnum < 1024 || portnum > 65535) 
    {
        printf("Error: invalid port number.\n");
        exit(1);
    }
    int bufferSize = atoi(argv[2]);
    if (bufferSize < 1) 
    {
        printf("Error: invalid buffer size.\n");
        exit(1);
    }

    int threadPoolSize = atoi(argv[3]);
    if (threadPoolSize < 1) 
    {
        printf("Error: invalid thread pool size.\n");
        exit(1);
    }

    // Create a socket.
    int sockfd, n;
    socklen_t clilen;
    char buffer[bufferSize];
    sockaddr_in serv_addr, cli_addr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        printf("Error: opening socket.\n");
        exit(1);
    }

    // Initialize the server address.
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portnum);

    // Bind the socket to the server address.
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
    {
        printf("Error: binding.\n");
        exit(1);
    }

    // Listen for connections.
    listen(sockfd, 5);
    clilen = sizeof(cli_addr);

    while(!exiting)
    {
        // Accept a connection from a jobCommander.
        int *newsockfd_ptr = (int *)malloc(sizeof(int));
        *newsockfd_ptr = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (*newsockfd_ptr < 0) 
        {
            printf("Error: accepting.\n");
            exit(1);
        }

        // Issue a controller thread to handle the command.
        std::thread t(handleCommand, newsockfd_ptr, bufferSize);
        t.detach();
    }

    close(sockfd);
    return 0;
}
