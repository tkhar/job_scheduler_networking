/*
File: Util.h

This file contains utility functions that are used by both the jobExecutorServer and jobCommander programs.

*/

#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <queue>
#include <mutex>

using std::string;
using std::vector;
using std::queue;
using std::mutex;

// A job is a triple: jobID, command, status, queuePosition.
struct Job {
    int jobID;
    string command;
    string status;
    int queuePosition;
    
    int socketFd;

    // When a job is running, save its PID.
    int running_pid;
};

// A mutex to protect the job data structures.
mutex jobMutex;

// A queue of jobs.
queue<Job> jobQueue;

// A vector of running jobs
vector<Job> runningJobs;

// The concurrency level of the jobExecutorServer.
// The jobExecutorServer can run at most this number of jobs concurrently.
int concurrencyLevel = 1;

// Universal Job id, starting at 1.
int jobID = 1;

// A flag to indicate that the jobExecutorServer is exiting.
// When this is true, the jobExecutorServer should not accept new jobs.
bool exiting = false;