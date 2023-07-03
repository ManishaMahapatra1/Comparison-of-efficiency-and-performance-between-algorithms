//MVTO
#include<iostream>
#include<fstream>
#include<stdlib.h>
#include<unistd.h>
#include<pthread.h>
#include<sys/time.h>
#include<vector>
#include<math.h>
#include<algorithm>
#include<map>
#include<random>
#include<atomic>
#include<ctime>
using namespace std;

//Declaring public global variables
std::atomic<int> Gtx_id(0);//For giving unique id's to transactions
double l; //Used to generate exponential distribution for transactions
std::default_random_engine gen;
std::exponential_distribution<double> distribution(l); //To calculate exponential distribution
//Creating map used to keep track of the maximum read shared transaction ID
map<int,int> maxRshd;
ofstream out; // Output stream
//Lock mutex
pthread_mutex_t idLock = PTHREAD_MUTEX_INITIALIZER;//For transaction id assignment
pthread_mutex_t vLock = PTHREAD_MUTEX_INITIALIZER;//For locking data-item/variable
pthread_mutex_t pLock = PTHREAD_MUTEX_INITIALIZER;//pThread lock
pthread_mutex_t mLock = PTHREAD_MUTEX_INITIALIZER;//For measuring abortCountGlobal correctly
map<int,int> status; //0=live; 1=abort; 2=committed
map<int,map<int,int> > v_Versions; //Map that stores the version of each variable
map<int,vector<int> > wL;
vector<int> temp;
//Number of variables, fixed threads, transactions, etc.
	int m;//Number of variables
    int nThreads;//Number of fixed threads
    //int nTx;//Number of transactions per thread
    int constVal;//Generate random number for write operations
    double commitTime = 0.0;
    std::atomic<int> abortCountGlobal(0);//Storing abort count for schedule
map<int, int> tx_log;//For storing data locally

int begin_trans()//Begins transaction and returns an unique id
{
    pthread_mutex_lock(&idLock);
    int Ltx_Id = ++Gtx_id;//Incrementing global transaction id count and assigning to local id
    pthread_mutex_unlock(&idLock);
    pthread_mutex_lock(&vLock);
    status[Ltx_Id] = 0;
    tx_log[Ltx_Id] = 0; //log the transaction locally
    pthread_mutex_unlock(&vLock);
    return Ltx_Id;
}

void cleanup(int tx_id)
{
    tx_log.erase(tx_id); //remove the transaction from local log
}

int read(int tx_id, int dx, int* ptLocal_Val) {
    pthread_mutex_lock(&vLock);
    // Comparing maximum write shared transaction id with the current tx_id and updating as per the condition satisfied
    int pG = -1, i;
    for (i = 0; i < wL[dx].size(); i++) {
        if (wL[dx][i] <= tx_id) {
            // Finding the next highest transaction id and updating the value in the variable if condition is true
            if (wL[dx][i] > pG) {
                pG = wL[dx][i];
            }
        }
    }
    if (pG == -1 || tx_id < maxRshd[dx]) {
        // Transaction is aborted
        status[tx_id] = 1;
        cleanup(tx_id);
        pthread_mutex_unlock(&vLock);
        return 0;
    }
    // Updating pointer to local value for the version of data item for current transaction
    *ptLocal_Val = v_Versions[dx][pG];
    // Add local version to log
    tx_log[dx] = *ptLocal_Val;
    // Updating maximum read shared transaction id
    if (tx_id > maxRshd[dx]) {
        maxRshd[dx] = tx_id;
    }
    pthread_mutex_unlock(&vLock);
    return 1;
}

int write(int tx_id, int dx) {
    pthread_mutex_lock(&vLock);
    // Comparing maximum read/write shared transaction id with the current tx_id and updating as per the condition satisfied
    if (wL[dx].size() > 0 && tx_id < maxRshd[dx]) {
        status[tx_id] = 1;
        cleanup(tx_id);
        pthread_mutex_unlock(&vLock);
        return 0;
    }
    // Add local version to log
    tx_log[dx] = v_Versions[dx][tx_id];
    wL[dx].push_back(tx_id);
    pthread_mutex_unlock(&vLock);
    return 1;
}

int tryCommit(int tx_id, vector<int> commit_Val) {
    int i;
    pthread_mutex_lock(&vLock);
    // Validate all read operations against the logged local versions
    for (auto log_it = tx_log.begin(); log_it != tx_log.end(); log_it++) {
        int dx = log_it->first;
        int local_val = log_it->second;
        if (v_Versions[dx][tx_id] != local_val) {
            status[tx_id] = 1;
            cleanup(tx_id);
            pthread_mutex_unlock(&vLock);
            return 0;
        }
    }
    // Commit the versions for the data item if the condition holds true
    for (i = 0; i < wL.size(); i++) {
        if (find(wL[i].begin(), wL[i].end(), tx_id) != wL[i].end()) {
            v_Versions[i][tx_id] = commit_Val[i];
        }
    }
    status[tx_id] = 2;
    cleanup(tx_id);
    pthread_mutex_unlock(&vLock);
    return 1;
}

string convertTime(time_t time)//Convert the execution time for the transaction for recording in the log
{
    tm * curr_t;
    curr_t = localtime(&time);
	string conv_time = "";
    conv_time = to_string(curr_t->tm_hour)+":"+to_string(curr_t->tm_min)+":"+to_string(curr_t->tm_sec);
	return conv_time;  
}

void* updtMem(void* unused)
{
    int abortCount = 0, status = 0;
    struct timeval critStartT, critEndT;
    abortCount=0;
        do
        {
            gettimeofday(&critStartT,NULL);//Transaction execution start time

            status = 0;
            int tx_id = begin_trans();//Calling function for beginning transaction and assigning transaction ID
            int nIterations = rand()%m;
            vector<int> v_Values;
            for(int i=0;i<m;i++)
            {
                v_Values.push_back(-1);
            }
            for(int i=0;i<nIterations;i++)
            {
                int local_Val = -1;
                int random_Var = rand()%m;
                int random_Val = rand()%constVal;
                random_Val++;
                if(read(tx_id,random_Var, &local_Val)==0)
                {
                    status = 1;
                    break;
                }
                struct timeval readT,writeT;
                gettimeofday(&readT,NULL);//Transaction read operation time

                pthread_mutex_lock(&pLock);//Locking the thread for generating the output for read
                out<<"Thread id: "<<pthread_self()<<" Transaction id: "<<tx_id<<" reads from: "<<random_Var<<" value: "<<local_Val<<" at: "<<convertTime(readT.tv_sec)<<"\n";
                pthread_mutex_unlock(&pLock);//Unlocking the thread

                local_Val= local_Val + random_Val;
                random_Var = rand()%m;
                if(write(tx_id,random_Var)==0)
                {
                    status = 1;
                    break;
                }
                gettimeofday(&writeT,NULL);//Transaction write operation time

                pthread_mutex_lock(&pLock);//Locking the thread for generating the output for write
                out<<"Thread id: "<<pthread_self()<<" Transaction id: "<<tx_id<<" writes to: "<<random_Var<<" value: "<<local_Val<<" at: "<<convertTime(writeT.tv_sec)<<"\n";
                pthread_mutex_unlock(&pLock);//Unlocking the thread

                float randomT = distribution(gen);
                usleep(randomT*1000000);
            }
            string pStat;//For printing the transaction status in log
            if(status!=1)
            {
                if(tryCommit(tx_id, v_Values)==1)//Checking if tx can be committed with the tryCommit function
                {
                    status = 2;
                    pStat = "COMMIT";//Setting the status string for output
                }
                else//If tx cannot be committed as the conditions do not satisfy
                {
                    status = 1;
                }
            }
            if(status==1)//Setting the status string for output as per the status value and incrementing abort count
            {
                pStat = "ABORT";
                abortCount++;
            }
            struct timeval commitT;
            gettimeofday(&commitT,NULL);//Transaction commit operation time

            pthread_mutex_lock(&pLock);//Locking the thread for generating the output for tryCommit
            out<<"Transaction id: "<<tx_id<<" tries to commit with result: "<<pStat<<" at: "<<convertTime(commitT.tv_sec)<<"\n";
            pthread_mutex_unlock(&pLock);//Unlocking the thread
        }
        while(status!=2);
        gettimeofday(&critEndT,NULL);

        pthread_mutex_lock(&mLock);//Lock for measuring global abort count
        abortCountGlobal.fetch_add(abortCount);
        commitTime += critEndT.tv_sec - critStartT.tv_sec + critEndT.tv_usec/1000000.0 - critStartT.tv_usec/1000000.0;
        pthread_mutex_unlock(&mLock);//Unlocking the mutex
}

int main()
{
    srand (time(NULL));

    ifstream in;
    in.open("input.txt");
    in>>nThreads;
    in>>m;
    in>>constVal;
    in>>l;
    temp.push_back(0);
    for(int i=0;i<m;i++)
    {
        wL[i] = temp;
        maxRshd[i] = -1;
        v_Versions[i][0]=0;
    }
    //cout << "" << nThreads << " " << m << " " << constVal << " " << nTx << " " << l << endl;
    pthread_t txId[nThreads];
    out.open("MVTO-log.txt");
    temp.push_back(0);
    //Initializing as per no. of variables from input file
    for(int i=0;i<m;i++)
    {
        wL[i] = temp;
        maxRshd[i] = -1;
        v_Versions[i][0]=0;
    }
    //Creating the pThreads 
    for(int i=0;i<nThreads;i++)
    {
        pthread_create(&txId[i],NULL,updtMem,NULL);
    }
    //Waits for a thread to terminate, detaches the thread, then returns the threads exit status. 
    //As the status parameter is NULL, the threads exit status is not returned.
    for(int i=0;i<nThreads;i++)
    {
        pthread_join(txId[i],NULL);   
    }
    cout<<"Average Commit time = "<<commitTime/(float)(nThreads)<<"\n";
    cout<<"Average Abort count = "<<abortCountGlobal/(float)(nThreads)<<"\n";
    out.close();
    pthread_mutex_destroy(&idLock);
    pthread_mutex_destroy(&vLock);
    pthread_mutex_destroy(&pLock);
    pthread_mutex_destroy(&mLock);
    return 0;
}