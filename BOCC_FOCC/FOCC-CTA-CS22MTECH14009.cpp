//FOCC-CTA
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
double l;
std::default_random_engine gen;
std::exponential_distribution<double> distribution(l); //To calculate exponential distribution
// Creating the ReadSet, WriteSet
map<int,vector<int> > rL;//Functions as a buffer
map<int,vector<int> > rS;//For storing set of read operations for current transaction
map<int,vector<int> > wS;
ofstream out; // Output stream
//Lock mutex
pthread_mutex_t idLock = PTHREAD_MUTEX_INITIALIZER;//For transaction id assignment
pthread_mutex_t vLock = PTHREAD_MUTEX_INITIALIZER;//For locking data-item/variable
pthread_mutex_t pLock = PTHREAD_MUTEX_INITIALIZER;//pThread lock
pthread_mutex_t mLock = PTHREAD_MUTEX_INITIALIZER;//For measuring abortCountGlobal correctly
pthread_mutex_t cLock = PTHREAD_MUTEX_INITIALIZER;//For locking cleanup function so that no other transaction access it while one transaction is executing it
map<int,int> status; //0=live; 1=abort; 2=committed
//Number of variables, fixed threads, transactions, etc.
	int m;//Number of variables
    int nThreads;//Number of fixed threads
    int nTx;//Number of transactions per thread
    int constVal;//Generate random number for write operations
    double commitTime = 0.0;
    std::atomic<int> abortCountGlobal(0);//Storing abort count for schedule
    vector<int> temp;//Dynamic array for storing values for r, w for each transaction

int begin_trans()//Begins transaction and returns an unique id
{
    pthread_mutex_lock(&idLock);
    int Ltx_Id = ++Gtx_id;//Incrementing global transaction id count and assigning to local id
    pthread_mutex_unlock(&idLock);
    pthread_mutex_lock(&vLock);
    rS[Ltx_Id] = temp;
    wS[Ltx_Id] = temp;
    status[Ltx_Id] = 0;
    pthread_mutex_unlock(&vLock);
    return Ltx_Id;
}

void cleanup(int tx_id)
{
    pthread_mutex_lock(&cLock);
    vector<int>::iterator pos;//Using vector iterators for the required functions for reducing complexity and running time
    wS.erase(tx_id);//Removing current transaction operations from write-set
    rS.erase(tx_id);//Removing current transaction operations from read-set
    for (int i = 0; i < m; i++)
    {
        pos = find(rL[i].begin(), rL[i].end(), tx_id);
        if (pos != rL[i].end())
        {
            rL[i].erase(pos);
        }
    }
    status.erase(tx_id);
    pthread_mutex_unlock(&cLock);
}

int read(int tx_id,int dx)//Reads the data-item value into local variable for a tx
{
    pthread_mutex_lock(&vLock);
    if(status[tx_id] == 1)//check transaction status
    {
        cleanup(tx_id);
        pthread_mutex_unlock(&vLock);
        return 0;
    }
    //Updating the read-set and read-list with current transaction operations
    rS[tx_id].push_back(dx);//push the element in the vector from the back
    rL[dx].push_back(tx_id);
    pthread_mutex_unlock(&vLock);
    return 1;
}

int write(int tx_id,int dx)//Writes the data-item to the new value of the local variable for a tx
{
    pthread_mutex_lock(&vLock);
    if(status[tx_id] == 1)//check transaction status
    {
        cleanup(tx_id);
        pthread_mutex_unlock(&vLock);
        return 0;
    }
    //Updating the write-set and write-list with current transaction operations
    wS[tx_id].push_back(dx);//push the element in the vector from the back
    pthread_mutex_unlock(&vLock);
    return 1;
}

int tryCommit(int tx_id)//returns 0 - abort or 1 - commit
{
    int ct=0, i;
    pthread_mutex_lock(&vLock);
    if (status[tx_id] == 1)//check transaction status
    {
        cleanup(tx_id);
        pthread_mutex_unlock(&vLock);
        return 0;
    }
    int size = wS[tx_id].size();
    //Checking intersection of write-set with read-set of live transactions by comparing with read-list
    for (i = 0; i < size; i++)
    {
        while(status[i] == 0)//Considering concurrent live transactions
        {
            if(rL[wS[tx_id][i]].size()>0)
            {
                cleanup(tx_id);//If WS[j] intersection RS^n[i] != null => Contradiction, then remove the transaction
                pthread_mutex_unlock(&vLock);
                return 0;
            }
        }
    }
    status[tx_id] = 2;//Set status for current transaction if committed
    ct = 1;
    pthread_mutex_unlock(&vLock);
    return ct;
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
    int abortCount = 0, cTx, status = 0;
    struct timeval critStartT, critEndT;

    for(cTx=0;cTx<nTx;cTx++)
    {
        abortCount=0;
        do
        {
            gettimeofday(&critStartT,NULL);//Transaction execution start time

            status = 0;
            int tx_id = begin_trans();//Calling function for beginning transaction and assigning transaction ID
            int nIterations = rand()%m;
            int local_Val, i;
            for(i=0;i<nIterations;i++)
            {
                local_Val = rand()%m;
                int random_Var = rand()%m;
                int random_Val = rand()%constVal;
                if(read(tx_id,random_Var)==0)
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
                if(tryCommit(tx_id)==1)//Checking if tx can be committed with the tryCommit function
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
}

int main()
{
    srand (time(NULL));

    ifstream in;
    in.open("input.txt");
    in>>nThreads;
    in>>m;
    in>>nTx;
    in>>constVal;
    in>>l;
    //cout << "" << nThreads << " " << m << " " << constVal << " " << nTx << " " << l << endl;
    nTx = nTx/nThreads; //no. of transaction per thread
    for(int i=0;i>m;i++)
    {
        rL[i] = temp;
    }
    pthread_t txId[nThreads];
    out.open("FOCC-CTA-log.txt");
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
    cout<<"Average Commit time = "<<commitTime/(float)(nThreads*nTx)<<"\n";
    cout<<"Average Abort count = "<<abortCountGlobal/(float)(nThreads*nTx)<<"\n";
    out.close();
    pthread_mutex_destroy(&idLock);
    pthread_mutex_destroy(&vLock);
    pthread_mutex_destroy(&pLock);
    pthread_mutex_destroy(&mLock);
    pthread_mutex_destroy(&cLock);
    return 0;
}
