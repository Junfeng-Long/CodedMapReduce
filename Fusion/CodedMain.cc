#include <iostream>
#include <mpi.h>

#include "CodedMaster.h"
#include "CodedWorker.h"

using namespace std;

int main()
{
  MPI::Init();
  int nodeRank = MPI::COMM_WORLD.Get_rank();
  int nodeTotal = MPI::COMM_WORLD.Get_size();

  if ( nodeRank == 0 ) {
    
    CodedMaster masterNode( nodeRank, nodeTotal );
    //MPI::COMM_WORLD.Split( 1, nodeRank );
    //MPI::Intracomm workerComm = MPI::COMM_WORLD.Split( 0, nodeRank );
    MPI::Intracomm workerComm;
    MPI_Comm_dup(MPI_COMM_WORLD, workerComm);
    MPI::Intracomm COM = MPI::COMM_WORLD.Split( 0, nodeRank );
    masterNode.setWorkerComm( workerComm );
    masterNode.run();
  }
  else {
    //cout<<nodeRank;
    CodedWorker workerNode( nodeRank );
    MPI::Intracomm workerCommACDC;
    MPI_Comm_dup(MPI_COMM_WORLD, workerCommACDC);
    MPI::Intracomm workerCommCDC = MPI::COMM_WORLD.Split( 1, nodeRank );
    workerNode.setWorkerComm( workerCommACDC, workerCommCDC );
    //cout<<"Node"<<nodeRank<<" Split succeed"<<endl;
    workerNode.run();
  }

  MPI::Finalize();
  
  return 0;
}
