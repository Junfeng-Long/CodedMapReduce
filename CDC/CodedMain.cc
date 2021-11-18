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
    MPI::Intracomm COM = MPI::COMM_WORLD.Split( 0, nodeRank );
    /* int o_rank, c_rank;
    int o_size, c_size; */
    /* MPI_Comm_rank(MPI_COMM_WORLD, &o_rank);
    MPI_Comm_rank(COM, &c_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &o_size);
    MPI_Comm_size(COM, &c_size);
    cout<<"master o_rank: "<<o_rank<<"    "<<"master c_rank"<<"   "<<c_rank<<"    "<<o_size<<"    "<<c_size<<endl; */
    masterNode.run();
  }
  else {
    CodedWorker workerNode( nodeRank );
    MPI::Intracomm workerComm = MPI::COMM_WORLD.Split( 1, nodeRank );
    
    //cout<<"worker o_rank: "<<o_rank<<"    "<<"worker c_rank"<<"   "<<c_rank<<"    "<<o_size<<"    "<<c_size<<endl;
    workerNode.setWorkerComm( workerComm );
    workerNode.run();
  }

  MPI::Finalize();
  
  return 0;
}
