#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <map>
#include <unordered_map>
#include <assert.h>
#include <algorithm>
#include <ctime>
#include <string.h>
#include <cstdint>
#include <iomanip>

#include "CodedMaster.h"
#include "Common.h"
#include "CodedConfiguration.h"
#include "PartitionSampling.h"
#include "Trie.h"
#include <fstream>
#include "CodeGeneration.h"


using namespace std;

void CodedMaster::run()
{
  cg = new CodeGeneration( conf.getNumInput(), conf.getNumReducer(), conf.getLoad() );
  if ( totalNode != 1 + conf.getNumReducer() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }
  


  
  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  PartitionList* mpartitionList = partitioner.createPartitions();
  for ( auto it = mpartitionList->begin(); it != mpartitionList->end(); it++ ) {
    partitionList.push_back(*it);
  }
  




  // BROADCAST CONFIGURATION TO WORKERS
  MPI::COMM_WORLD.Bcast( &conf, sizeof( CodedConfiguration ), MPI::CHAR, 0 );
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.


  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = mpartitionList->begin(); it != mpartitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
  }

  genMulticastGroup();


  // TIME BUFFER
  int numWorker = conf.getNumReducer();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


  // COMPUTE CODE GENERATION TIME  
  //cout<<"Node"<<rank<<" Code Generating"<<endl;
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": CODEGEN | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl; 
      
  

  

  // COMPUTE MAP TIME
  //cout<<"Node"<<rank<<" Map"<<endl;
  clock_t mtime;
  mtime = clock();
  execMap();
  mtime = clock() - mtime;

  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  avgTime += double( mtime ) / CLOCKS_PER_SEC;  
  maxTime = max(maxTime, double( mtime ) / CLOCKS_PER_SEC);
  cout << rank
       << ": MAP     | Avg = " << setw(10) << avgTime/(numWorker+1)
       << "   Max = " << setw(10) << maxTime << endl;  
 
  
  // COMPUTE ENCODE TIME
  //cout<<"Node"<<rank<<" Encode"<<endl;
  clock_t etime;
  etime = clock();
  execEncoding();
  etime = clock() - etime;

  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  avgTime += double( etime ) / CLOCKS_PER_SEC;  
  maxTime = max(maxTime, double( etime ) / CLOCKS_PER_SEC);
  cout << rank
       << ": ENCODE  | Avg = " << setw(10) << avgTime/(numWorker+1)
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE SHUFFLE TIME
  execShuffle();
  /* avgTime = 0;
  double txRate = 0;
  double avgRate = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    MPI::COMM_WORLD.Recv( &rTime, 1, MPI::DOUBLE, i, 0 );
    avgTime += rTime;    
    MPI::COMM_WORLD.Recv( &txRate, 1, MPI::DOUBLE, i, 0 );
    avgRate += txRate;
  }
  cout << rank
       << ": SHUFFLE | Sum = " << setw(10) << avgTime
       << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl;
 */ 
  

  
  //cout<<"Shuffle";
  // COMPUTE DECODE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": DECODE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  

  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank
       << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
  
 
  // CLEAN UP MEMORY
  for ( auto it = mpartitionList->begin(); it != mpartitionList->end(); it++ ) {
    delete [] *it;
  }
}

void CodedMaster::execMap() 
{
  // Get a set of inputs to be processed
  InputSet inputSet = cg->getM( rank );

  // Build trie
  unsigned char prefix[ conf.getKeySize() ];
  trie = buildTrie( &partitionList, 0, partitionList.size(), prefix, 0, 2 );
  

  // Read input files and partition data
  for ( auto init = inputSet.begin(); init != inputSet.end(); init++ ) {
    unsigned int inputId = *init;

    // Read input
    char filePath[ MAX_FILE_PATH ];
    sprintf( filePath, "%s_%d", conf.getInputPath(), inputId - 1 );
    ifstream inputFile( filePath, ios::in | ios::binary | ios::ate );
    if ( !inputFile.is_open() ) {
      cout << rank << ": Cannot open input file " << filePath << endl;
      assert( false );
    }

    unsigned long fileSize = inputFile.tellg();
    unsigned long int lineSize = conf.getLineSize();
    unsigned long int numLine = fileSize / lineSize;
    inputFile.seekg( 0, ios::beg );
    PartitionCollection& pc = inputPartitionCollection[ inputId ];

    // Create lists of lines
    for ( unsigned int i = 0; i < conf.getNumReducer(); i++ ) {
      pc[ i ] = new LineList;
      // inputPartitionCollection[ inputId ][ i ] = new LineList;
    }
    
    // Partition data in the input file
    for ( unsigned long i = 0; i < numLine; i++ ) {
      unsigned char* buff = new unsigned char[ lineSize ];
      inputFile.read( ( char * ) buff, lineSize );
      unsigned int wid = trie->findPartition( buff );
      pc[ wid ]->push_back( buff );
      // inputPartitionCollection[ inputId ][ wid ]->push_back( buff );
    }

    // Remove unnecessarily lists (partitions associated with the other nodes having the file)
    
    NodeSet fsIndex = cg->getNodeSetFromFileID( inputId );
    for ( unsigned int i = 0; i < conf.getNumReducer(); i++ ) {
      if( fsIndex.find( i + 1 ) != fsIndex.end() ) {
        LineList* list = pc[ i ];
        // LineList* list = inputPartitionCollection[ inputId ][ i ];
        for ( auto lit = list->begin(); lit != list->end(); lit++ ) {
          delete [] *lit;
        }
        delete list;	
      }
    }
    

    inputFile.close();
  }
  //writeInputPartitionCollection();
}


void CodedMaster::execEncoding()
{ 
  vector< NodeSet > SetS =  cg->getNodeSubsetS();
  int Snum = SetS.size();
  
  for (int i = 0; i < Snum; i++){
    NodeSet subsetS = cg->getSubsetSFromId(i);
    unsigned lineSize = conf.getLineSize();
    SubsetSId nsid = i;
    unsigned long long maxSize = 0;
    // Construct chucks of input from data with index ns\{q}
    for( auto qit = subsetS.begin(); qit != subsetS.end(); qit++ ) {
      int destId = *qit;      
      NodeSet inputIdx( subsetS );
      inputIdx.erase( destId );
      
      unsigned long fid = cg->getFileIDFromNodeSet( inputIdx );
      VpairList vplist;
      vplist.push_back( Vpair( destId, fid ) );
      
      unsigned int partitionId = destId - 1;
      //cout<<destId<<"  "<<fid<<endl;
      LineList* ll = inputPartitionCollection[ fid ][ partitionId ];

      auto lit = ll->begin();
      unsigned long long Size = ll->size(); // a number of lines ( not bytes )
      maxSize = max(maxSize, Size);
      // first chunk to second last chunk
      unsigned char* data = new unsigned char[ Size * lineSize ];
      for( unsigned long long j = 0; j < Size; j++ ) {
        memcpy( data + j * lineSize, *lit, lineSize );
        lit++;
      }
      DataChunk dc;
      dc.data = data;
      dc.size = Size;
      encodePreData[ nsid ][ vplist ].push_back( dc );
      delete ll;      
    }

    // Initialize encode data
    encodeDataSend[ nsid ].data = new unsigned char[ maxSize * lineSize ](); // Initial it with 0
    encodeDataSend[ nsid ].size = maxSize;
    unsigned char* data = encodeDataSend[ nsid ].data;

    // Encode Data
    for( auto qit = subsetS.begin(); qit != subsetS.end(); qit++ ) {

      int destId = *qit;      
      NodeSet inputIdx( subsetS );
      inputIdx.erase( destId );
      unsigned long fid = cg->getFileIDFromNodeSet( inputIdx );
      VpairList vplist;
      vplist.push_back( Vpair( destId, fid ) );

      // Start encoding
      unsigned char* predata = encodePreData[ nsid ][ vplist ][0].data;
      unsigned long long size = encodePreData[ nsid ][ vplist ][0].size;
      unsigned long long maxiter = size * lineSize / sizeof( uint32_t );
      for( unsigned long long i = 0; i < maxiter; i++ ) {
        ( ( uint32_t* ) data )[ i ] ^= ( ( uint32_t* ) predata )[ i ];
      }

      // Fill metadata
      MetaData md;
      md.vpList = vplist;
      md.vpSize[ vplist[ 0 ] ] = size; // Assume Eta = 1;
      md.size = size; 
      encodeDataSend[ nsid ].metaList.push_back( md );
    }

    // Serialize Metadata
    EnData& endata = encodeDataSend[ nsid ];
    unsigned int ms = 0;
    ms += sizeof( unsigned int ); // metaList.size()
    for ( unsigned int m = 0; m < endata.metaList.size(); m++ ) {
      ms += sizeof( unsigned int ); // vpList.size()
      ms += sizeof( int ) * 2 * endata.metaList[ m ].vpList.size(); // vpList
      ms += sizeof( unsigned int ); // vpSize.size()
      ms += ( sizeof( int ) * 2 + sizeof( unsigned long long ) ) * endata.metaList[ m ].vpSize.size(); // vpSize
      ms += sizeof( unsigned int ); // partNumber
      ms += sizeof( unsigned long long ); // size
    }
    encodeDataSend[ nsid ].metaSize = ms;

    unsigned char* mbuff = new unsigned char[ ms ];
    unsigned char* p = mbuff;
    unsigned int metaSize = endata.metaList.size();
    memcpy( p, &metaSize, sizeof( unsigned int ) );
    p += sizeof( unsigned int );
    // meta data List
    for ( unsigned int m = 0; m < metaSize; m++ ) {
      MetaData mdata = endata.metaList[ m ];
      unsigned int numVp = mdata.vpList.size();
      memcpy( p, &numVp, sizeof( unsigned int ) );
      p += sizeof( unsigned int );
      // vpair List
      for ( unsigned int v = 0; v < numVp; v++ ) {
        memcpy( p, &( mdata.vpList[ v ].first ), sizeof( int ) );
        p += sizeof( int );
        memcpy( p, &( mdata.vpList[ v ].second ), sizeof( int ) );
        p += sizeof( int );
      }
      // vpair size Map
      unsigned int numVps = mdata.vpSize.size();
      memcpy( p, &numVps, sizeof( unsigned int ) );
      p += sizeof( unsigned int );
      for ( auto vpsit = mdata.vpSize.begin(); vpsit != mdata.vpSize.end(); vpsit++ ) {
        Vpair vp = vpsit->first;
        unsigned long long size = vpsit->second;
        memcpy( p, &( vp.first ), sizeof( int ) );
        p += sizeof( int );
        memcpy( p, &( vp.second ), sizeof( int ) );
        p += sizeof( int );
        memcpy( p, &size, sizeof( unsigned long long ) );
        p += sizeof( unsigned long long );
      }
      memcpy( p, &( mdata.partNumber ), sizeof( unsigned int ) );
      p += sizeof( unsigned int );
      memcpy( p, &( mdata.size ), sizeof( unsigned long long ) );
      p += sizeof( unsigned long long );
    }
    encodeDataSend[ nsid ].serialMeta = mbuff;
  }
}

void CodedMaster::execShuffle()
{
  clock_t tolSize = 0;
  clock_t time = 0;
  clock_t txTime = 0;
  time -= clock();
  vector< NodeSet > SetS =  cg->getNodeSubsetS();
  int Snum = SetS.size();
  
  for (int i = 0; i < Snum; i++){
    MPI::Intracomm mcComm = multicastGroupMap[ i ];
    txTime -= clock();
    sendEncodeData( encodeDataSend[ i ], mcComm );
    txTime += clock();
    tolSize += ( encodeDataSend[ i ].size * conf.getLineSize() ) + encodeDataSend[ i ].metaSize + ( 2 * sizeof(unsigned long long ) );
  }
  time += clock();
  cout << rank
       << ": SHUFFLE | Sum = " << setw(10) << double(time) / CLOCKS_PER_SEC
       << "   Rate = " << setw(10) << ( tolSize * 8 * 1e-6 ) / ( double( txTime ) / CLOCKS_PER_SEC ) << " Mbps" << endl;
}

TrieNode* CodedMaster::buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth )
{
  if ( prefixSize >= maxDepth || lower == upper ) {
    return new LeafTrieNode( prefixSize, partitionList, lower, upper );
  }
  InnerTrieNode* result = new InnerTrieNode( prefixSize );
  int curr = lower;
  for ( unsigned char ch = 0; ch < 255; ch++ ) {
    prefix[ prefixSize ] = ch;
    lower = curr;
    while( curr < upper ) {
      if( cmpKey( prefix, partitionList->at( curr ), prefixSize + 1 ) ) {
	break;
      }
      curr++;
    }
    result->setChild( ch, buildTrie( partitionList, lower, curr, prefix, prefixSize + 1, maxDepth ) );
  }
  prefix[ prefixSize ] = 255;
  result->setChild( 255, buildTrie( partitionList, curr, upper, prefix, prefixSize + 1, maxDepth ) );
  return result;
}

void CodedMaster::genMulticastGroup()
{
  map< NodeSet, SubsetSId > ssmap = cg->getSubsetSIdMap();
  for( auto nsit = ssmap.begin(); nsit != ssmap.end(); nsit++ ) {
    NodeSet ns = nsit->first;
    SubsetSId nsid = nsit->second;
    MPI::Intracomm mgComm = workerComm.Split(1, rank);
    multicastGroupMap[ nsid ] = mgComm;
  }
}

void CodedMaster::sendEncodeData( CodedMaster::EnData& endata, MPI::Intracomm& comm )
{
  // Send actual data
  unsigned lineSize = conf.getLineSize();  
  int rootId = 0;
  comm.Bcast( &( endata.size ), 1, MPI::UNSIGNED_LONG_LONG, rootId );
  
  comm.Bcast( endata.data, endata.size*lineSize, MPI::UNSIGNED_CHAR, rootId );
  
  delete [] endata.data;

  // Send serialized meta data
  comm.Bcast( &( endata.metaSize ), 1, MPI::UNSIGNED_LONG_LONG, rootId ); 
  comm.Bcast( endata.serialMeta, endata.metaSize, MPI::UNSIGNED_CHAR, rootId );
  delete [] endata.serialMeta;
}


