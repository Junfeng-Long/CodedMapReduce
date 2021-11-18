#ifndef _CMR_MASTER
#define _CMR_MASTER

#include <mpi.h>
#include <unordered_map>
#include <pthread.h>
#include "CodedConfiguration.h"
#include "CodeGeneration.h"
#include "Utility.h"
#include "Trie.h"

class CodedMaster
{
 public:/*define some useful data structure*/
  typedef unordered_map< unsigned int, LineList* > PartitionCollection; // key = destID
  typedef unordered_map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID
  typedef unordered_map< SubsetSId, MPI::Intracomm > MulticastGroupMap;
  /* typedef map< unsigned int, LineList* > PartitionCollection; // key = destID */
  /* typedef map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID */
  /* typedef map< SubsetSId, MPI::Intracomm > MulticastGroupMap;   */
  typedef map< Vpair, unsigned long long > VpairSizeMap;
  //typedef pair< int, unsigned long> dfpair;
  typedef struct _DataChunk {
    unsigned char* data;
    unsigned long long size; // number of lines
  } DataChunk;
  typedef map< VpairList, vector< DataChunk > > DataPartMap;
  typedef unordered_map< SubsetSId, DataPartMap > NodeSetDataMap;  // [Encode/Decode]PreData
  /* typedef map< SubsetSId, DataPartMap > NodeSetDataPartMap;  // [Encode/Decode]PreData   */
  
  typedef struct _MetaData {
    VpairList vpList;
    VpairSizeMap vpSize;
    unsigned int partNumber; // { 1, 2, ... }
    unsigned long long size; // number of lines
  } MetaData;
  
  typedef struct _EnData {
    vector< MetaData > metaList;
    unsigned char* data;          // encoded chunk
    unsigned long long size;      // in number of lines
    unsigned char* serialMeta;
    unsigned long long metaSize;  // in number of bytes
  } EnData;
  typedef unordered_map< SubsetSId, EnData > NodeSetEnDataMap;  // SendData
  typedef unordered_map< SubsetSId, vector< EnData > > NodeSetVecEnDataMap;  // RecvData
  /* typedef map< SubsetSId, EnData > NodeSetEnDataMap;  // SendData */
  /* typedef map< SubsetSId, vector< EnData > > NodeSetVecEnDataMap;  // RecvData   */
  

  typedef struct {
    SubsetSId sid;
    EnData endata;
  } DecodeJob;


 private:
  CodedConfiguration conf;
  unsigned int rank;
  unsigned int totalNode;
  CodeGeneration* cg;
  MPI::Intracomm workerComm;
  PartitionList partitionList;
  InputPartitionCollection inputPartitionCollection;
  LineList localList;
  NodeSet localLoadSet;
  TrieNode* trie;
  NodeSetDataMap encodePreData;
  NodeSetEnDataMap encodeDataSend;
  MulticastGroupMap multicastGroupMap;
  pthread_t decodeThread;

 public:
 CodedMaster( unsigned int _rank, unsigned int _totalNode ): rank( _rank ), totalNode( _totalNode ) {};
  ~CodedMaster() {};

  void run();
  void execMap();
  void execEncoding();
  void execShuffle();
  void genMulticastGroup();
  void setWorkerComm( MPI::Intracomm& comm ) { workerComm = comm; }
  void sendEncodeData( CodedMaster::EnData& endata, MPI::Intracomm& comm );
  TrieNode* buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth );

};




#endif
