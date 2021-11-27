#ifndef _CMR_CONFIGURATION
#define _CMR_CONFIGURATION

#include "Configuration.h"

class CodedConfiguration : public Configuration {

 private:
  unsigned int load;
  
 public:
 CodedConfiguration(): Configuration() {
    numInput = 6;    // N is assumed to be K choose r     
    numReducer = 3;  // K
    load = 2;        // r  
    master_file = 3;
    worker_file = 3;  
    
    inputPath = "../Input1/Input10G";
    outputPath = "../Output1/Output10G";
    partitionPath = "../Partition1/Partition10000-C";
    numSamples = 10000;    
  }
  ~CodedConfiguration() {}

  unsigned int getLoad() const { return load; }
  unsigned int getNumMasterFile() const { return master_file; }
  unsigned int getNumWorkerFile() const { return master_file; }
};

#endif
