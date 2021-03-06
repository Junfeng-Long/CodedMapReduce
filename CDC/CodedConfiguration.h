#ifndef _CMR_CONFIGURATION
#define _CMR_CONFIGURATION

#include "Configuration.h"

class CodedConfiguration : public Configuration {

 private:
  unsigned int load;
  
 public:
 CodedConfiguration(): Configuration() {
    numInput = 3;    // N is assumed to be K choose r     
    numReducer = 3;  // K
    load = 2;        // r    
    
    inputPath = "../Input/Input1M";
    outputPath = "../Output/Output1M";
    partitionPath = "./Partition/Partition10000-C";
    numSamples = 10000;    
  }
  ~CodedConfiguration() {}

  unsigned int getLoad() const { return load; }
};

#endif
