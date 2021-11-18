#ifndef _CMR_CONFIGURATION
#define _CMR_CONFIGURATION

#include "Configuration.h"

class CodedConfiguration : public Configuration {

 private:
  unsigned int load;
  
 public:
 CodedConfiguration(): Configuration() {
    numInput = 252;    // N is assumed to be K choose r     
    numReducer = 10;  // K
    load = 5;        // r    
    
    inputPath = "./Input/Input10G";
    outputPath = "./Output/Output10G";
    partitionPath = "./Partition/Partition10000-C";
    numSamples = 10000;    
  }
  ~CodedConfiguration() {}

  unsigned int getLoad() const { return load; }
};

#endif
