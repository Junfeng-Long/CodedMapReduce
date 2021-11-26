#ifndef _MR_CONFIGURATION
#define _MR_CONFIGURATION

class Configuration {

 protected:
  unsigned int numReducer;
  unsigned int numInput;  
  unsigned int master_file;
  unsigned int worker_file;  
  
  const char *inputPath;
  const char *outputPath;
  const char *partitionPath;
  unsigned long numSamples;
  
 public:
  Configuration() {
    numReducer = 3;
    numInput = numReducer;    
    
    inputPath = "./Input/Input10000";
    outputPath = "./Output/Output10000";
    partitionPath = "./Partition/Partition10000";
    numSamples = 10000;
  }
  ~Configuration() {}
  const static unsigned int KEY_SIZE = 10;
  const static unsigned int VALUE_SIZE = 90;  
  
  unsigned int getNumReducer() const { return numReducer; }
  unsigned int getNumInput() const { return numInput; }  
  unsigned int getNumMasterFile() const { return master_file; }
  unsigned int getNumWorkerFile() const { return worker_file; }  
  const char *getInputPath() const { return inputPath; }
  const char *getOutputPath() const { return outputPath; }
  const char *getPartitionPath() const { return partitionPath; }
  unsigned int getKeySize() const { return KEY_SIZE; }
  unsigned int getValueSize() const { return VALUE_SIZE; }
  unsigned int getLineSize() const { return KEY_SIZE + VALUE_SIZE; }
  unsigned long getNumSamples() const { return numSamples; }  
};

#endif
