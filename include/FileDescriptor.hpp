#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <utility>

enum class OpenMode {
  Create, Default
}; 

struct FileDescriptor {
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  
  FileDescriptor(const std::string path, 
                 const OpenMode open_mode = OpenMode::Default) 
  {
    fd = (open_mode == OpenMode::Default) ? open(path.c_str(), O_RDWR) : 
                                            open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd == -1)
      throw std::runtime_error("Error: Cannot open file, file may not exist");
    
    struct stat st;
    if (fstat(fd, &st) == -1)
      throw std::runtime_error("Error: Cannot get file size");
    file_size = st.st_size;
  };

  FileDescriptor(FileDescriptor&& other) 
    : fd       {std::exchange(other.fd, -1)}, 
      file_size{std::exchange(other.file_size, 0)} 
  {}
  
  FileDescriptor& operator=(FileDescriptor&& other) {
    if (close(fd) == -1) std::cerr << "Error: Cannot close file\n";

    fd        = std::exchange(other.fd, -1);;
    file_size = std::exchange(other.file_size, 0); 
    return *this;
  }

  ~FileDescriptor() { 
    if (fd != -1 && close(fd) == -1)
      std::cerr << "Error: Cannot close file dtor()\n"; 
  };
 
  int32_t fd        = -1;
  int32_t file_size =  0;
};
