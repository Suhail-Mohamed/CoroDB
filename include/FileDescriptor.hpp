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
  };

  FileDescriptor(FileDescriptor&& other) 
    : fd {std::exchange(other.fd, -1)} {}
  
  FileDescriptor& operator=(FileDescriptor&& other) {
    if (fd > 0 && close(fd) == -1) std::cerr << "Error: Cannot close file\n";

    fd = std::exchange(other.fd, -1);
    return *this;
  }

  ~FileDescriptor() { 
    if (fd > 0 && close(fd) == -1)
      std::cerr << "Error: Cannot close file dtor()\n"; 
  }
  
  int32_t get_file_size() {
    struct stat st;
    if (fstat(fd, &st) == -1)
      throw std::runtime_error("Error: Cannot get file size");
    
    return st.st_size;
  }

  ssize_t file_read(void* buffer, size_t size) {
    ssize_t bytes_read = read(fd, buffer, size);
    if (bytes_read == -1)
      throw std::runtime_error("Error: Failed to read from file");
    
    return bytes_read;
  }
 
  ssize_t file_write(const void* buffer, size_t size) {
    ssize_t bytes_written = write(fd, buffer, size);
    if (bytes_written == -1)
      throw std::runtime_error("Error: Failed to write to file");
    
    return bytes_written;
  }

  int32_t fd = -1;
};
