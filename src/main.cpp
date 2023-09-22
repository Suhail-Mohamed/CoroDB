#include <iostream>
#include "Parser.hpp"

int main() {
  Parser parser;
  
  for (std::string line; std::cout << "CoroDB> " && std::getline(std::cin, line);) {
    if (!line.empty()) {
      parser.parse_query(line);
    }
  }
}
