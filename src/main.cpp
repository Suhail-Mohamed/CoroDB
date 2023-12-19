#include <iostream>
#include "DatabaseManager.hpp"

int main() {
  DatabaseManager& db_manager = DatabaseManager::get_instance();
  db_manager.start_cmdline();
}
