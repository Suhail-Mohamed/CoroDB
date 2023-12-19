#pragma once

#include <stdexcept>
#include <sys/stat.h>

#include <array>
#include <concepts>
#include <functional>
#include <iostream>
#include <numeric>
#include <ranges>
#include <string_view>
#include <span>
#include <unordered_map>
#include <vector>
#include <variant>

enum Type {
  Integer,
  Float,
  String,
  NullType
};

enum Command {
  Create,
  CreateIndex,
  Delete,
  Drop,
  Foreign,
  From,
  Insert,
  Primary,
  Select,
  Set,
  Size,
  Update,
  Vacuum,
  Where,
  NullCommand
};

enum TypeOfJoin {
  Left,
  Inner,
  Right,
  NullJoin
};

/********************************************************************************/
/* Database constants */
constexpr size_t MAX_PARAMS   = 128; /* maximum number of attributes a database row can have */
constexpr size_t MAX_FOREIGN  = 3;   /* maximum foreign values a table can have */
constexpr size_t MAX_PRIM_KEY = 5;   /* maximum size of a key, either primary key or index key */

/********************************************************************************/

constexpr size_t NUMERIC_SIZE = sizeof(int32_t);
constexpr size_t MAX_STRING   = 50;

struct DatabaseType {    
  DatabaseType() = default;
  DatabaseType(Type t)
    : type{t}, type_size{NUMERIC_SIZE} {};

  DatabaseType(Type t, size_t size)
    : type{t}, type_size{std::min(MAX_STRING, size)} {};

  friend std::ostream& operator<<(std::ostream& os, 
                                  const DatabaseType& data)
  {
    os << "[Type: ";
    switch (data.type) {
      case Type::Integer: os << "Integer "; break;
      case Type::Float  : os << "Float ";   break;
      case Type::String : os << "String ";  break;
      default: break;
    }

    os << ", Size : " << data.type_size;
    std::cout << "] ";
    return os;
  }

  Type   type      = Type::NullType;
  size_t type_size = 0;
};


/********************************************************************************/
/* Record information */
using RecordLayout = std::vector<DatabaseType>;
using RecordData   = std::variant<int32_t, float, std::string>;
using Record	   = std::vector<RecordData>;

int32_t    calc_record_size(const RecordLayout& layout);
RecordData cast_to(const std::string  attr_value, 
                   const DatabaseType db_type); 

/********************************************************************************/
/* AST information, used for parsing where clauses */
using RecordComp = std::function<bool(const RecordData&, const RecordData&)>;
using BoolConj   = std::function<bool(bool, bool)>;

size_t left (size_t layer);
size_t right(size_t layer);

struct ASTNode {
  BoolConj   conj = BoolConj();
  RecordComp comp = RecordComp();

  std::string lhs, rhs;
};

void print_ast(const std::array<ASTNode, MAX_PARAMS>& ast, 
               size_t layer,
               size_t num_spaces);

/********************************************************************************/
/* DISPLAY FUNCTION AND TYPES CAN BE REMOVED IF NOT DEBUGGING*/

template <typename T>
concept Iterable = requires(T t) {
{ t.begin() } -> std::forward_iterator;
{ t.end() }   -> std::forward_iterator;
};

template <Iterable Container>
void print_n_elements(const Container& container, int32_t n) {
  /* print n elements from front of container */
  std::cout << "[";
  for (const auto& value : container | std::views::take(n))
    std::cout << value << ", ";
  std::cout << "]\n";
}


const std::unordered_map<Command, std::string> swap_command_map {
  {Command::Create , "create"}     , {Command::CreateIndex, "create_index"},
  {Command::Delete , "delete"}     , {Command::Drop       , "drop"},
  {Command::Foreign, "foreign_key"}, {Command::From       , "from"},
  {Command::Insert , "insert"}     , {Command::Primary    , "primary_key"},
  {Command::Select , "select"}     , {Command::Set	  , "set"},
  {Command::Size   , "size"}       , {Command::Update     , "update"},
  {Command::Vacuum , "vacuum"}     , {Command::Where      , "where"},
  {Command::NullCommand, "NULL"}
};

const std::unordered_map<TypeOfJoin, std::string> swap_join_map {
  {TypeOfJoin::Left, "left"}, {TypeOfJoin::Inner, "inner"}, {TypeOfJoin::Right, "right"},
  {TypeOfJoin::NullJoin, "NULL"}
};

/********************************************************************************/

/* We only keep 1 parser running in the program so we allocate all the data 
   we need upfront */
using AttrList    = std::array<std::string , MAX_PARAMS>;
using ASTTree     = std::array<ASTNode     , MAX_PARAMS>;
using ForeignData = std::array<std::string , MAX_FOREIGN>; 
using PrimKeyList = std::array<std::string , MAX_PRIM_KEY>; 
using LayoutList  = std::array<DatabaseType, MAX_PARAMS>; 
using TableData   = std::array<std::string , 2>;

struct SQLStatement {
  Command    command   = Command::NullCommand;
  TypeOfJoin join_type = TypeOfJoin::NullJoin;

  int32_t num_attr    = 0;
  int32_t num_primary = 0;
  int32_t num_foreign = 0;
  int32_t num_set     = 0;

  TableData table_name;
  TableData join_attr;

  ForeignData foreign_keys;
  ForeignData foreign_table;
  PrimKeyList prim_key;
  LayoutList  table_layout;
  AttrList    table_attr;
  AttrList    set_attr;
  AttrList    set_value;
  ASTTree     where_tree;

  friend std::ostream& operator<<(std::ostream&	os, 
                                  const SQLStatement& stmt) 
  {
    os << "	Command        : " << swap_command_map.at(stmt.command) << "\n";
    os << "	Table names    : " << stmt.table_name[0] << ", " << stmt.table_name[1] << "\n";
    os << "	Join attributes: " << stmt.join_attr[0]  << ", " << stmt.join_attr[1] << "\n";
    os << "	Join Type      : " << swap_join_map.at(stmt.join_type) << "\n\n";

    os << "	Attributes        : "; print_n_elements(stmt.table_attr   , stmt.num_attr);
    os << "	Primary key       : "; print_n_elements(stmt.prim_key     , stmt.num_primary);
    os << "	Foreign attributes: "; print_n_elements(stmt.foreign_keys , stmt.num_foreign);
    os << "	Foreign tables    : "; print_n_elements(stmt.foreign_table, stmt.num_foreign);
    os << "	Set attributes    : "; print_n_elements(stmt.set_attr     , stmt.num_set);
    os << "	Set values        : "; print_n_elements(stmt.set_value    , stmt.num_set);
    os << "	Database types    : "; print_n_elements(stmt.table_layout , stmt.num_attr);

    os << "\nWHERE CLAUSE:\n********\n"; print_ast(stmt.where_tree, 0, 0);
    return os;
  }

  std::string get_table_name() const 
  { return table_name[0]; }

  std::string get_join_table_name() const 
  { return table_name[1]; }
};
