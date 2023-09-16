#pragma once

#include <stdexcept>
#include <sys/stat.h>

#include <array>
#include <concepts>
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
    Where
};

enum Comparator {
    Equal,
    NotEqual,
    Larger,
    Smaller,
    LargerThanOrEqual,
    SmallerThanOrEqual,
    NullComp
};

enum Conjunctor {
    And, 
    Or,
    NullConj
};

enum TypeOfJoin {
    Left,
    Inner,
    Right
};

/********************************************************************************/
/* Database constants */
/* maximum number of attributes a database row can have */
constexpr size_t MAX_PARAMS   = 128;
constexpr size_t MAX_FOREIGN  = 8;
constexpr size_t MAX_PRIM_KEY = 16;

/********************************************************************************/

constexpr size_t NUMERIC_SIZE = sizeof(int32_t);

struct DatabaseType {    
    DatabaseType() = default;
    DatabaseType(Type t)
	: type{t}, type_size{NUMERIC_SIZE} {};
    
    DatabaseType(Type t, size_t size)
	: type{t}, type_size{size} {};

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
/* AST information, used for parsing where clauses */
size_t left (size_t layer);
size_t right(size_t layer);

struct ASTNode {
    Conjunctor	     conj = Conjunctor::NullConj;
    Comparator	     comp = Comparator::NullComp;
    std::string_view lhs, rhs;
};

void print_ast(const std::array<ASTNode, MAX_PARAMS>& ast, 
	       size_t layer,
	       size_t num_spaces);

/********************************************************************************/
/* Record information */
using RecordLayout = std::vector<DatabaseType>;
using RecordData   = std::variant<int32_t, float, std::string>;
using Record	   = std::vector<RecordData>;

size_t calc_record_size(const RecordLayout& layout);

/********************************************************************************/
/* DISPLAY FUNCTION AND TYPES CAN BE REMOVED IF NOT DEBUGGING*/

template <typename T>
concept Iterable = requires(T t) {
    { t.begin() } -> std::forward_iterator;
    { t.end() }   -> std::forward_iterator;
};

template <Iterable Container>
void print_n_elements(const Container& container, size_t n) {
    /* print n elements from front of container */
    std::cout << "[";
    for (const auto& value : container | std::views::take(n))
        std::cout << value << " ";
    std::cout << "]\n";
}


const std::unordered_map<Command, std::string> swap_command_map {
    {Command::Create , "create"}     , {Command::CreateIndex, "create_index"},
    {Command::Delete , "delete"}     , {Command::Drop       , "drop"},
    {Command::Foreign, "foreign_key"}, {Command::From       , "from"},
    {Command::Insert , "insert"}     , {Command::Primary    , "primary_key"},
    {Command::Select , "select"}     , {Command::Set	    , "set"},
    {Command::Size   , "size"}       , {Command::Update     , "update"},
    {Command::Vacuum , "vacuum"}     , {Command::Where      , "where"}
};


const std::unordered_map<TypeOfJoin, std::string> swap_join_map {
    {TypeOfJoin::Left, "left"}, {TypeOfJoin::Inner, "inner"}, {TypeOfJoin::Right, "right"}	
};

const std::unordered_map<Comparator, std::string> swap_comp_map {
    {Comparator::Equal  , "=="}, {Comparator::NotEqual          , "!="},
    {Comparator::Larger , ">"} , {Comparator::LargerThanOrEqual , ">="},
    {Comparator::Smaller, "<"} , {Comparator::SmallerThanOrEqual, "<="}
};

const std::unordered_map<Conjunctor, char> swap_conj_map {
    {Conjunctor::And, '&'}, {Conjunctor::Or, '|'}
};

/********************************************************************************/

/* We only keep 1 parser running in the program so we allocate all the data 
   we need upfront */
using AttrList    = std::array<std::string_view, MAX_PARAMS>;
using ASTTree     = std::array<ASTNode         , MAX_PARAMS>;
using ForeignData = std::array<std::string_view, MAX_FOREIGN>; 
using PrimKeyList = std::array<std::string_view, MAX_PRIM_KEY>; 
using TableData   = std::array<std::string_view, 2>;

struct SQLStatement {
    Command    command;
    TypeOfJoin join_type;
    
    size_t num_attr    = 0;
    size_t num_primary = 0;
    size_t num_foreign = 0;
    size_t num_set     = 0;
    size_t node_size   = 0;

    TableData table_name;
    TableData join_attr;
    
    ForeignData  foreign_attr;
    ForeignData  foreign_table;
    PrimKeyList  prim_key;
    RecordLayout table_layout;
    AttrList	 table_attr;
    AttrList	 set_attr;
    AttrList	 set_value;
    ASTTree      where_tree;

    friend std::ostream& operator<<(std::ostream&	os, 
				    const SQLStatement& stmt) 
    {
	os << "	Command        : " << swap_command_map.at(stmt.command) << "\n";
	os << "	Table names    : " << stmt.table_name[0] << ", " << stmt.table_name[1] << "\n";
	os << "	Join attributes: " << stmt.join_attr[0]  << ", " << stmt.join_attr[1] << "\n";
	os << "	Join Type      : " << swap_join_map.at(stmt.join_type) << "\n\n";

	os << "	Attributes        : "; print_n_elements(stmt.table_attr   , stmt.num_attr);
	os << "	Primary key       : "; print_n_elements(stmt.prim_key     , stmt.num_primary);
	os << "	Foreign attributes: "; print_n_elements(stmt.foreign_attr , stmt.num_foreign);
	os << "	Foreign tables    : "; print_n_elements(stmt.foreign_table, stmt.num_foreign);
	os << "	Set attributes    : "; print_n_elements(stmt.set_attr     , stmt.num_set);
	os << "	Set values        : "; print_n_elements(stmt.set_value    , stmt.num_set);
	os << "	Database types    : "; print_n_elements(stmt.table_layout , stmt.num_attr);
	
	os << "\nWHERE CLAUSE:\n********\n"; print_ast(stmt.where_tree, 0, 0);
	return os;
    }
};
