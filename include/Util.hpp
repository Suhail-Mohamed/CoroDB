#pragma once

#include <array>
#include <iostream>
#include <string_view>
#include <unordered_map>

enum DataBaseType {
    Integer,
    Float,
    String
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

size_t left  (size_t layer);
size_t right (size_t layer);

struct ASTNode {
    Conjunctor	     conj = Conjunctor::NullConj;
    Comparator	     comp = Comparator::NullComp;
    std::string_view lhs, rhs;
};

/********************************************************************************/

const uint32_t MAX_PARAMS = 128;

template <typename T>
void print_array(const std::array<T, MAX_PARAMS>& arr, 
		 size_t num_elements) 
{
    std::cout << "{";
    for (size_t i = 0; i < num_elements; ++i)
        std::cout << arr[i] << ", ";
    std::cout << "}\n";
}


void print_ast(const std::array<ASTNode, MAX_PARAMS>& ast, 
	       size_t layer,
	       size_t num_spaces);

struct SQLStatement {
    Command    command;
    TypeOfJoin join_type;
    
    size_t num_attr    = 0;
    size_t num_primary = 0;
    size_t num_foreign = 0;
    size_t num_set     = 0;
    size_t node_size   = 0;

    std::array<std::string_view, 2>          table_name;
    std::array<std::string_view, 2>	     join_attr;
    std::array<DataBaseType    , MAX_PARAMS> db_type;
    std::array<size_t          , MAX_PARAMS> str_size; 
    std::array<std::string_view, MAX_PARAMS> attr;
    std::array<std::string_view, MAX_PARAMS> prim_key;
    std::array<std::string_view, MAX_PARAMS> foreign_attr;
    std::array<std::string_view, MAX_PARAMS> foreign_table;
    std::array<std::string_view, MAX_PARAMS> set_attr;
    std::array<std::string_view, MAX_PARAMS> set_value;
    std::array<ASTNode         , MAX_PARAMS> where_tree;

    friend std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt) {
	os << "	Command        : " << swap_command_map.at(stmt.command) << "\n";
	os << "	Table names    : " << stmt.table_name[0] << ", " << stmt.table_name[1] << "\n";
	os << "	Join attributes: " << stmt.join_attr[0]  << ", " << stmt.join_attr[1] << "\n";
	os << "	Join Type      : " << swap_join_map.at(stmt.join_type) << "\n\n";

	os << "	Attributes        : "; print_array(stmt.attr         , stmt.num_attr);
	os << "	String sizes      : "; print_array(stmt.str_size     , stmt.num_attr);
	os << "	Primary key       : "; print_array(stmt.prim_key     , stmt.num_primary);
	os << "	Foreign attributes: "; print_array(stmt.foreign_attr , stmt.num_foreign);
	os << "	Foreign tables    : "; print_array(stmt.foreign_table, stmt.num_foreign);
    	os << "	Set attributes    : "; print_array(stmt.set_attr     , stmt.num_set);
	os << "	Set values        : "; print_array(stmt.set_value    , stmt.num_set);
	os << "	Database types    : "; print_array(stmt.db_type      , stmt.num_attr);
	
	os << "\n_____________________WHERE CLAUSE_____________________\n"; print_ast(stmt.where_tree, 0, 0);

	return os;
    }
};




