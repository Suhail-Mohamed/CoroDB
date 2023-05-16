#pragma once

#include <array>
#include <iostream>
#include <string_view>


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
    SmallerThanOrEqual
};

enum Conjunctor {
    NullConjunctor,
    And, 
    Or
};

enum TypeOfJoin {
    Left,
    Inner,
    Right
};

struct Conditional {
    std::string_view lhs;
    std::string_view rhs;
    Comparator       comparator;
    
    friend std::ostream& operator<<(std::ostream& os, const Conditional& cond) {
	os << "[" << cond.lhs << ", " << cond.comparator << ", " << cond.rhs << "]\n";
	return os;
    }
};

const uint32_t MAX_PARAMS = 128;

template <typename T>
void print_array(const std::array<T, MAX_PARAMS>& arr, 
		 size_t num_elements) 
{
    std::cout << "{";
    for (size_t i = 0; i < num_elements; ++i)
        std::cout << arr[i] << " ";
    std::cout << "}\n";
}

struct SQLStatement {
    Command    command;
    TypeOfJoin join_type;
    
    size_t num_attr    = 0;
    size_t num_cond    = 0;
    size_t num_conj    = 0;
    size_t num_primary = 0;
    size_t num_foreign = 0;

    std::array<std::string_view, 2>          table_name;
    std::array<std::string_view, 2>	     join_attr; 
    std::array<std::string_view, MAX_PARAMS> attr;
    std::array<size_t          , MAX_PARAMS> str_size;
    std::array<std::string_view, MAX_PARAMS> prim_key;
    std::array<std::string_view, MAX_PARAMS> foreign_attr;
    std::array<std::string_view, MAX_PARAMS> foreign_table;
    std::array<DataBaseType    , MAX_PARAMS> db_type;
    std::array<Conditional     , MAX_PARAMS> cond;
    std::array<Conjunctor      , MAX_PARAMS> conj;

    friend std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt) {
	os << "	Command        : " << stmt.command << "\n";
	os << "	Table names    : " << stmt.table_name[0] << ", " << stmt.table_name[1] << "\n";
	os << "	Join attributes: " << stmt.join_attr[0]  << ", " << stmt.join_attr[1] << "\n\n";

	os << "	Attributes        : "; print_array(stmt.attr, stmt.num_attr);
	os << "	String sizes      : "; print_array(stmt.str_size, stmt.num_attr);
	os << "	Primary key       : "; print_array(stmt.prim_key, stmt.num_primary);
	os << "	Foreign attributes: "; print_array(stmt.foreign_attr, stmt.num_foreign);
	os << "	Foreign tables    : "; print_array(stmt.foreign_table, stmt.num_foreign);
	os << "	Database types    : "; print_array(stmt.db_type, stmt.num_attr);
	os << "	Conditions        : "; print_array(stmt.cond, stmt.num_cond);
	os << "	Conjuncts         : "; print_array(stmt.conj, stmt.num_conj);
	
	return os;
    }
};




