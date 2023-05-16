#pragma once

#include "Util.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <iostream>
#include <iterator>
#include <span> 
#include <string>
#include <string_view>
#include <unordered_map>

const std::unordered_map<std::string, Command> command_map {
	{"create"     , Command::Create} , {"delete"     , Command::Delete}, 
	{"drop"       , Command::Drop}   , {"foreign_key", Command::Foreign}, 
	{"from"       , Command::From}   , {"insert"     , Command::Insert}, 
	{"primary_key", Command::Primary}, {"select"     , Command::Select},
	{"where"      , Command::Where} 
};

const std::unordered_map<std::string, DataBaseType> type_map {
	{"int", DataBaseType::Integer}, {"float", DataBaseType::Float}, 
};

const std::unordered_map<char, TypeOfJoin> join_map {
	{'l', TypeOfJoin::Left}, {'i', TypeOfJoin::Inner}, {'r', TypeOfJoin::Right}	
};

const std::unordered_map<std::string, Comparator> comp_map {
	{"==", Comparator::Equal}  , {"!=", Comparator::NotEqual},
	{">" , Comparator::Larger} , {">=", Comparator::LargerThanOrEqual},
	{"<" , Comparator::Smaller}, {"<=", Comparator::SmallerThanOrEqual}
};

const std::unordered_map<char, Conjunctor> conj_map {
	{'&', Conjunctor::And}, {'|', Conjunctor::Or}
};

struct Parser {
	std::string_view get_command(std::string_view& sv);
	std::string_view get_bracket_content(std::string_view& sv);

	bool parse_query(const std::string& user_query);
	void parse_bracket(std::string_view& sv);
	void parse_where(std::string_view& sv);
	void parse_conditional(std::string_view sv);

	
	size_t split_string(std::string_view sv,
						std::span<std::string_view> tokens,
						const std::string delimiters);

	std::string  query;
	SQLStatement statement;
	std::array<std::string_view, MAX_PARAMS> clause_chunk;
};
