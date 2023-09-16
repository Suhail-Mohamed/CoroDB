#pragma once

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
#include <ranges>

#include "Util.hpp"

const std::unordered_map<std::string, Command> command_map {
  {"create"     , Command::Create} , {"create_index", Command::CreateIndex},
  {"delete"     , Command::Delete} , {"drop"        , Command::Drop}, 
  {"foreign_key", Command::Foreign}, {"from"        , Command::From}, 
  {"insert"     , Command::Insert} , {"primary_key" , Command::Primary}, 
  {"select"     , Command::Select} , {"set"         , Command::Set}, 
  {"size"       , Command::Size}   , {"update"      , Command::Update}, 
  {"vacuum"     , Command::Vacuum} , {"where"       , Command::Where} 
};

const std::unordered_map<std::string, Type> type_map {
  {"int", Type::Integer}, {"float", Type::Float}, 
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

/********************************************************************************/

struct Parser {
  Command          get_command        (std::string_view& sv);
  std::string_view get_bracket_content(std::string_view& sv);

  bool is_valid_bracket(const std::string_view sv);

  void parse_query      (const std::string& user_query);
  void parse_bracket    (const Command    command,
                         std::string_view br_content, 
                         std::string_view extra_content = "");
  void parse_create     (std::string_view sv_br, 
                         std::string_view sv_extra);
  void parse_from       (std::string_view sv);
  void parse_where      (const std::string_view sv, 
                         const size_t           layer);
  void parse_conditional(const std::string_view sv, 
                         const size_t           layer);

  size_t split_string(std::string_view sv,
                      std::span<std::string_view> tokens,
                      const char delimiter);

  std::string  query;
  SQLStatement statement;
};

