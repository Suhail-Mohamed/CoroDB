#include "Parser.hpp"
#include "Util.hpp"
#include <stdexcept>
#include <string_view>

Command Parser::get_command(std::string_view& sv) {
	const size_t br_pos     = sv.find('(');
	auto         sv_command = sv.substr(0, br_pos);
	const auto   srch       = std::string(sv_command);

	if (!command_map.count(srch))
		throw std::runtime_error("Invalid SQLCommand:" 
								 "potential bracket error\n");
	sv = sv.substr(br_pos);
	return command_map.at(srch);
}

/********************************************************************************/

std::string_view Parser::get_bracket_content(std::string_view& sv) {
	if (sv[0] != '(') return sv;

	size_t  end     = 1;
	int32_t balance = 1;
	
	while (balance && end < sv.size()) {
		balance += (sv[end] == '(');
		balance -= (sv[end] == ')');
		++end;
	}

	auto br_content = sv.substr(1, end - 2);
	sv = sv.substr(end);

	return br_content;
}

/********************************************************************************/

bool Parser::is_valid_bracket(std::string_view sv) {
	int32_t balance = 0;

	for (size_t i = 0; i < sv.size(); ++i) {
		balance += (sv[i] == '(');
		balance -= (sv[i] == ')');
	}

	return (balance == 0);
}

/********************************************************************************/

void Parser::parse_query(const std::string& user_query) {
	std::string_view sv_query;
	Command			 command;
	
	query = user_query;
	
	for (char& c : query) 
		c = std::tolower(c);
	
	query.erase(std::remove_if(std::begin(query), std::end(query), 
				[](unsigned char c) { return std::isspace(c); }), 
				std::end(query));
	
	if (query == "exit") {
		std::cout << "EXITING PROGRAM\n";
		exit(1);
	}
	
	sv_query          = query;
	command           = get_command(sv_query);
	statement.command = command;
	
	while (!sv_query.empty()) {
		if (command == Command::Create||
			command == Command::CreateIndex||
			command == Command::Insert) {
			auto table_name = get_bracket_content(sv_query);
			auto br_content = get_bracket_content(sv_query);
			parse_bracket(command, br_content, table_name);
		} else
			parse_bracket(command,
						  get_bracket_content(sv_query));
		
		if (!sv_query.empty())
			command = get_command(sv_query);
	}

	std::cout << "Statement Data:\n" << statement << "\n";
}

/********************************************************************************/

void Parser::parse_bracket(Command		    command,
						   std::string_view br_content, 
						   std::string_view extra_content) 
{
	switch (command) {
		case Command::Create:
			parse_create(br_content, extra_content);	
			break;
		case Command::Primary:
			statement.num_primary = 
				split_string(br_content, statement.prim_key, ",");
			break;
		case Command::Foreign: {
			statement.num_foreign = 
				split_string(br_content, statement.foreign_attr, ",");
			
			for (size_t i = 0; i < statement.num_foreign; ++i) {
				const auto   sv    = statement.foreign_attr[i];
				const size_t colon = sv.find(':');
				
				statement.foreign_attr[i]  = sv.substr(0, colon);
				statement.foreign_table[i] = sv.substr(colon + 1);
			}
			break;
		}
		case Command::Vacuum:
		case Command::Delete:
		case Command::Drop: 
		case Command::Update:
			statement.table_name[0] = br_content;
			break;
		case Command::From:
			parse_from(br_content);
			break;
		case Command::CreateIndex:
		case Command::Insert:
			statement.table_name[0] = extra_content; 
			statement.num_attr = 
				split_string(br_content, statement.attr, ",");
			break;
		case Command::Select:
			statement.num_attr = 
				split_string(br_content, statement.attr, ",");			
			break;
		case Command::Set: {
			statement.num_set = 
					split_string(br_content, statement.set_attr, ",");

			for (size_t i = 0; i < statement.num_set; ++i) {
				const auto   sv = statement.set_attr[i];
				const size_t eq = sv.find('=');
				
				statement.set_attr[i]  = sv.substr(0, eq);
				statement.set_value[i] = sv.substr(eq + 1);
			}
			break;
		}
		case Command::Size:
			statement.node_size = 
				stoi(std::string(br_content));
			break;
		case Command::Where:
			if (!is_valid_bracket(br_content)) 
				throw std::runtime_error("Error parsing 'where' command:" 
										 "Invalid bracketing\n");
			parse_where(br_content, 0);
			break;
		default: throw std::runtime_error("Error: Invalid command used\n");
	}
}


/********************************************************************************/

void Parser::parse_create(std::string_view sv_br, 
						  std::string_view sv_extra) 
{
	if (sv_extra.empty())
		throw std::runtime_error("Error parsing 'create' command:" 
								 "error in create string format");

	statement.table_name[0] = sv_extra; 
	statement.num_attr      = split_string(sv_br, statement.attr, ",");

	for (size_t i = 0; i < statement.num_attr; ++i) {
		const auto sv_attr = statement.attr[i];
		const auto colon   = sv_attr.find(':');
		const auto type    = std::string(sv_attr.substr(colon + 1));
		statement.attr[i]  = sv_attr.substr(0, colon);
		
		if (!type_map.count(type)) {
			const size_t underscore = type.find('_');
			
			if (underscore == std::string::npos)
				throw std::runtime_error("Error parsing 'create' command:" 
										 "error in string type format\n");

			const auto str_size   = type.substr(underscore + 1);
			statement.str_size[i] = std::stoi(str_size);
			statement.db_type[i]  = DataBaseType::String;
		}
	}
}

/********************************************************************************/

void Parser::parse_from(std::string_view sv) {
	const size_t colon = sv.find(':');
	if (colon == std::string_view::npos) {
		statement.table_name[0] = sv;
		return;
	}
	
	const size_t colon_2 = sv.find_last_of(':');
	if (colon_2 == std::string_view::npos)
		throw std::runtime_error("Error parsing 'from' command:" 
								 "second colon expected\n");

	const auto srch = char(sv[0]);
	if (!join_map.count(srch))
		throw std::runtime_error("Error parsing 'from' command:" 
								 "invalid join type\n");

	statement.join_type = join_map.at(srch);

	const auto join_tables = sv.substr(colon + 1, colon_2 - colon - 1);
	const auto join_attr   = sv.substr(colon_2 + 1);

	split_string(join_tables, statement.table_name, "&");
	split_string(join_attr  , statement.join_attr , "=");
}

/********************************************************************************/

void Parser::parse_where(std::string_view sv, size_t layer) {
	if (layer > MAX_PARAMS) 
		throw std::runtime_error("Error parsing 'where' command:" 
								 "where clause is too long\n");
	int32_t balance = 0;
	size_t  idx     = 0;

	for (; idx < sv.size(); ++idx) {
		balance += (sv[idx] == '(');
		balance -= (sv[idx] == ')');
		
		if (conj_map.count(sv[idx]) && balance == 0) {
			statement.where_tree[layer].conj = 
				conj_map.find(sv[idx])->second;
			break;
		}
	}

	if (statement.where_tree[layer].conj !=
		Conjunctor::NullConj) {
		auto lhs = sv.substr(0, idx);
		auto rhs = sv.substr(idx + 1);
		parse_where(get_bracket_content(lhs), left(layer));
		parse_where(get_bracket_content(rhs), right(layer));
	} else
		parse_conditional(sv, layer);
}

/********************************************************************************/

void Parser::parse_conditional(std::string_view sv, size_t layer) {
	Comparator comp;
	size_t	   comp_pos;
	size_t     size_key;

	for (auto [key, value] : comp_map)
		if (comp_pos = sv.find(key); 
			comp_pos != std::string_view::npos) {
			comp     = value;
		    size_key = key.size();
			break;
		}
		
	statement.where_tree[layer].lhs  = sv.substr(0, comp_pos);
	statement.where_tree[layer].rhs  = sv.substr(comp_pos + size_key);
	statement.where_tree[layer].comp = comp;
}

/********************************************************************************/

size_t Parser::split_string(std::string_view sv,
							std::span<std::string_view> tokens,
							const std::string delimiters) 
{
	size_t idx = 0, end = 0;
	
	auto search_delims = [&delimiters, &sv]() -> size_t {
		size_t d_pos = sv.size();

		for (char delim : delimiters)
			if (auto pos = sv.find(delim);
				pos != std::string_view::npos)
				d_pos = std::min(d_pos, pos);

		return d_pos;
	};
	
	end = search_delims();
	
	while (end < sv.size()) {
		tokens[idx++] = sv.substr(0, end);
		sv  = sv.substr(end + 1); 
		end = search_delims();
	}
	
	tokens[idx] = sv;
	return idx + 1; 
}
