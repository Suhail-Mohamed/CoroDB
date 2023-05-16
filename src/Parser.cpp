#include "Parser.hpp"
#include "Util.hpp"

std::string_view Parser::get_command(std::string_view& sv) {
	const size_t br_pos  = sv.find('(');
	auto         command = sv.substr(0, br_pos);
	
	sv = sv.substr(br_pos);
	return command;
}

std::string_view Parser::get_bracket_content(std::string_view& sv) {
	size_t br_pos     = sv.find(')');
	auto   br_content = sv.substr(1, br_pos - 1);
	
	sv = sv.substr(br_pos + 1);
	return br_content;
}

bool Parser::parse_query(const std::string& user_query) {
	std::string_view sv_query, command;
	
	query = user_query;
	
	std::transform(std::begin(query), std::end(query), std::begin(query), 
				   [](unsigned char c) { return std::tolower(c); });
	
	query.erase(std::remove_if(std::begin(query), std::end(query), 
				[](unsigned char c) { return std::isspace(c); }), 
				std::end(query));
	
	if (query == "exit") {
		std::cout << "EXITING PROGRAM\n";
		exit(1);
	}
	
	sv_query = query;
	command  = get_command(sv_query);
	while (!sv_query.empty()) {
		const auto srch = std::string(command);
		if (!command_map.count(srch))
			throw std::runtime_error("Invalid SQLCommand: potential bracket error\n");

		statement.command = command_map.at(std::string(command));
		parse_bracket(sv_query);
		
		if (!sv_query.empty())
			command = get_command(sv_query);
	}

	std::cout << statement << "\n";
	return true;
}

void Parser::parse_bracket(std::string_view& sv) {
	/* Making the assumption where clauses are always at the end of a query */
	std::string_view br_content = (statement.command == Command::Where) ?
		sv.substr(1, sv.size() - 2) : get_bracket_content(sv);

	switch (statement.command) {
		case Command::Create: {
			statement.table_name[0] = br_content;
			br_content              = get_bracket_content(sv);
			statement.num_attr      = split_string(br_content, statement.attr, ",");

			for (size_t i = 0; i < statement.num_attr; ++i) {
				const auto sv_attr = statement.attr[i];
				const auto colon   = sv_attr.find(':');
				const auto type    = std::string(sv_attr.substr(colon + 1));
				statement.attr[i]  = sv_attr.substr(0, colon);
				
				if (!type_map.count(type)) {
					const size_t underscore = type.find('_');
					if (underscore == std::string::npos)
						throw std::runtime_error("Error parsing 'create' command: error in string type format\n");
		
					const auto str_size   = type.substr(underscore + 1);
					statement.str_size[i] = std::stoi(str_size);
					statement.db_type[i]  = DataBaseType::String;
				}
			}

			break;
		}
		case Command::Primary:
			statement.num_primary = split_string(br_content, statement.prim_key, ",");
			break;
		case Command::Foreign: {
			statement.num_foreign = split_string(br_content, statement.foreign_attr, ",");
			
			for (size_t i = 0; i < statement.num_foreign; ++i) {
				const auto   str   = statement.foreign_attr[i];
				const size_t colon = str.find(':');
				
				statement.foreign_attr[i]  = str.substr(0, colon);
				statement.foreign_table[i] = str.substr(colon + 1);
			}
			break;
		}
		case Command::Delete:
		case Command::Drop: 
		case Command::Update:
			statement.table_name[0] = br_content;
			break;
		case Command::From: {
			const size_t colon = br_content.find(':');
			if (colon == std::string_view::npos) {
				statement.table_name[0] = br_content;
				break;
			}
			
			const size_t colon_2 = br_content.find_last_of(':');
			if (colon_2 == std::string_view::npos)
				throw std::runtime_error("Error parsing 'from' command: second colon expected\n");

			const auto srch = char(br_content[0]);
			if (!join_map.count(srch))
				throw std::runtime_error("Error parsing 'from' command: invalid join type\n");

			statement.join_type = join_map.at(srch);

			const auto join_tables  = br_content.substr(colon + 1, colon_2 - 1);
			const auto join_attr    = br_content.substr(colon_2 + 1);
			split_string(join_tables, statement.table_name, "&");
			split_string(join_attr  , statement.attr, "=");
			break;
		}
		case Command::Insert: {
			statement.table_name[0] = br_content;
		    br_content              = get_bracket_content(sv);
			statement.num_attr      = split_string(br_content, statement.attr, ",");
			break;
		}
		case Command::Select:
			statement.num_attr = split_string(br_content, statement.attr, ",");			
			break;
		case Command::Where:
			parse_where(br_content);
			break;
		default: throw std::runtime_error("Error: Invalid command used\n");
	}
}

void Parser::parse_where(std::string_view& sv) {
	std::array<std::string_view, MAX_PARAMS> tokens;
	
	print_array(tokens, split_string(sv, tokens, "&|"));
}

void Parser::parse_conditional(std::string_view sv)  {
	if (sv.empty())   return;
	if (sv[0] == '(') parse_conditional(get_bracket_content(sv));

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
	
	statement.cond[statement.num_cond].lhs = sv.substr(0, comp_pos);
	statement.cond[statement.num_cond].rhs = sv.substr(comp_pos + size_key);
	statement.cond[statement.num_cond++].comparator = comp; 
}


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
		sv   = sv.substr(end + 1); 
		end  = search_delims();
	}
	
	tokens[idx] = sv;
	return idx + 1; 
}
