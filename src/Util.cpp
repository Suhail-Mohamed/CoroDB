#include "Util.hpp"

size_t left (size_t layer) { return 2*layer + 1; };
size_t right(size_t layer) { return 2*layer + 2; };

/********************************************************************************/

int32_t calc_record_size(const RecordLayout& layout) {
  int32_t r_size = std::accumulate(std::begin(layout), std::end(layout), 0,
                                  [](int acc, const auto& db_type) {
                                    return acc + db_type.type_size;
                                  });
  return r_size;
}

/********************************************************************************/

RecordData cast_to(const std::string  attr_value, 
                   const DatabaseType db_type) 
{
  switch (db_type.type) {
    case Type::String : return attr_value; break;
    case Type::Integer: return std::stoi(attr_value); break;
    case Type::Float  : return std::stof(attr_value); break;
    default: throw std::runtime_error("Error: Invalid DataType cannot cast");
  }
}

/********************************************************************************/
/* debug functions */
std::string get_comp(const RecordComp& rec_comp) {
  if (rec_comp.target_type() == typeid(std::equal_to<RecordData>))
    return "==";
  else if (rec_comp.target_type() == typeid(std::not_equal_to<RecordData>))
    return "!=";
  else if (rec_comp.target_type() == typeid(std::less<RecordData>))
    return "<";
  else if (rec_comp.target_type() == typeid(std::less_equal<RecordData>))
    return "<=";
  else if (rec_comp.target_type() == typeid(std::greater<RecordData>))
    return ">";
  else if (rec_comp.target_type() == typeid(std::greater_equal<RecordData>))
    return ">=";
  else 
    return "UNKNOWN";
}

std::string get_conj(const BoolConj& bool_conj) {
  if (bool_conj.target_type() == typeid(std::logical_and<>))
    return "&";
  else if (bool_conj.target_type() == typeid(std::logical_or<>))
    return "|";
  else
    return "UNKNOWN";
}

void print_ast(const std::array<ASTNode, MAX_PARAMS>& ast, 
               size_t layer, 
               size_t num_spaces)
{
  if (!ast[layer].comp && !ast[layer].conj) return;

  print_ast(ast, left(layer), num_spaces + 1);

  std::cout << std::string(num_spaces, '\t');

  if (ast[layer].comp)
    std::cout << ast[layer].lhs << "  " 
              << get_comp(ast[layer].comp) << "  " 
              << ast[layer].rhs << "\n";
  else 
    std::cout << "Conjunctor: " 
              << get_conj(ast[layer].conj) 
              << "\n";

  print_ast(ast, right(layer), num_spaces + 1); 
}



