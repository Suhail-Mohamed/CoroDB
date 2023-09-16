#include "Util.hpp"

size_t left (size_t layer) { return 2*layer + 1; };
size_t right(size_t layer) { return 2*layer + 2; };

/********************************************************************************/

void print_ast(const std::array<ASTNode, MAX_PARAMS>& ast, 
			   size_t layer, 
			   size_t num_spaces)
{
  if (ast[layer].comp == Comparator::NullComp &&
	ast[layer].conj == Conjunctor::NullConj) return;

  print_ast(ast, left(layer), num_spaces + 1);

  std::cout << std::string(num_spaces, '\t');

  if (ast[layer].comp != Comparator::NullComp) {
	std::cout << ast[layer].lhs << "  " 
			  << swap_comp_map.at(ast[layer].comp) << "  " 
			  << ast[layer].rhs << "\n";
  } else 
	std::cout << "Conjunctor: " 
			  << swap_conj_map.at(ast[layer].conj) 
			  << "\n";
  
  print_ast(ast, right(layer), num_spaces + 1); 
}

/********************************************************************************/

size_t calc_record_size(const RecordLayout& layout) {
  size_t r_size = std::accumulate(std::begin(layout), std::end(layout), 0,
								  [](int acc, const auto& db_type) {
									return acc + db_type.type_size;
								  });
  return r_size;
}


