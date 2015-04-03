#ifndef COMMON_TYPES_H_
#define COMMON_TYPES_H_

#include <Eigen/Dense>
#include <Eigen/Sparse>
#include <vector>
#include <unordered_map>

typedef int idx_t;

typedef Eigen::SparseMatrix<double> smat_t;
typedef Eigen::MatrixXd dmat_t;

typedef Eigen::Triplet<double> triplet_t;
typedef std::vector<triplet_t> tri_vec;

typedef Eigen::VectorXd vec_t;
typedef std::vector<double> vecd_t;
typedef std::vector<idx_t> veci_t;
typedef std::vector<std::string> str_vec;

typedef std::unordered_map<std::string, idx_t> str_map;
typedef str_map::const_iterator str_map_citer;
typedef str_map::iterator str_map_iter;

#endif //COMMON_TYPES_H_

