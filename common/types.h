#ifndef COMMON_TYPES_H_
#define COMMON_TYPES_H_

#include <../lib/Eigen/Eigen>

typedef int idx_t;

typedef Eigen::SparseMatrix<double> smat_t;
typedef Eigen::MatrixXd dmat_t;

typedef Eigen::Triplet<double> triplet_t;
typedef std::vector<triplet_t> tri_vec;

typedef Eigen:VectorXd vec_t;
typedef std::vector<double> vecd_t;
typedef std::vector<idx_t> veci_t;

#endif //COMMON_TYPES_H_

