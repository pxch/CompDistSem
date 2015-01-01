#ifndef UTILS_READ_PMI_MATRIX_H_
#define UTILS_READ_PMI_MATRIX_H_

#include "../common/types.h"
#include <string>
#include <omp.h>

namespace DistVec{
    void ReadPMIMatrix (const std::string filename, smat_t& mat) {
        std::cout << "\tLoading PMI matrix from " << filename << std::endl;

        double start_time = omp_get_wtime();

        std::ifstream in(filename.c_str());
        if (!in.is_open()) {
            std::cerr << "Error in opening file: " << filename << std::endl;
            exit(-1);
        }

        idx_t row_cnt = 0, col_cnt = 0, nnz_cnt = 0;

        std::string line;
        size_t pos1, pos2;
        idx_t row_idx, col_idx;
        double val;

        std::vector<triplet_t> tri_list;

        while(std::getline(in, line)) {
            line = line.substr(1, line.length() - 2);
            pos1 = line.find("\t");
            pos2 = line.find(",");

            row_idx = atoi(line.substr(0, pos1));
            col_idx = atoi(line.substr(pos1 + 1, pos2 - pos1 - 1));
            val = atof(line.substr(pos2 + 1, line.length() - pos2 - 1));

            if (row_idx + 1 > row_cnt) {
                row_cnt = row_idx + 1;
            }
            if (col_idx + 1 > col_cnt) {
                col_cnt = col_idx + 1;
            }

            tri_list.push_back(triplet_t(row_idx, col_idx, 1));
            ++nnz_cnt;
        }

        std::cout << "\t\trows:\t" << row_cnt << std::endl;
        std::cout << "\t\tcols:\t" << col_cnt << std::endl;
        std::cout << "\t\tnnzs:\t" << nnz_cnt << std::endl;

        mat.resize(row_cnt, col_cnt);
        mat.reserve(nnz_cnt);

        mat.setFromTriplets(tri_list.begin(), tri_list.end());

        std::cout << "\tDone. Elapsed time = ";
        std::cout << omp_get_wtime() - start_time << "s" << std::endl;
    }
}

#endif //UTILS_READ_PMI_MATRIX_H_

