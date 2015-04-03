#ifndef UTILS_H_
#define UTILS_H_

#include "types.h"
#include <algorithm>
#include <string>

namespace DistVec {
    double cosDist (const vec_t& l, const vec_t& r);

    bool hasNan (const vec_t& vec);

    std::vector<std::string> split(const std::string& s, char delim);

    bool existsFile (const std::string& name);

    void ReadDir (std::string path, str_vec& files);

    void ReadVocab (std::string path, str_map& map);
    void ReadPMI (std::string path, smat_t& mat);

    void ReadDMat (std::string path, idx_t row, idx_t col, dmat_t& mat);
}

#endif //UTILS_H_

