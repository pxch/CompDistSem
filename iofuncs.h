#ifndef UTILS_IO_FUNCS_H_
#define UTILS_IO_FUNCS_H_

#include "types.h"
#include <string>

namespace DistVec {
    void ReadPMIMatrix (std::string path, smat_t& mat);

    void ReadVocabularyMap (std::string path, str_map& map);

    void ReadDir (std::string path, str_vec& files);

    double CosDist (smat_t& mat, str_map& map, std::string word1, std::string word2);
}

#endif //UTILS_IO_FUNCS_H_

