#ifndef CLASS_DIST_VEC_H_
#define CLASS_DIST_VEC_H_

#include "types.h"
#include <string>

namespace DistVec {
    class DistVec {
    public:
        DistVec ();
        DistVec (std::string vocab_dir, std::string pmi_dir);
        ~DistVec ();

        void ReadVocabularyMap (std::string path);
        void ReadPMIMatrix (std::string path);

        double CosDist (std::string word1, std::string word2);

    private:
        str_map vocab;
        smat_t pmi;

//        void ReadDir (std::string path, str_vec& files);
    };
}

#endif //CLASS_DIST_VEC_H_

