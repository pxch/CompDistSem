#ifndef CLASS_DIST_VEC_H_
#define CLASS_DIST_VEC_H_

#include "redsvd.h"
#include "types.h"
#include "utils.h"
#include <string>

namespace DistVec {
    class DistVec {
    public:
        DistVec (int d);
        ~DistVec ();

        void LoadWord (std::string path);
        void LoadPhrase (std::string path, std::string label);

        void LoadTrainList (std::string path);
        void SetOutputDir (std::string dir);

        void CompTransMat ();
        void CompDenseWordVec ();
        void CompDensePhraseVec ();

        double CosDist (std::string word1, std::string word2);

        void TrainPLF (std::string label);
    private:
        int dim;
        str_map word_vocab, phrase_vocab;
        smat_t word_pmi, phrase_pmi;

        str_vec train_list[5];

        dmat_t trans_mat, word_dmat, phrase_dmat;

        dmat_t arg_space, phrase_space;
        dmat_t mod_mat;
        vec_t mod_vec;

        std::string output_dir;

        void PrepTrainMat (std::string train_word, int loc);
        void RidgeRegression (double lambda, dmat_t& result, double& dist, double& S_trace);

        void WriteDMat (std::string path, dmat_t& mat);
        void WriteOutputMat (std::string path);
        void WriteWordMat (std::string path);

        idx_t GetWordIdx (std::string word);
    };
}

#endif //CLASS_DIST_VEC_H_

