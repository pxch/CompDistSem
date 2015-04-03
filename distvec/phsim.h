#ifndef CLASS_PH_SIM_H_
#define CLASS_PH_SIM_H_

#include "types.h"
#include "utils.h"
#include <string>

namespace DistVec {
    class PhSim {
    public:
        PhSim (int d, int v);
        ~PhSim ();

        void LoadVocab (std::string path);
        void LoadWordMat (std::string path);
        void SetModMatDir (std::string dir);

        void CompRules (std::string in_path, std::string out_prefix);

        vec_t CompPhVec (std::string ph);
        vec_t CompPhVec (std::vector<std::string> elems);

        dmat_t& GetWordDMat () {
            return this->word_dmat;
        }

    private:
        idx_t dim;
        idx_t vocab_size;

        str_map word_vocab;
        dmat_t word_dmat;

        std::string mod_mat_dir;

        void LoadModMat (std::string path, dmat_t& mat);
        idx_t GetWordIdx (std::string word);
    };
}

#endif //CLASS_PH_SIM_H_

