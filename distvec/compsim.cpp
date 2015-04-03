#include "phsim.h"
#include "types.h"
#include "utils.h"
#include <iostream>

using namespace std;

int main (int argc, char *argv[]) {
    if (argc != 8) {
        cerr << "Usage: ./test1 dim vocab_size vocab_path word_dmat_path"
            << " mod_mat_dir rule_path output_prefix" << endl;
        exit(1);
    }

    idx_t dim = atoi(argv[1]);
    idx_t vocab_size = atoi(argv[2]);
    string vocab_path = argv[3];
    string word_dmat_path = argv[4];
    string mod_mat_dir = argv[5];
    if (mod_mat_dir[mod_mat_dir.length()-1] != '/') {
        mod_mat_dir += "/";
    }
    string rule_path = argv[6];
    string output_prefix = argv[7];

    DistVec::PhSim phSim(dim, vocab_size);


    phSim.LoadVocab(vocab_path);
    phSim.LoadWordMat(word_dmat_path);

    phSim.SetModMatDir(mod_mat_dir);

    phSim.CompRules(rule_path, output_prefix);

    /*
    string ex1 = "#VP# stand-vb dobj-#1# on-in pobj-#2# leg-nn #2# #1#";
    string ex2 = "#VP# approach-vb nsubj-#1# dog-nn #1# dobj-#2# on-in pobj-#3# pebbly-jj amod-#4# beach-nn #4# #3# #2#";
    string ex3 = "#FAIL#";
    string ex4 = "#VP# look-vb";
    string ex5 = "#SINGLETON# carrot-nn";
    string ex6 = "#NP# wooded-jj amod-#1# area-nn #1#";

    vec_t vec1 = phSim.CompPhVec(ex1);
    cout << vec1.transpose() << endl;

    vec_t vec2 = phSim.CompPhVec(ex2);
    cout << vec2.transpose() << endl;

    vec_t vec3 = phSim.CompPhVec(ex3);
    cout << vec3.transpose() << endl;

    vec_t vec4 = phSim.CompPhVec(ex4);
    cout << vec4.transpose() << endl;

    vec_t vec5 = phSim.CompPhVec(ex5);
    cout << vec5.transpose() << endl;

    vec_t vec6 = phSim.CompPhVec(ex6);
    cout << vec6.transpose() << endl;
    */

    return 0;
}

