#include "types.h"
#include "iofuncs.h"
#include "distvec.h"
#include <iostream>

using namespace std;

int main (int argc, char *argv[]) {
    if (argc != 3) {
        cerr << "Usage: ./test path_to_vocabulary path_to_pmi" << endl;
        exit(1);
    }

    string vocab_dir = argv[1];
//    str_map vocab;
//    DistVec::ReadVocabularyMap(vocab_dir, vocab);

    string pmi_dir = argv[2];
//    smat_t pmi;
//    DistVec::ReadPMIMatrix(pmi_dir, pmi);

    DistVec::DistVec distVec(vocab_dir, pmi_dir);

    string word1, word2;
    while (1) {
        cout << "Input first word, or q to quit:" << endl;
        cin >> word1;
        if (word1 == "q") {
            break;
        }
        cout << "Input second word, or q to quit:" << endl;
        cin >> word2;
        if (word2 == "q") {
            break;
        }
        cout << "Similarity between " << word1 << " and " << word2 << " is: ";
//        double dist = DistVec::CosDist(pmi, vocab, word1, word2);
        double dist = distVec.CosDist(word1, word2);
        if (dist >= 0) {
            cout << dist << endl;
        }
    }

    return 0;
}

