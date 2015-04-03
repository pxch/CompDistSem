#include "distvec.h"
#include "types.h"
#include "utils.h"
#include <iostream>

using namespace std;

int main (int argc, char *argv[]) {
    if (argc != 5) {
        cerr << "Usage: ./trainplf sparse_space_dir training_word_list output_dir dimension" << endl;
        exit(1);
    }

    string input_dir = argv[1];
    if (input_dir[input_dir.length()-1] != '/') {
        input_dir += "/";
    }

    string train_list_path = argv[2];

    string output_dir = argv[3];
    if (output_dir[output_dir.length()-1] != '/') {
        output_dir += "/";
    }

    int dim = atoi(argv[4]);

    DistVec::DistVec distVec(dim);

    distVec.LoadTrainList(train_list_path);
    distVec.SetOutputDir(output_dir);

    distVec.LoadWord(input_dir);
    distVec.CompTransMat();
    distVec.CompDenseWordVec();

    string labels[4] = {"amod", "nsubj", "dobj", "pobj"};

    for (int i = 0; i < 4; ++i) {
        distVec.LoadPhrase(input_dir, labels[i]);
        distVec.CompDensePhraseVec();

        distVec.TrainPLF(labels[i]);
    }

/*
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
        double dist = distVec.CosDist(word1, word2);
        if (dist >= 0) {
            cout << dist << endl;
        }
    }
*/
    return 0;
}

