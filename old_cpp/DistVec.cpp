#include "distvec.h"
#include "iofuncs.h"

#include <dirent.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <omp.h>

using namespace std;

namespace DistVec {
    DistVec::DistVec () {}

    DistVec::DistVec (string vocab_dir, string pmi_dir) {
        ReadVocabularyMap(vocab_dir);
        ReadPMIMatrix(pmi_dir);
    }

    DistVec::~DistVec () {
        vocab.clear();
        pmi.setZero();
    }

    void DistVec::ReadVocabularyMap (string path) {
        vocab.clear();

        cout << "------------------------------" << endl;
        str_vec files;
        ReadDir(path, files);

        cout << "Loading vocabulary map" << endl;
        
        double start_time = omp_get_wtime();

        idx_t word_cnt = 0;
        veci_t nlines(files.size());

        for (int i = 0; i < files.size(); ++i) {
            cout << "\tReading file " << files[i] << ":\t";
            ifstream in(files[i].c_str());

            nlines[i] = 0;
            string line;

            if (!in.is_open()) {
                cerr << "Error!" << endl;
                exit(-1);
            }

            while(getline(in, line)) {
                vocab.insert(pair<string, idx_t>(line, word_cnt));
                ++word_cnt;
                ++nlines[i];
            }
            cout << nlines[i] << " lines" << endl;
        }

        cout << "\tnumber of words:\t" << word_cnt << endl;

        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl; 
    }

    void DistVec::ReadPMIMatrix (string path) {
        pmi.setZero();

        cout << "------------------------------" << endl;
        str_vec files;
        ReadDir(path, files);

        cout << "Loading PMI matrix" << endl;
        
        double start_time = omp_get_wtime();

        idx_t row_cnt = 0, col_cnt = 0, nnz_cnt = 0;
        veci_t nlines(files.size());

        tri_vec tri_list;

        for (int i = 0; i < files.size(); ++i) {
            cout << "\tReading file " << files[i] << ":\t";
            ifstream in(files[i].c_str());
            if (!in.is_open()) {
                cerr << "Error!" << endl;
                exit(-1);
            }

            nlines[i] = 0;

            string line;
            size_t pos1, pos2;
            idx_t row_idx, col_idx;
            double val;

            while(getline(in, line)) {
                line = line.substr(1, line.length() - 2);
                pos1 = line.find("\t");
                pos2 = line.find(",");

                row_idx = atoi(line.substr(0, pos1).c_str());
                col_idx = atoi(line.substr(pos1 + 1, pos2 - pos1 - 1).c_str());
                val = atof(line.substr(pos2 + 1, line.length() - pos2 - 1).c_str());
    /*
                if (nnz_cnt < 10)
                cout << row_idx << "\t" << col_idx << "\t" << val << endl;
    */
                if (row_idx + 1 > row_cnt)
                    row_cnt = row_idx + 1;
                if (col_idx + 1 > row_cnt)
                    row_cnt = col_idx + 1;

                if (row_idx + 1 > col_cnt)
                    col_cnt = row_idx + 1;
                if (col_idx + 1 > col_cnt)
                    col_cnt = col_idx + 1;

                tri_list.push_back(triplet_t(row_idx, col_idx, 1));
                ++nnz_cnt;
                if (row_idx != col_idx) {
                    tri_list.push_back(triplet_t(col_idx, row_idx, 1));
                    ++nnz_cnt;
                }
                ++nlines[i];
            }
            cout << nlines[i] << " lines" << endl;
//            nnz_cnt += nlines[i];
        }

        cout << "\trows:\t" << row_cnt << endl;
        cout << "\tcols:\t" << col_cnt << endl;
        cout << "\tnnzs:\t" << nnz_cnt << endl;

        pmi.resize(row_cnt, col_cnt);
        pmi.reserve(nnz_cnt);

        pmi.setFromTriplets(tri_list.begin(), tri_list.end());

        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl;
    }

    double DistVec::CosDist (string word1, string word2) {
        str_map_iter iter1 = vocab.find(word1);
        str_map_iter iter2 = vocab.find(word2);
        if (iter1 == vocab.end()) {
            cout << "Can not find word " << word1 << endl;
            return -1.0;
        } else if (iter2 == vocab.end()) {
            cout << "Can not find word " << word2 << endl;
            return -1.0;
        } else {
            idx_t idx1 = iter1->second;
            idx_t idx2 = iter2->second;
            vec_t vec1 = pmi.col(idx1);
            vec_t vec2 = pmi.col(idx2);
            return vec1.dot(vec2) / vec1.norm() / vec2.norm();
        }
    }
}

