#include "iofuncs.h"

#include <dirent.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <omp.h>

using namespace std;

namespace DistVec {
    void ReadPMIMatrix (string path, smat_t& mat) {
        mat.setZero();

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

        mat.resize(row_cnt, col_cnt);
        mat.reserve(nnz_cnt);

        mat.setFromTriplets(tri_list.begin(), tri_list.end());

        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl;
    }

    void ReadVocabularyMap (std::string path, str_map& map) {
        map.clear();

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
                map.insert(pair<string, idx_t>(line, word_cnt));
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

    void ReadDir (string path, str_vec& files) {
        if (path[path.length()-1] != '/') {
            path += "/";
        }
        cout << "Reading from directory " << path;

        files.clear();

        DIR* dir;
        struct dirent *ent;
        if ((dir = opendir(path.c_str())) != NULL) {
            while ((ent = readdir(dir)) != NULL) {
//                if (strcmp(ent->d_name, ".") != 0 &&
//                    strcmp(ent->d_name, "..") != 0 &&
//                    strcmp(ent->d_name, "_SUCCESS") != 0)
                if (strncmp(ent->d_name, "part-", 5) == 0)
                    files.push_back(path + ent->d_name);
            }
            closedir(dir);
        }

        cout << ":\tfind " << files.size() << " files." << endl;
    }

    double CosDist (smat_t& mat, str_map& map, string word1, string word2) {
        str_map_iter iter1 = map.find(word1);
        str_map_iter iter2 = map.find(word2);
        if (iter1 == map.end()) {
            cout << "Can not find word " << word1 << endl;
            return -1.0;
        } else if (iter2 == map.end()) {
            cout << "Can not find word " << word2 << endl;
            return -1.0;
        } else {
            idx_t idx1 = iter1->second;
            idx_t idx2 = iter2->second;
            vec_t vec1 = mat.col(idx1);
            vec_t vec2 = mat.col(idx2);
            return vec1.dot(vec2) / vec1.norm() / vec2.norm();
        }
    }

}

