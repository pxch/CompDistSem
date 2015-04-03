#include "utils.h"

#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <math.h>
#include <omp.h>
#include <sys/stat.h>

using namespace std;

namespace DistVec {
    double cosDist (const vec_t& l, const vec_t& r) {
        return fabs(l.dot(r) / l.norm() / r.norm());
    }

    bool hasNan (const vec_t& vec) {
        return !(vec.array() == vec.array()).all();
    }

    vector<string> split (const string& s, char delim) {
        vector<string> elems;
        stringstream ss(s);
        string item;
        while (getline(ss, item, delim)) {
            elems.push_back(item);
        }
        return elems;
    }

    bool existsFile (const string& name) {
        struct stat buffer;
        return (stat(name.c_str(), &buffer) == 0);
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
                if (strncmp(ent->d_name, "part-", 5) == 0)
                    files.push_back(path + ent->d_name);
            }
            closedir(dir);
        }

        sort(files.begin(), files.end());
        cout << ":\tfind " << files.size() << " files." << endl;
    }

    void ReadDMat (string path, idx_t row, idx_t col, dmat_t& mat) {
        mat.resize(row, col);

        //cout << "------------------------------" << endl;
        //cout << "Loading dense matrix from: " << path << endl;
        //double start_time = omp_get_wtime();

        ifstream in(path.c_str());
        if (!in.is_open()) {
            cerr << "Error!" << endl;
            exit(-1);
        }

        double val;
        for (idx_t i = 0; i < col; ++i) {
            for (idx_t j = 0; j < row; ++j) {
                in >> val;
                mat(j, i) = val;
            }
        }

        //cout << "Done. Elapsed time = ";
        //cout << omp_get_wtime() - start_time << "s" << endl;
        //cout << "------------------------------" << endl; 
    }

    void ReadVocab (string path, str_map& map) {
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

            while (getline(in, line)) {
                map.insert(pair<string, idx_t>(line, word_cnt));
                ++word_cnt;
                ++nlines[i];
            }
            cout << nlines[i] << " lines" << endl;
            in.close();
        }

        cout << "\tnumber of words:\t" << word_cnt << endl;

        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl; 
    }

    void ReadPMI (string path, smat_t& mat) {
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

            while (getline(in, line)) {
                line = line.substr(1, line.length() - 2);
                pos1 = line.find("\t");
                pos2 = line.find(",");

                col_idx = atoi(line.substr(0, pos1).c_str());
                row_idx = atoi(line.substr(pos1 + 1, pos2 - pos1 - 1).c_str());
                val = atof(line.substr(pos2 + 1, line.length() - pos2 - 1).c_str());

                if (row_idx + 1 > row_cnt)
                    row_cnt = row_idx + 1;

                if (col_idx + 1 > col_cnt)
                    col_cnt = col_idx + 1;

                tri_list.push_back(triplet_t(row_idx, col_idx, 1));
                ++nnz_cnt;
/*
                if (symm) {
                    if (row_idx != col_idx) {
                        tri_list.push_back(triplet_t(col_idx, row_idx, 1));
                        ++nnz_cnt;
                    }
                }
*/
                ++nlines[i];
            }
            cout << nlines[i] << " lines" << endl;
            in.close();
        }
/*
        if (symm) {
            if (row_cnt > col_cnt) {
                col_cnt = row_cnt;
            } else if (col_cnt > row_cnt) {
                row_cnt = col_cnt;
            }
        }
*/
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

}

