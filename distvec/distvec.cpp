#include "distvec.h"
#include "utils.h"

#include <dirent.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <string.h>
#include <omp.h>

using namespace std;

namespace DistVec {
    DistVec::DistVec (int d) {
        this->dim = d;
    }

    DistVec::~DistVec () {
        word_vocab.clear();
        word_pmi.setZero();
        phrase_vocab.clear();
        phrase_pmi.setZero();
    }

    void DistVec::LoadWord (string path) {
        ReadVocab(path + "wordvocab", this->word_vocab);
        ReadPMI(path + "wordpmi", this->word_pmi);
    }

    void DistVec::LoadPhrase (string path, string label) {
        ReadVocab(path + label + "-phrasevocab", this->phrase_vocab);
        ReadPMI(path + label + "-phrasepmi", this->phrase_pmi);
    }

    void DistVec::LoadTrainList (string prefix) {
        train_list[0].clear();
        train_list[1].clear();
        train_list[2].clear();
        train_list[3].clear();
        train_list[4].clear();

        string labels[5] = {"amod", "nsubj", "dobj", "pobj", "acomp"};

        for (int i = 0; i < 5; ++i) {
            string path = prefix + labels[i];
            cout << "------------------------------" << endl;
            cout << "Loading training list from " << path << endl;
            ifstream in(path.c_str());

            if (!in.is_open()) {
                cerr << "Error!" << endl;
                exit(-1);
            }

            string line, word, pos;
            while (getline(in, line)) {
                word = line.substr(0, line.find("\t"));
                train_list[i].push_back(word);
                /*
                pos = word.substr(word.length() - 2, 2);
                if (pos == "jj") {
                    train_list[0].push_back(word);
                } else if (pos == "vb") {
                    train_list[1].push_back(word);
                } else if (pos == "in") {
                    train_list[2].push_back(word);
                }
                */
            }

            in.close();

            cout << "Find " << train_list[i].size() << " " << labels[i] << endl;
            /*
            cout << "Find " << train_list[0].size() << " adjectives";
            cout << ", " << train_list[1].size() << " verbs";
            cout << ", " << train_list[2].size() << " prepositions" << endl;
            */
            cout << "------------------------------" << endl;
        }
    }

    void DistVec::SetOutputDir (string dir) {
        this->output_dir = dir;
    }

    void DistVec::CompTransMat () {
/*
        RedSVD::RedSymEigen<smat_t> eigenSolver(word_pmi, 300);
        trans_mat = eigenSolver.eigenvectors();
        word_dmat = word_pmi * trans_mat;
*/
        cout << "------------------------------" << endl;
        cout << "SVD on word pmi matrix" << endl;
        double start_time = omp_get_wtime();
        RedSVD::RedSVD<smat_t> svdSolver(word_pmi, dim);
/*
        cout << "\tU size: " << svdSolver.matrixU().rows() << "\t"
            << svdSolver.matrixU().cols() << endl;
        cout << "\tV size: " << svdSolver.matrixV().rows() << "\t"
            << svdSolver.matrixV().cols() << endl;
        cout << "\tSingular vector size: " << svdSolver.singularValues().count() << endl;
        cout << "Done. Elapsed time = ";
*/
        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl; 

        trans_mat.resize(dim, word_pmi.rows());
        trans_mat = svdSolver.matrixU().transpose();
    }

    void DistVec::CompDenseWordVec () {
        cout << "------------------------------" << endl;
        cout << "Compute dense vector for words" << endl;
        double start_time = omp_get_wtime();
        word_dmat.resize(dim, word_pmi.cols());
        word_dmat = trans_mat * word_pmi;

        // Normalize word vector
        for (idx_t i = 0; i < word_dmat.cols(); ++i) {
            word_dmat.col(i) = word_dmat.col(i).normalized();
        }

        WriteDMat(output_dir + "word.dm", this->word_dmat);
        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl; 
    }

    void DistVec::CompDensePhraseVec () {
        cout << "------------------------------" << endl;
        cout << "Compute dense vector for phrases" << endl;
        double start_time = omp_get_wtime();
        phrase_dmat.resize(dim, phrase_pmi.cols());
        phrase_dmat = trans_mat * phrase_pmi;
        cout << "Done. Elapsed time = ";
        cout << omp_get_wtime() - start_time << "s" << endl;
        cout << "------------------------------" << endl; 
    }

    void DistVec::TrainPLF (string label) {
        int tl_idx = -1;
        int loc = -1;
        if (label == "amod") {
            tl_idx = 0;
            loc = 0;
        } else if (label == "nsubj") {
            tl_idx = 1;
            loc = 1;
        } else if (label == "dobj") {
            tl_idx = 2;
            loc = 0;
        } else if (label == "pobj") {
            tl_idx = 3;
            loc = 0;
        } else if (label == "acomp") {
            tl_idx = 4;
            loc = 0;
        } else {
            cerr << "Invalid label: " << label << endl;
            return;
        }

        mod_mat.resize(dim, dim);

        string train_word;
        int train_word_idx;
        for (int i = 0; i < train_list[tl_idx].size(); ++i) {
            cout << "------------------------------" << endl; 
            train_word = train_list[tl_idx][i];
            cout << "Train word " + train_word << endl;

            train_word_idx = GetWordIdx(train_word);
            if (train_word_idx == -1) {
                cout << "\tNot found!" << endl;
                continue;
            }
            mod_vec = word_dmat.col(train_word_idx);

            double start_time = omp_get_wtime();
            PrepTrainMat(train_word, loc);
            idx_t N = arg_space.cols();

            double params_range[3] = {0.1, 1, 10};
            double dist[3], S_trace[3];
            dmat_t result[3];
            for (int i = 0; i < 3; ++i) {
                RidgeRegression(params_range[i], result[i], dist[i], S_trace[i]);
            }

            int min_idx = -1;
            double min_err = numeric_limits<double>::max();
            double gcv_err = numeric_limits<double>::max();
            for (int i = 0; i < 3; ++i) {
                double nom = (1 - S_trace[i] / N) * (1 - S_trace[i] / N) * N;
                if (nom != 0) {
                    gcv_err = (dist[i] * dist[i]) / nom;
                }
                if (gcv_err < min_err) {
                    min_err = gcv_err;
                    min_idx = i;
                }
            }
            if (min_idx == -1) {
                min_idx = 0;
            }
            cout << "\tBest lambda in cross validation: " << params_range[min_idx] << endl;

            // RidgeRegression(0.1);
            WriteDMat(output_dir + train_word + "." + label + ".dm", result[min_idx]);
            cout << "Done. Elapsed time = ";
            cout << omp_get_wtime() - start_time << "s" << endl;
            cout << "------------------------------" << endl; 
        }
    }

    void DistVec::PrepTrainMat (string train_word, int loc) {
        if (loc != 0 && loc != 1) {
            cerr << "Location must be 0 or 1!" << endl;
            return;
        }

        idx_t match_num = 0;
        string phrase, mod_word, arg_word;
        idx_t p_idx, a_idx;
        veci_t p_indices, a_indices;
        int sep;

        p_indices.clear();
        a_indices.clear();

        str_map_iter it;
        for (it = phrase_vocab.begin(); it != phrase_vocab.end(); ++it) {
            phrase = it->first;
            p_idx = it->second;
            sep = phrase.find("+"); 
            if (loc == 0) {
                mod_word = phrase.substr(0, sep);
                arg_word = phrase.substr(sep+1, phrase.length()-sep-1);
            } else {
                arg_word = phrase.substr(0, sep);
                mod_word = phrase.substr(sep+1, phrase.length()-sep-1);
            }
            if (mod_word == train_word) {
                a_idx = GetWordIdx(arg_word);
                if (a_idx != -1) {
                    p_indices.push_back(p_idx);
                    a_indices.push_back(a_idx);
                    ++match_num;
                }
            }
        }

        cout << "\tFind " << match_num << " training samples" << endl;
        phrase_space.resize(dim, match_num);
        arg_space.resize(dim, match_num);
        for (idx_t i = 0; i < match_num; ++i) {
            phrase_space.col(i) = phrase_dmat
                .col(p_indices[i]).normalized() - mod_vec;
            arg_space.col(i) = word_dmat.col(a_indices[i]);
        }
    }

    void DistVec::RidgeRegression (double lambda, dmat_t& result, double& dist, double& S_trace) {
        result.resize(this->dim, this->dim);
        dmat_t A_t = arg_space.transpose();
        dmat_t lambda_diag = vec_t(dim)
            .setConstant(lambda).asDiagonal();
        dmat_t tmp_mat = (arg_space * A_t + lambda_diag).inverse();
        dmat_t tmp_res = A_t * tmp_mat;
        result = phrase_space * tmp_res;
        dist = (result * arg_space - phrase_space).norm();
        S_trace = A_t.cwiseProduct(tmp_res).sum();
    }

    void DistVec::WriteDMat (string path, dmat_t& mat) {
        cout << "\tWriting to " << path << endl;
        ofstream out(path, ofstream::out);
        for (idx_t i = 0; i < mat.cols(); ++i) {
            out << mat.col(i).transpose() << endl;
        }
        out.close();
    }

    void DistVec::WriteOutputMat (string path) {
        cout << "\tWriting to " << path << endl;
        ofstream out(path, ofstream::out);
        for (idx_t i = 0; i < dim; ++i) {
            out << mod_mat.col(i).transpose() << endl;
        }
        out.close();
    }

    void DistVec::WriteWordMat (string path) {
        cout << "\tWriting dense matrix for word to " << path << endl;
        ofstream out(path, ofstream::out);
        for (idx_t i = 0; i < word_dmat.cols(); ++i) {
            out << word_dmat.col(i).transpose() << endl;
        }
        out.close();
    }

    double DistVec::CosDist (string word1, string word2) {
        idx_t idx1 = GetWordIdx(word1);
        idx_t idx2 = GetWordIdx(word2);
        if (idx1 == -1) {
            cout << "Can not find word " << word1 << endl;
            return -1.0;
        } else if (idx2 == -1) {
            cout << "Can not find word " << word2 << endl;
            return -1.0;
        } else {
            vec_t vec1 = word_pmi.col(idx1);
            vec_t vec2 = word_pmi.col(idx2);
            return vec1.dot(vec2) / vec1.norm() / vec2.norm();
        }
    }

    idx_t DistVec::GetWordIdx (string word) {
        str_map_citer iter = word_vocab.find(word);
        if (iter == word_vocab.end()) {
            return -1;
        } else {
            return iter->second;
        }
    }

}

