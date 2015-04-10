#include "phsim.h"
#include "utils.h"

#include <iostream>
#include <fstream>

using namespace std;

namespace DistVec {
    PhSim::PhSim (int d, int v) {
        this->dim = d;
        this->vocab_size = v;
    }

    PhSim::~PhSim () {}

    void PhSim::CompRules (string in_path, string out_prefix) {
        cout << "------------------------------" << endl;
        cout << "Loading rules from: " << in_path << endl;
        double start_time = omp_get_wtime();

        ifstream in(in_path);
        if (!in.is_open()) {
            cerr << "Error!" << endl;
            exit(-1);
        }

        ofstream out_score(out_prefix + "_scores", ofstream::out);
        ofstream out_phrase(out_prefix + "_phs", ofstream::out);
        ofstream out_vector(out_prefix + "_vecs", ofstream::out);

        string line;
        string lPh, rPh;
        vector<string> lPhSeq, rPhSeq;
        vector<string>::iterator iter;
        vector<vec_t> lVecSeq, rVecSeq;
        double sim;

        vector<string> phList;
        vector<vec_t> vecList;

        phList.clear();
        vecList.clear();

        int cnt = 0;
        //line = "1 = hold-vb nsubj-#1# homeless-jj amod-#2# man-nn #2# #1# dobj-#3# sign-nn #3#	2 = beg-vb dobj-#1# for-in pobj-#2# money-nn #2# #1#	3 = fdjio-vb		1 = travel-vb dobj-#1# by-in pobj-#2# car-nn #2# #1# nsubj-#3# woman-nn #3#	2 = man-nn";

        while (getline(in, line)) {
            ++cnt;
            int sep = line.find("\t\t");
            lPh = line.substr(0, sep);
            rPh = line.substr(sep+2, line.length()-sep-2);

            cout << lPh << "\t" << rPh << endl;

            if (lPh.compare("#FAIL#") == 0 || rPh.compare("#FAIL#") == 0) {
                out_score << cnt << "\t-2\t-2\t#FAIL#" << endl;
                continue;
            }
            lPhSeq = split(lPh, '\t');
            rPhSeq = split(rPh, '\t');
            for (int i = 0; i < lPhSeq.size(); ++i) {
                int sep = lPhSeq[i].find("=");
                lPhSeq[i] = lPhSeq[i].substr(sep+2, lPhSeq[i].length()-sep-2);
            }
            for (int i = 0; i < rPhSeq.size(); ++i) {
                int sep = rPhSeq[i].find("=");
                rPhSeq[i] = rPhSeq[i].substr(sep+2, rPhSeq[i].length()-sep-2);
            }

            lVecSeq.clear();
            rVecSeq.clear();
            iter = lPhSeq.begin();
            while (iter != lPhSeq.end()) {
                vec_t vec = CompPhVec(*iter);
                if (vec != vec_t::Zero(this->dim) && !hasNan(vec)) {
                    lVecSeq.push_back(vec);
                    ++ iter;
                } else {
                    iter = lPhSeq.erase(iter);
                }
            }
            iter = rPhSeq.begin();
            while (iter != rPhSeq.end()) {
                vec_t vec = CompPhVec(*iter);
                if (vec != vec_t::Zero(this->dim) && !hasNan(vec)) {
                    rVecSeq.push_back(vec);
                    ++ iter;
                } else {
                    iter = rPhSeq.erase(iter);
                }
            }

/*
            if (cnt < 20) {
                cout << "Computing phrasal vectors for pair #" << cnt << endl;
                cout << "LHS: " << endl;
                for (int i = 0; i < lPhSeq.size(); ++i) {
                    cout << "\t" << lPhSeq[i] << endl;
                }
                cout << "RHS: " << endl;
                for (int i = 0; i < rPhSeq.size(); ++i) {
                    cout << "\t" << rPhSeq[i] << endl;
                }
            }
*/

            if (lVecSeq.size() == 0 || rVecSeq.size() == 0) {
                out_score << cnt << "\t-1\t-1\t#FAIL#" << endl;
                continue;
            }

            vec_t lVecMean = vec_t::Zero(this->dim);
            for (int i = 0; i < lVecSeq.size(); ++i) {
                lVecMean += lVecSeq[i];
            }
            vec_t rVecMean = vec_t::Zero(this->dim);
            for (int i = 0; i < rVecSeq.size(); ++i) {
                rVecMean += rVecSeq[i];
            }

            double meanSim = cosDist(lVecMean, rVecMean);

            vector<double> greedySim;
            vector<string> greedyPair;
            greedySim.clear();
            greedyPair.clear();
            while (lVecSeq.size() > 0 && rVecSeq.size() > 0) {
                int lIdx = -1, rIdx = -1;
                double maxSim = -1.0;
                for (int i = 0; i < lVecSeq.size(); ++i) {
                    for (int j = 0; j < rVecSeq.size(); ++j) {
                        double tmpSim = cosDist(lVecSeq[i], rVecSeq[j]);
                        if (tmpSim > maxSim) {
                            maxSim = tmpSim;
                            lIdx = i;
                            rIdx = j;
                        }
                    }
                }
                greedySim.push_back(maxSim);
                greedyPair.push_back(lPhSeq[lIdx] + " : " + rPhSeq[rIdx]);

                phList.push_back(lPhSeq[lIdx]);
                phList.push_back(rPhSeq[rIdx]);
                vecList.push_back(lVecSeq[lIdx]);
                vecList.push_back(rVecSeq[rIdx]);

                lPhSeq.erase(lPhSeq.begin() + lIdx);
                rPhSeq.erase(rPhSeq.begin() + rIdx);
                lVecSeq.erase(lVecSeq.begin() + lIdx);
                rVecSeq.erase(rVecSeq.begin() + rIdx);
            }
/*
            vec_t lVec = CompPhVec(lPh);
            vec_t rVec = CompPhVec(rPh);
            if (lVec == vec_t::Zero(this->dim) || rVec == vec_t::Zero(this->dim)) {
                sim = -1.0;
            } else {
                sim = lVec.dot(rVec) / lVec.norm() / rVec.norm();
            }
            out << cnt << "\t" << sim << endl;
*/
            out_score << cnt << "\t" << meanSim << "\t";
            for (int i = 0; i < greedySim.size(); ++i) {
                out_score << greedySim[i];
                if (i != greedySim.size() -1) {
                    out_score << ", ";
                } else {
                    out_score << "\t";
                }
            }
            for (int i = 0; i < greedyPair.size(); ++i) {
                out_score << greedyPair[i];
                if (i != greedySim.size() -1) {
                    out_score << ", ";
                } else {
                    out_score << endl;
                }
            }
        }
        for (int i = 0; i < phList.size(); ++i) {
            out_phrase << phList[i] << endl;
        }
        for (int i = 0; i < vecList.size(); ++i) {
            out_vector << vecList[i].transpose() << endl;
        }

        in.close();
        out_score.close();
        out_phrase.close();
        out_vector.close();
    }

    vec_t PhSim::CompPhVec (string ph) {
        cout << "------------------------------" << endl;
        cout << "Parse phrase: " << ph << endl;

/*
        if (ph.compare("#FAIL#") == 0) {
            return vec_t::Zero(this->dim);
        }
*/
        vector<string> elems = split(ph, ' ');
        //elems.erase(elems.begin());

        return CompPhVec(elems);
/*
        if (elems[0] == "#SINGLETON#") {
            string word = elems[1];
            idx_t word_idx = GetWordIdx(word);
            if (word_idx == -1) {
                return vec_t::Zero(this->dim);
            }
            cout << "Find singleton word: " << word << "in vocabulary" << endl;
            return word_dmat.col(word_idx);
        }

        vec_t result(this->dim);

        if (elems[0] == "#VP#") {
            cout << "This is a verb phrase.";
            string verb = elems[1];
            idx_t verb_idx = GetWordIdx(verb);
            if (verb_idx == -1) {
                return vec_t::Zero(this->dim);
            }
            cout << " Find verb: " << verb << " in vocabulary" << endl;
            result = word_dmat.col(verb_idx);

            int elem_idx = 2;
            while (elem_idx < elems.size()) {
                int pos = elems[elem_idx].find('-');
                string dep_label = elems[elem_idx].substr(0, pos);

                cout << "Process " << dep_label << " relation:";

                string comp_label = elems[elem_idx].substr(pos+1, elems[elem_idx].length());

                vector<string> comp;
                for (int idx1 = elem_idx + 1; idx1 < elems.size(); ++idx1) {
                    if (elems[idx1].compare(comp_label) == 0) {
                        elem_idx = idx1 + 1;
                        break;
                    } else {
                        comp.push_back(elems[idx1]);
                    }
                }

                string mod_mat_file = this->mod_mat_dir + verb + "." + dep_label + ".dm";
                if (!existsFile(mod_mat_file)) {
                    cout << " matrix file " << mod_mat_file << " not found!" << endl;
                    continue;
                }
                cout << " find matrix file " << mod_mat_file << endl;

                dmat_t mod_mat;
                LoadModMat(mod_mat_file, mod_mat);

                vec_t arg_vec = CompPhVec(comp);
                if (arg_vec != vec_t::Zero(this->dim)) {
                    result = result + mod_mat * arg_vec;
                }
            }
        }
*/
        cout << "------------------------------" << endl;
    }

    vec_t PhSim::CompPhVec (vector<string> elems) {
        cout << "Parse phrase: ";
        for (int i = 0; i < elems.size(); ++i) {
            cout << elems[i] << " ";
        }
        cout << endl;

        vec_t result(this->dim);

        string word = elems[0];
        idx_t word_idx = GetWordIdx(word);
        if (word_idx == -1) {
            cout << "Cannot find word: " << word << endl;
            result.setZero();
            return result;
        }
        cout << "Find word: " << word << endl;

        result = word_dmat.col(word_idx);

        int elem_idx = 1;
        while (elem_idx < elems.size()) {
            string label = elems[elem_idx];
            int sep = label.find('-');
            string dep_label = label.substr(0, sep);

            cout << "Process " << dep_label << " relation: ";

            string comp_label = label.substr(sep+1, label.length()-sep-1);

            vector<string> comp;
            for (int idx1 = elem_idx + 1; idx1 < elems.size(); ++idx1) {
                if (elems[idx1].compare(comp_label) == 0) {
                    elem_idx = idx1 + 1;
                    break;
                } else {
                    comp.push_back(elems[idx1]);
                }
            }

            vec_t arg_vec = CompPhVec(comp);

            if (dep_label == "pmod") {
                result = result + arg_vec;
                result = result.normalized();
            } else {
                string mod_mat_file = this->mod_mat_dir + word + "." + dep_label + ".dm";
                if (!existsFile(mod_mat_file)) {
                    cout << " matrix file " << mod_mat_file << " not found!" << endl;
                    result.setZero();
                    return result;
                }
                cout << " find matrix file " << mod_mat_file << endl;

                dmat_t mod_mat;
                LoadModMat(mod_mat_file, mod_mat);

                if (arg_vec == vec_t::Zero(this->dim)) {
                    result.setZero();
                    return result;
                }
                result = result + mod_mat * arg_vec;
                result = result.normalized();
            }
        }

        return result;
    }

    idx_t PhSim::GetWordIdx (string word) {
        str_map_citer iter = word_vocab.find(word);
        if (iter == word_vocab.end()) {
            return -1;
        } else {
            return iter->second;
        }
    }

    void PhSim::LoadVocab (string path) {
        ReadVocab(path, this->word_vocab);
    }

    void PhSim::LoadWordMat (string path) {
        ReadDMat(path, dim, vocab_size, this->word_dmat);
    }

    void PhSim::LoadModMat (string path, dmat_t& mat) {
        ReadDMat(path, dim, dim, mat);
    }

    void PhSim::SetModMatDir (string dir) {
        this->mod_mat_dir = dir;
    }
}

