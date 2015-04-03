#include "iofuncs.h"

#include <dirent.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <omp.h>

using namespace std;

namespace DistVec {
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

        cout << ":\tfind " << files.size() << " files." << endl;
    }
}

