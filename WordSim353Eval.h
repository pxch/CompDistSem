#ifndef CLASS_WORDSIM353_EVAL_
#define CLASS_WORDSIM353_EVAL_

namespace DistVec {
    class WordSim353Eval {
    public:
        WordSim353Eval ();
        WordSim353Eval (std::string file_path);
        ~WordSim353Eval ();

        void ReadData (std::string path);

        void Evaluate ( 
