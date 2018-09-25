#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>
#include <vector>
#include <algorithm>
#include <thread>

#define LL long long

using namespace std;

void remove_file(string dir) {
    /// Remove file from memory.
    char *fileDir = new char [dir.length() + 1];
    strcpy(fileDir, dir.c_str());
    remove(fileDir);
}


void remove_file(int step, int fileId) {
    string fileDir = "step_" + to_string(step) + "_file_" + to_string(fileId) + ".txt";
    remove_file(fileDir);
}

void copy_file(string inputDir, string outputDir) {
    /// Copy content from inputDir to outputDir.
    ifstream input(inputDir, ios::binary);
    ofstream output(outputDir, ios::binary);
    string line;
    while(getline(input, line)) {
        output << line << endl;
    }
    input.close();
    output.close();

    remove_file(inputDir);
}

void copy_step(int step, int inputId, int outputId) {
    string inputDir = "step_" + to_string(step - 1) + "_file_" + to_string(inputId) + ".txt";
    string outputDir = "step_" + to_string(step) + "_file_" + to_string(outputId) + ".txt";
    copy_file(inputDir, outputDir);
}

void print_to_output(int step, string outputDir) {
    string externalDir = "step_" + to_string(step) + "_file_1.txt";
    copy_file(externalDir, outputDir);
}

void merge_file(int step, int firstId, int secondId, int outputId) {
    /// Merging two sorted file in firstInputDir and secondInputDir
    /// to other sorted file outputDir which have all lines from both of them.

    ifstream firstInput, secondInput;
    ofstream output;

    string firstInputDir = "step_" + to_string(step - 1) + "_file_" + to_string(firstId) + ".txt";
    string secondInputDir = "step_" + to_string(step - 1) + "_file_" + to_string(secondId) + ".txt";
    string outputDir = "step_" + to_string(step) + "_file_" + to_string(outputId) + ".txt";

    firstInput.open(firstInputDir, ios::in);
    secondInput.open(secondInputDir, ios::in);
    output.open(outputDir, ios::out);

    string line_1, line_2;

    /* Variable to check if there is still not the end of second file. */
    bool notEnd = bool(getline(secondInput, line_2));

    /* Iterate through two files to merge them. */
    while (getline(firstInput, line_1)) {
        while(notEnd && line_1 > line_2) {
            output << line_2 << endl;
            notEnd = bool(getline(secondInput, line_2));
        }
        output << line_1 << endl;
    }

    /* If the second file is not end then take the rest of it. */
    while(notEnd) {
        output << line_2 << endl;
        notEnd = bool(getline(secondInput, line_2));
    }

    firstInput.close();
    secondInput.close();
    output.close();

    remove_file(firstInputDir);
    remove_file(secondInputDir);
}

void minor_sort(vector <string> * text, int outputId) {
    /// A minor sorting for collections of text that fit to the memory limit.

    sort(text->begin(), text->end());

    string outputDir = "step_0_file_" + to_string(outputId) + ".txt";
    ofstream output(outputDir, ios::out);
    for(int i = 0; i < text->size(); ++i)
        output << text->at(i) << endl;
    output.close();
}

void sorting(char *inputDir, char *outputDir, LL memLimit) {
    /// Sort all line from inputDir using external files and write output to outputDir.
    /// Use external merge sort as major sorting algorithm.

    ifstream input;
    ofstream output;
    input.open(inputDir);

    string line;
    int threadId = 0;
    LL totalSize = 0, numFiles = 0, nThreads = max(1LL, memLimit / 4000);
    bool allThreadAreUsed = false;

    vector <string> * text = new vector <string>();
    vector <thread> * threads = new vector <thread>(nThreads);

    /* Read all line of input and store in text vector.
    When size of text vector exceed quarter of our memory, sort all element of
    this vector and store into new external file. After each two files created, we
    merge them into a new file. */
    while(getline(input, line)) {
        text->push_back(line);
        totalSize += (line.length() + 1) ;
        if(totalSize > min(memLimit / 8, 1000000LL)) {
            ++numFiles;
            minor_sort(text, numFiles);
            text->clear();
            if(numFiles % 2 == 0) {
                if(allThreadAreUsed)
                    threads->at(threadId).join();
                threads->at(threadId) = thread(merge_file, 1, numFiles - 1, numFiles, numFiles / 2);
                threadId = (threadId + 1) % nThreads;
                if(threadId == 0)
                    allThreadAreUsed = true;
            }
            totalSize = 0;
        }
    }
    // Add the remained text to external file
    ++numFiles;
    minor_sort(text, numFiles);

    delete text;

    input.close();

    /*If there is only one file remain in step 0 then copy it to step 1.
    If there is two files remain in step 0 then merge them. */
    if(allThreadAreUsed)
        threads->at(threadId).join();
    if(numFiles % 2)
        threads->at(threadId) = thread(copy_step, 1, numFiles, numFiles / 2 +1);
    else
        threads->at(threadId) = thread(merge_file, 1, numFiles - 1, numFiles, numFiles / 2);
    threadId = (threadId + 1) % nThreads;
    if(threadId == 0)
        allThreadAreUsed = true;

    int step = 1;
    int lastThreadId = 0; // Store first thread id of the last step
    numFiles = (numFiles + 1) / 2;

    // Iterate to apply merge sort without recursion.
    while(numFiles > 1) {
        ++step;
        int curThreadId = threadId;
        for(int i = 1; i <= numFiles; ++i) {
            if(i % 2 == 0) {
                if(allThreadAreUsed)
                    threads->at(threadId).join();
                if (numFiles <= nThreads / 2) {
                    // Join thread of two merge files
                    threads->at((lastThreadId + nThreads + i - 1) % nThreads).join();
                    threads->at((lastThreadId + nThreads + i - 2) % nThreads).join();
                }
                threads->at(threadId) = thread(merge_file, step, i-1, i, i / 2);
                threadId = (threadId + 1) % nThreads;
                if(threadId == 0) allThreadAreUsed = true;
            }
            else {
                if(i == numFiles) {
                    if(allThreadAreUsed)
                        threads->at(threadId).join();

                    //Join thread of file program copy from
                    if(numFiles <= nThreads / 2)
                        threads->at((lastThreadId + nThreads + i - 1) % nThreads).join();

                    threads->at(threadId) = thread(copy_step, step, i, (i+1) / 2);
                    threadId = (threadId + 1) % nThreads;
                    if(threadId == 0) allThreadAreUsed = true;
                }
            }
        }
        lastThreadId = curThreadId;

        // After each merge step number of file divided by 2.
        numFiles = (numFiles + 1) / 2;
    }
    // Join all threads
    for(int i = 0; i < nThreads; ++i) {
        if(threads->at(i).joinable())
            threads->at(i).join();
    }

    print_to_output(step, outputDir);
}

int main(int argc, char * argv[]) {
    char *inputDir, *outputDir;
    LL memLimit;

    inputDir = argv[1];
    outputDir = argv[2];
    memLimit = stoll(argv[3]);

    ios_base::sync_with_stdio(false);
    cin.tie(NULL);
    cout.tie(NULL);

    sorting(inputDir, outputDir, memLimit);
}
