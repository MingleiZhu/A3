
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

using namespace std;

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs,
                    function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    auto comp = [=] (MyDB_RecordIteratorAltPtr a, MyDB_RecordIteratorAltPtr b) {
        a->getCurrent(lhs);
        b->getCurrent(rhs);
        return !comparator();
    };

    // use priority queue to merge multiple sorted runs
    priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, decltype(comp)> pq(comp);
    for (auto iter : mergeUs) {
        if (iter->advance()) {
            pq.push(iter);
        }
    }

    MyDB_RecordPtr record = sortIntoMe.getEmptyRecord();
    while (!pq.empty()) {
        MyDB_RecordIteratorAltPtr iter = pq.top();
        pq.pop();
        iter->getCurrent(record);
        sortIntoMe.append(record);
        if (iter->advance()) {
            pq.push(iter);
        }
    }
}


vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter, MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator,
	MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    vector<MyDB_PageReaderWriter> mergedList;
    MyDB_PageReaderWriter currentPage(*parent);
    currentPage.clear();

    // if leftIter is empty, just add all the records in rightIter
    if (!leftIter->advance()) {
        while(rightIter->advance()) {
            rightIter->getCurrent(rhs);
            appendRecord(currentPage, mergedList, rhs, parent);
        }
    }

    // if rightIter is empty, just add all the records in leftIter
    else if (!rightIter->advance()) {
         do {
            leftIter->getCurrent(lhs);
            appendRecord(currentPage, mergedList, lhs, parent);
        } while(leftIter->advance());
    }
    else {
        while (true) {
            leftIter->getCurrent(lhs);
            rightIter->getCurrent(rhs);

            if (comparator()) {
                appendRecord(currentPage, mergedList, lhs, parent);
                if(!leftIter->advance()) {
                    appendRecord(currentPage, mergedList, rhs, parent);
                    while (rightIter->advance()) {
                        rightIter->getCurrent(rhs);
                        appendRecord(currentPage, mergedList, rhs, parent);
                    }
                    break;
                }
            }
            else {
                appendRecord(currentPage, mergedList, rhs, parent);
                if(!rightIter->advance()) {
                    appendRecord(currentPage, mergedList, lhs, parent);
                    while(leftIter->advance()) {
                        leftIter->getCurrent(lhs);
                        appendRecord(currentPage, mergedList, lhs, parent);
                    }
                    break;
                }
            }
        }
    }

    mergedList.push_back(currentPage);
    return mergedList;
}

void appendRecord(MyDB_PageReaderWriter& currentPage, vector<MyDB_PageReaderWriter>& pageList, MyDB_RecordPtr record, MyDB_BufferManagerPtr parent) {
    // when currentPage space is not enough, we create a new anonymous page to store the merged records
    if (!currentPage.append(record)) {
        pageList.push_back(currentPage);
        MyDB_PageReaderWriter temp(*parent);
        temp.clear();
        temp.append(record);
        currentPage = temp;
    }
}

vector<MyDB_PageReaderWriter> mergeSort(vector<MyDB_PageReaderWriter>& sortMe, MyDB_BufferManagerPtr bufferManager, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    if (sortMe.size() <= 1) {
        return sortMe;
    }
    else {
        vector<MyDB_PageReaderWriter> leftPart (sortMe.begin(), sortMe.begin() + (sortMe.size() / 2));
        vector<MyDB_PageReaderWriter> rightPart (sortMe.begin() + (sortMe.size() / 2), sortMe.end());
        vector<MyDB_PageReaderWriter> sortedLeft = mergeSort(leftPart, bufferManager, comparator, lhs, rhs);
        vector<MyDB_PageReaderWriter> sortedRight = mergeSort(rightPart, bufferManager, comparator, lhs, rhs);
        vector<MyDB_PageReaderWriter> result = mergeIntoList(bufferManager, getIteratorAlt(sortedLeft), getIteratorAlt(sortedRight), comparator, lhs, rhs);
        return result;
    }
}

void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
           function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    // we need to merge all sorted runs in merge phase to get the sorted file
    vector<MyDB_RecordIteratorAltPtr> sortedRuns;

    int pageNum = 0;
    while (pageNum < sortMe.getNumPages()) {

        // load runSize pages into RAM, sort them in sort phase
        vector<MyDB_PageReaderWriter> run;

        for (int i = 0; i < runSize && pageNum < sortMe.getNumPages(); i++) {
            run.push_back(*sortMe[pageNum].sort(comparator, lhs, rhs));
            pageNum++;
        }
        // use mergesort to implement sort phase (sort a whole run)
        vector<MyDB_PageReaderWriter> sortedRun = mergeSort(run, sortMe.getBufferMgr(), comparator, lhs, rhs);

        //add current sorted run to the sortedRuns list
        sortedRuns.push_back(getIteratorAlt(sortedRun));
    }

    // below is the merge phase, merge all the sorted runs to a single file
    mergeIntoFile(sortIntoMe, sortedRuns, comparator, lhs, rhs);
}

// below is iterative version of mergesort, but its time complexity is worse

//void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
//           function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
//    vector<MyDB_RecordIteratorAltPtr> sortedRuns;
//    int pageNum = 0;
//    while (pageNum < sortMe.getNumPages()) {
//
//        // load runSize pages into RAM, sort them in sort phase
//        vector<MyDB_PageReaderWriter> run;
//
//        for (int i = 0; i < runSize && pageNum < sortMe.getNumPages(); i++) {
//            run.push_back(*sortMe[pageNum].sort(comparator, lhs, rhs));
//            pageNum++;
//        }
//
//        // sort each page in run
//        vector<vector<MyDB_PageReaderWriter>> sortedLists;
//        for (int i = 0; i < run.size() / 2; i++) {
//            vector<MyDB_PageReaderWriter> sortedList = mergeIntoList(sortMe.getBufferMgr(), run[2 * i].getIteratorAlt(), run[2 * i + 1].getIteratorAlt(), comparator, lhs, rhs);
//            sortedLists.push_back(sortedList);
//        }
//        if (run.size() % 2 == 1) {
//            vector<MyDB_PageReaderWriter> sortedList;
//            sortedList.push_back(run.back());
//            sortedLists.push_back(sortedList);
//        }
//        // merge sorted lists of pages until we only have one sorted list of pages
//        while (sortedLists.size() > 1) {
//            vector<MyDB_PageReaderWriter> list1 = sortedLists.back();
//            sortedLists.pop_back();
//            vector<MyDB_PageReaderWriter> list2 = sortedLists.back();
//            sortedLists.pop_back();
//            vector<MyDB_PageReaderWriter> list = mergeIntoList(sortMe.getBufferMgr(), getIteratorAlt(list1),
//                                                               getIteratorAlt(list2), comparator, lhs, rhs);
//            sortedLists.push_back(list);
//        }
//        MyDB_RecordIteratorAltPtr runIter = getIteratorAlt(sortedLists[0]);
//        sortedRuns.push_back(runIter);
//    }
//    mergeIntoFile(sortIntoMe, sortedRuns, comparator, lhs, rhs);
//}

#endif
