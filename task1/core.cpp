#include "../include/core.h"
#include <cstring>
#include <vector>
#include <unordered_map>
#include <stdlib.h>
#include <cassert>
#include <iostream>
#include <map>


// computes hamming distance between a and b (both null terminated strings). 
// Returns 4 if the Hamming distance
// is greater or equal than 4 or the strings are of different length.
int hamming_distance(const char* a, const char* b, int nb) {
    int it_b = 0;
    int dist = 0;
    for (const char* it = a; *it && dist < 4; it++) {
        if (it_b >= nb) return 4;
        dist += (*it != *(b + it_b)) ? 1 : 0;
        it_b++;
    }
    if (it_b < nb) return 4;
    return dist;
}

int min(int a, int b, int c) {
    return a < b ? (a < c ? a : c) : (b < c ? b : c);
}

int edit_distance(const char* a, const char* b, int nb, int maxDist) {
    int length = std::strlen(a) + 1;
    int width = nb + 1;
    int matrix[length][width];
    int minDist;
    for (int i = 0; i < length; i++) {
        minDist = maxDist;
        for (int j = 0; j < width; j++) {
            if (i == 0) {
                matrix[i][j] = j;
            }
            else if (j == 0) {
                matrix[i][j] = i;
            }
            else {
                int entry = min(matrix[i - 1][j - 1] + (a[i - 1] != b[j - 1]), matrix[i - 1][j] + 1, matrix[i][j - 1] + 1);
                matrix[i][j] = entry;
                if (entry < minDist) {
                    minDist = entry;
                }
            }
        }
        if (minDist > maxDist) {
            return maxDist;
        }
    }
    // if ((matrix[length - 1][width - 1] > maxDist ? maxDist : matrix[length - 1][width - 1]) < maxDist) {
    //     std::cout << "edit distance: " << matrix[length - 1][width - 1] << " for " << a << " and ";
    //     for (int i = 0; i < nb; i++) {
    //         std::cout << b[i];
    //     }
    //     std::cout << std::endl;
    // }
    return matrix[length - 1][width - 1] > maxDist ? maxDist : matrix[length - 1][width - 1];
}

struct QueryElement
{
    QueryID query_id;
    const char* query_str;
    MatchType match_type;
    int match_dist;
};

struct Query {
    QueryID query_id;
    int start;
    int end;
    char* query_str; // for freeing
};

struct Document
{
    DocID doc_id;
    int num_res;
    QueryID* query_ids;
};

// stores the query elements in a vector
std::vector<QueryElement> queries;
// amount of queries
int num_queries = 0;
// hash map for id to query
std::unordered_map<QueryID, Query> query_map;
// stores results
std::vector<Document> documents;
// number of available results
int num_available_results = 0;


ErrorCode InitializeIndex() { return EC_SUCCESS; }

ErrorCode DestroyIndex() {
    // free all query strings
    for (auto it = query_map.begin(); it != query_map.end(); it++) {
        free(it->second.query_str);
    }
    return EC_SUCCESS;
}

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
    // if (match_type == MT_EDIT_DIST) {
    //     std::cout << query_id << ": " << query_str << std::endl;
    // }
    char* mutable_query_str = strdup(query_str);
    const char* c_start = mutable_query_str;
    int num_queries_start = num_queries;
    for (char* it = mutable_query_str; *it; it++) {
        if (*it == ' ') {
            QueryElement qu;
            qu.query_id = query_id;
            qu.query_str = c_start;
            qu.match_type = match_type;
            qu.match_dist = match_dist;
            queries.push_back(qu);
            num_queries++; //should be consistent with the size of queries

            c_start = it + 1;
            *it = '\0'; // null terminate word
        }
    }
    // add last word
    QueryElement qu;
    qu.query_id = query_id;
    qu.query_str = c_start;
    qu.match_type = match_type;
    qu.match_dist = match_dist;
    queries.push_back(qu);
    num_queries++;

    Query q;
    q.query_id = query_id;
    q.start = num_queries_start;
    q.end = num_queries;
    q.query_str = mutable_query_str;
    query_map[query_id] = q;
    return EC_SUCCESS;
}

ErrorCode EndQuery(QueryID query_id) {
    if (query_map.find(query_id) == query_map.end()) {
        return EC_FAIL;
    }
    Query q = query_map[query_id];


    queries.erase(std::next(queries.begin(), q.start), std::next(queries.begin(), q.end));
    //update query map
    for (auto it = query_map.begin(); it != query_map.end(); it++) {
        if (it->second.start > q.start) {
            it->second.start -= q.end - q.start;
        }
        if (it->second.end > q.start) {
            it->second.end -= q.end - q.start;
        }
    }
    num_queries -= q.end - q.start;
    free(q.query_str);
    query_map.erase(query_id);
    return EC_SUCCESS;
}

ErrorCode MatchDocument(DocID doc_id, const char* doc_str) {
    // if (doc_id == 48) {
    //     std::cout << "doc_id: " << doc_id << " doc_str: " << doc_str << std::endl;
    // }

    std::map<QueryID, int> query_match_count;
    std::unordered_map<const QueryElement*, bool> query_element_matched;
    std::vector<QueryID> matched_query_ids; //matched ids
    Document doc;
    doc.doc_id = doc_id;
    int num_res = 0;
    //iterate over doc_str
    int nb = 0;
    const char* b = doc_str;
    const char* doc_str_iterator;
    for (doc_str_iterator = doc_str; *doc_str_iterator; doc_str_iterator++) {
        if (*doc_str_iterator == ' ') {
            nb = doc_str_iterator - b;
            for (auto query_element_iterator = queries.begin(); query_element_iterator != queries.end(); query_element_iterator++) {
                if (query_element_matched.find(&(*query_element_iterator)) != query_element_matched.end()) { // already matched entries
                    continue;
                }
                int dist = 4;
                switch (query_element_iterator->match_type) {
                case MT_EXACT_MATCH:
                    dist = hamming_distance(query_element_iterator->query_str, b, nb) == 0 ? 0 : 4;
                    break;
                case MT_HAMMING_DIST:
                    dist = hamming_distance(query_element_iterator->query_str, b, nb);
                    break;
                case MT_EDIT_DIST:
                    dist = edit_distance(query_element_iterator->query_str, b, nb, query_element_iterator->match_dist + 1);
                    break;
                }
                if (dist <= query_element_iterator->match_dist) {
                    if (query_match_count.find(query_element_iterator->query_id) == query_match_count.end()) {
                        query_match_count[query_element_iterator->query_id] = 0;
                    }
                    query_match_count[query_element_iterator->query_id]++;
                    query_element_matched[&(*query_element_iterator)] = true;
                }
            }
            b = doc_str_iterator + 1;
        }
    }

    nb = doc_str_iterator - b;
    for (auto query_element_iterator = queries.begin(); query_element_iterator != queries.end(); query_element_iterator++) {
        if (!(query_element_matched.find(&(*query_element_iterator)) != query_element_matched.end())) { // already matched entries


            int dist = 4;
            switch (query_element_iterator->match_type) {
            case MT_EXACT_MATCH:
                dist = hamming_distance(query_element_iterator->query_str, b, nb) == 0 ? 0 : 4;
                break;
            case MT_HAMMING_DIST:
                dist = hamming_distance(query_element_iterator->query_str, b, nb);
                break;
            case MT_EDIT_DIST:
                dist = edit_distance(query_element_iterator->query_str, b, nb, query_element_iterator->match_dist + 1);
                break;
            }
            if (dist <= query_element_iterator->match_dist) {
                if (query_match_count.find(query_element_iterator->query_id) == query_match_count.end()) {
                    query_match_count[query_element_iterator->query_id] = 0;
                }
                query_match_count[query_element_iterator->query_id]++;
                query_element_matched[&(*query_element_iterator)] = true;
            }
        }
    }

    for (auto match_count_iterator = query_match_count.begin(); match_count_iterator != query_match_count.end(); match_count_iterator++) {
        if (match_count_iterator->second == query_map[match_count_iterator->first].end - query_map[match_count_iterator->first].start) {
            num_res++;
            matched_query_ids.push_back(match_count_iterator->first);
            // if (doc_id == 48) {
            //     std::cout << match_count_iterator->first << " has " << match_count_iterator->second << " matches from " << query_map[match_count_iterator->first].query_str << std::endl;
            // }
        }
    }
    doc.num_res = num_res;
    doc.query_ids = (QueryID*)malloc(num_res * sizeof(QueryID));
    for (int i = 0; i < num_res; i++) {
        doc.query_ids[i] = matched_query_ids[i];
    }
    if (num_res > 0) {
        num_available_results++;
        documents.push_back(doc);
    }

    return EC_SUCCESS;
}

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids) {
    // Get the first undeliverd resuilt from "docs" and return it
    *p_doc_id = 0; *p_num_res = 0; *p_query_ids = 0;
    if (num_available_results == 0) return EC_NO_AVAIL_RES;
    *p_doc_id = documents[num_available_results - 1].doc_id; *p_num_res = documents[num_available_results - 1].num_res; *p_query_ids = documents[num_available_results - 1].query_ids;
    documents.erase(documents.begin() + num_available_results);
    num_available_results--;
    return EC_SUCCESS;
}
