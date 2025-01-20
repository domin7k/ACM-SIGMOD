#include "../include/core.h"
#include <cstring>
#include <vector>
#include <unordered_map>
#include <stdlib.h>
#include <cassert>
#include <iostream>
#include <map>
#include <emmintrin.h> 
#include <algorithm>


// computes hamming distance between a and b (both null terminated strings). 
// Returns 4 if the Hamming distance
// is greater or equal than 4 or the strings are of different length.
int hamming_distance(const char* a, const char* b, int na, int nb, int maxDist) {
    if (na != nb) {
        printf("EEEEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOORRRRRRRRRRRRR");
        return 4;
    }

    int dist = 0;
    int i = 0;

    // Process 16 bytes at a time using SSE2
    for (; i <= na - 16; i += 16) {
        __m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
        __m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));
        __m128i vcmp = _mm_cmpeq_epi8(va, vb);
        int mask = _mm_movemask_epi8(vcmp);
        dist += 16 - __builtin_popcount(mask);
        if (dist >= 4) return 4;
    }

    // Process remaining bytes
    for (; i < na; ++i) {
        dist += (a[i] != b[i]) ? 1 : 0;
        if (dist >= 4) return 4;
    }

    return dist;
}

int min(int a, int b, int c) {
    return a < b ? (a < c ? a : c) : (b < c ? b : c);
}

int EditDistanceRef(const char* a, int na, const char* b, int nb, int maxDist)
{
    int oo = 0x7FFFFFFF;

    static int T[2][MAX_WORD_LENGTH + 1];

    int ia, ib;

    int cur = 0;
    ia = 0;

    for (ib = 0;ib <= nb;ib++)
        T[cur][ib] = ib;

    cur = 1 - cur;

    for (ia = 1;ia <= na;ia++)
    {
        ib = 0;
        T[cur][ib] = ia;

        int minDist = oo;

        for (ib = 1;ib <= nb;ib++)
        {
            int ret = oo;

            int d1 = T[1 - cur][ib] + 1;
            int d2 = T[cur][ib - 1] + 1;
            int d3 = T[1 - cur][ib - 1]; if (a[ia - 1] != b[ib - 1]) d3++;

            if (d1 < ret) ret = d1;
            if (d2 < ret) ret = d2;
            if (d3 < ret) ret = d3;

            T[cur][ib] = ret;
            if (ret < minDist) minDist = ret;
        }
        if (minDist >= maxDist) return maxDist;

        cur = 1 - cur;
    }

    int ret = T[1 - cur][nb];

    return ret;
}

int edit_distance(const char* a, const char* b, int na, int nb, int maxDist) {
    int dist = 0;
    dist = EditDistanceRef(a, na, b, nb, maxDist);
    return dist;
}

struct QueryElement
{
    QueryID query_id;
    char* query_str;
    unsigned int found_in_doc = 0;
    QueryElement(QueryID id, char* str) : query_id(id), query_str(str) {}
};

struct Query {
    MatchType match_type;
    unsigned int match_dist;
    std::vector<QueryElement*> query_elements; // for freeing
};

struct Result
{
    DocID doc_id;
    int num_res;
    QueryID* query_ids;
};

struct Document
{
    DocID doc_id;
    char* doc_str;
};

// stores the query elements in a vector
std::vector<QueryElement> queriesExactMatch[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesHammingDist1[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesHammingDist2[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesHammingDist3[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesEditDist1[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesEditDist2[MAX_WORD_LENGTH + 1];
std::vector<QueryElement> queriesEditDist3[MAX_WORD_LENGTH + 1];
// hash map for id to query
std::unordered_map<QueryID, Query> query_map;
// stores the number of matches for each query
std::map<QueryID, int> query_match_count;
// buffers the input documents
std::vector<Document> documents_buffer;
//number of buffered documents
int num_buffered_documents = 0;
// stores results
std::vector<Result> available_results;
// number of available results
int num_available_results = 0;

// Function to create a null-terminated copy of b
char* null_terminated_copy(const char* b, int nb) {
    char* b_copy = new char[nb + 1];
    std::memcpy(b_copy, b, nb);
    b_copy[nb] = '\0';
    return b_copy;
}

ErrorCode InitializeIndex() { return EC_SUCCESS; }

ErrorCode DestroyIndex() {
    // free all query strings in the vectors
    for (int i = 0; i < MAX_WORD_LENGTH; i++) {
        for (auto& query : queriesExactMatch[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesHammingDist1[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesHammingDist2[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesHammingDist3[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesEditDist1[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesEditDist2[i]) {
            free(query.query_str);
        }
        for (auto& query : queriesEditDist3[i]) {
            free(query.query_str);
        }
    }
    return EC_SUCCESS;
}

QueryElement* addToRightVector(MatchType match_type, unsigned int match_dist, const char* query_string, int length, QueryID query_id) {
    char* query_copy = null_terminated_copy(query_string, length);
    switch (match_type) {
    case MT_EXACT_MATCH:
        queriesExactMatch[length].emplace_back(query_id, query_copy);
        return &queriesExactMatch[length].back();
    case MT_HAMMING_DIST:
        switch (match_dist) {
        case 1:
            queriesHammingDist1[length].emplace_back(query_id, query_copy);
            return &queriesHammingDist1[length].back();
        case 2:
            queriesHammingDist2[length].emplace_back(query_id, query_copy);
            return &queriesHammingDist2[length].back();
        case 3:
            queriesHammingDist3[length].emplace_back(query_id, query_copy);
            return &queriesHammingDist3[length].back();
        default:
            // trow error
            throw std::invalid_argument("Invalid match distance for Hamming distance");
        }
    case MT_EDIT_DIST:
        switch (match_dist) {
        case 1:
            queriesEditDist1[length].emplace_back(query_id, query_copy);
            return &queriesEditDist1[length].back();
        case 2:
            queriesEditDist2[length].emplace_back(query_id, query_copy);
            return &queriesEditDist2[length].back();
        case 3:
            queriesEditDist3[length].emplace_back(query_id, query_copy);
            return &queriesEditDist3[length].back();
        default:
            throw std::invalid_argument("Invalid match distance for Edit distance");

        }
    default:
        return nullptr;
    }
}



std::vector<QueryID> exact_match(const char* word, int length) {
    std::vector<QueryID> res;
    for (auto& query : queriesExactMatch[length]) {
        if (strcmp(query.query_str, word) == 0) {
            res.push_back(query.query_id);
        }
    }

    return res;
}

std::vector<QueryID> hamming_match(const char* word, int length, DocID doc_id) {
    std::vector<QueryID> res;

    for (auto& query : queriesHammingDist1[length]) {

        if (query.found_in_doc < doc_id && hamming_distance(query.query_str, word, length, length, 1) <= 1) {
            query.found_in_doc = doc_id;
            res.push_back(query.query_id);

        }
    }

    for (auto& query : queriesHammingDist2[length]) {
        if (query.found_in_doc < doc_id && hamming_distance(query.query_str, word, length, length, 2) <= 2) {
            query.found_in_doc = doc_id;
            res.push_back(query.query_id);
        }
    }

    for (auto& query : queriesHammingDist3[length]) {

        if (query.found_in_doc < doc_id && hamming_distance(query.query_str, word, length, length, 3) <= 3) {
            query.found_in_doc = doc_id;
            res.push_back(query.query_id);
        }
    }
    return res;
}

std::vector<QueryID> edit_match(const char* word, int length, DocID doc_id) {
    std::vector<QueryID> res;
    for (int i = length - 1; i <= length + 1; i++) {
        for (auto& query : queriesEditDist1[i]) {
            if (query.found_in_doc < doc_id && edit_distance(query.query_str, word, i, length, 2) <= 1) {
                query.found_in_doc = doc_id;
                res.push_back(query.query_id);
            }
        }
    }
    for (int i = length - 2; i <= length + 2; i++) {
        for (auto& query : queriesEditDist2[i]) {
            if (query.found_in_doc < doc_id && edit_distance(query.query_str, word, i, length, 3) <= 2) {
                query.found_in_doc = doc_id;
                res.push_back(query.query_id);
            }
        }
    }
    for (int i = length - 3; i <= length - 1; i++) {
        for (auto& query : queriesEditDist3[i]) {
            if (query.found_in_doc < doc_id && edit_distance(word, query.query_str, length, i, 4) <= 3) {
                query.found_in_doc = doc_id;
                res.push_back(query.query_id);
            }
        }
    }
    for (int i = length; i <= length + 3; i++) {
        for (auto& query : queriesEditDist3[i]) {
            if (query.found_in_doc < doc_id && edit_distance(query.query_str, word, i, length, 4) <= 3) {
                query.found_in_doc = doc_id;
                res.push_back(query.query_id);
            }
        }
    }
    return res;
}



ErrorCode MatchDocument(DocID doc_id, const char* doc_str) {
    documents_buffer.push_back({ doc_id, null_terminated_copy(doc_str, strlen(doc_str)) });
    num_buffered_documents++;
    return EC_SUCCESS;
}

void match_document(DocID doc_id, const char* doc_str, std::vector<Result>* results, int* num_results) {
    std::unordered_map<std::string, bool> strings_previously_viewed = std::unordered_map<std::string, bool>();
    Result doc;
    doc.doc_id = doc_id;
    int num_res = 0;
    //iterate over doc_str
    int nb = 0;
    const char* b = doc_str;
    std::vector<QueryID> matched_query_ids;
    std::vector<QueryID> query_ids;
    for (const char* doc_str_iterator = doc_str; *doc_str_iterator; doc_str_iterator++) {
        if (*doc_str_iterator == ' ') {
            nb = doc_str_iterator - b;

            char* ntc = null_terminated_copy(b, nb);

            if (strings_previously_viewed.find(ntc) != strings_previously_viewed.end()) {
                b = doc_str_iterator + 1;
                continue;
            }
            std::string ntc_str = ntc;
            strings_previously_viewed[ntc_str] = true;
            //iterate over queries  
            //add exact matches
            std::vector<QueryID> exact_matches = exact_match(ntc, nb);
            query_ids.insert(query_ids.end(), exact_matches.begin(), exact_matches.end());
            //add hamming matches
            std::vector<QueryID> hamming_matches = hamming_match(ntc, nb, doc_id);
            query_ids.insert(query_ids.end(), hamming_matches.begin(), hamming_matches.end());
            //add edit matches
            std::vector<QueryID> edit_matches = edit_match(ntc, nb, doc_id);
            query_ids.insert(query_ids.end(), edit_matches.begin(), edit_matches.end());
            b = doc_str_iterator + 1;
        }
    }

    std::map<int, int> count_map;
    for (const QueryID& elem : query_ids) {
        count_map[elem]++;
    }

    for (const auto& pair : count_map) {
        if (pair.second >= query_match_count[pair.first]) {
            matched_query_ids.push_back(pair.first);
            num_res++;
        }
    }

    doc.num_res = num_res;
    doc.query_ids = (QueryID*)malloc(num_res * sizeof(QueryID));
    for (int i = 0; i < num_res; i++) {
        doc.query_ids[i] = matched_query_ids[i];
    }
    if (num_res > 0) {
        (*num_results)++;
        (*results).push_back(doc);
    }
}

void match_buffer() {
    for (int i = 0; i < num_buffered_documents; i++) {
        std::vector<Result> results;
        int num_results = 0;
        match_document(documents_buffer[i].doc_id, documents_buffer[i].doc_str, &results, &num_results);
        available_results.insert(available_results.end(), results.begin(), results.end());
        num_available_results += num_results;
        free(documents_buffer[i].doc_str);
    }
    documents_buffer.clear();
    num_buffered_documents = 0;
}

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids) {
    match_buffer();
    // Get the first undeliverd resuilt from "docs" and return it
    *p_doc_id = 0; *p_num_res = 0; *p_query_ids = 0;
    if (num_available_results == 0) return EC_NO_AVAIL_RES;
    *p_doc_id = available_results[num_available_results - 1].doc_id; *p_num_res = available_results[num_available_results - 1].num_res; *p_query_ids = available_results[num_available_results - 1].query_ids;
    available_results.erase(available_results.begin() + num_available_results);
    num_available_results--;
    return EC_SUCCESS;
}

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
    match_buffer();
    const char* c_start = query_str;
    int n_words = 0;
    std::vector<QueryElement*> q_words = std::vector<QueryElement*>(MAX_QUERY_WORDS);
    for (const char* it = query_str; *it; it++) {
        if (*it == ' ') {

            q_words[n_words] = addToRightVector(match_type, match_dist, c_start, it - c_start, query_id);
            n_words++;
            c_start = it + 1;

        }
    }
    // add last word

    q_words[n_words] = addToRightVector(match_type, match_dist, c_start, query_str + strlen(query_str) - c_start, query_id);
    query_map[query_id] = { match_type, match_dist, q_words };
    query_match_count[query_id] = n_words + 1;
    return EC_SUCCESS;
}

ErrorCode EndQuery(QueryID query_id) {
    match_buffer();
    if (query_map.find(query_id) == query_map.end()) {
        return EC_FAIL;
    }
    Query q = query_map[query_id];
    switch (q.match_type)
    {
    case MT_EXACT_MATCH:
        for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
            queriesExactMatch[i].erase(std::remove_if(queriesExactMatch[i].begin(), queriesExactMatch[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesExactMatch[i].end());
        }
        break;
    case MT_HAMMING_DIST:
        switch (q.match_dist)
        {
        case 1:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesHammingDist1[i].erase(std::remove_if(queriesHammingDist1[i].begin(), queriesHammingDist1[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesHammingDist1[i].end());
            }
            break;
        case 2:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesHammingDist2[i].erase(std::remove_if(queriesHammingDist2[i].begin(), queriesHammingDist2[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesHammingDist2[i].end());
            }
            break;
        case 3:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesHammingDist3[i].erase(std::remove_if(queriesHammingDist3[i].begin(), queriesHammingDist3[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesHammingDist3[i].end());
            }
            break;
        default:
            break;
        }
    case MT_EDIT_DIST:
        switch (q.match_dist)
        {
        case 1:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesEditDist1[i].erase(std::remove_if(queriesEditDist1[i].begin(), queriesEditDist1[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesEditDist1[i].end());
            }
            break;
        case 2:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesEditDist2[i].erase(std::remove_if(queriesEditDist2[i].begin(), queriesEditDist2[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesEditDist2[i].end());
            }
            break;
        case 3:
            for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
                queriesEditDist3[i].erase(std::remove_if(queriesEditDist3[i].begin(), queriesEditDist3[i].end(), [query_id](QueryElement& query) {return query.query_id == query_id; }), queriesEditDist3[i].end());
            }
            break;
        default:
            break;
        }
    }

    query_map.erase(query_id);

    return EC_SUCCESS;
}