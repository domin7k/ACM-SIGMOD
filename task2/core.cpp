#include "../include/core.h"
#include <cstring>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <future>
#include <functional>
#include <algorithm>

using namespace std;

// Hilfsfunktionen
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
    int length = strlen(a) + 1;
    int width = nb + 1;
    vector<vector<int>> matrix(length, vector<int>(width));
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
                int entry = min(matrix[i - 1][j - 1] + (a[i - 1] != b[j - 1]), 
                              matrix[i - 1][j] + 1, 
                              matrix[i][j - 1] + 1);
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
    return matrix[length - 1][width - 1] > maxDist ? maxDist : matrix[length - 1][width - 1];
}

// Datenstrukturen
struct QueryElement {
    QueryID query_id;
    const char* query_str;
    MatchType match_type;
    int match_dist;
};

struct Query {
    QueryID query_id;
    int start;
    int end;
    char* query_str;
};

struct Document {
    DocID doc_id;
    int num_res;
    QueryID* query_ids;
};

// Globale Variablen
vector<QueryElement> queries;
int num_queries = 0;
unordered_map<QueryID, Query> query_map;
vector<Document> documents;
int num_available_results = 0;
mutex results_mutex;

// Basis-Funktionen
ErrorCode InitializeIndex() {
    return EC_SUCCESS;
}

ErrorCode DestroyIndex() {
    for (auto it = query_map.begin(); it != query_map.end(); it++) {
        free(it->second.query_str);
    }
    return EC_SUCCESS;
}

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
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
            num_queries++;

            c_start = it + 1;
            *it = '\0';
        }
    }
    
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

ErrorCode MatchDocument(DocID doc_id, const char* doc_str) {
    const int num_threads = thread::hardware_concurrency();
    vector<thread> threads;
    mutex mtx;
    
    // Dokument in Wörter aufteilen
    vector<pair<const char*, size_t>> words;
    const char* word_start = doc_str;
    
    for (const char* it = doc_str; *it; it++) {
        if (*it == ' ') {
            words.emplace_back(word_start, it - word_start);
            word_start = it + 1;
        }
    }
    if (*word_start) {
        words.emplace_back(word_start, strlen(word_start));
    }

    // Ergebnisstrukturen
    unordered_map<QueryID, int> query_matches;
    vector<const QueryElement*> matched_elements;

    auto process_word = [&](const pair<const char*, size_t>& word) {
        vector<const QueryElement*> local_matches;
        
        for (const auto& query_element : queries) {
            int dist = 4;
            switch (query_element.match_type) {
                case MT_EXACT_MATCH:
                    dist = hamming_distance(query_element.query_str, word.first, word.second) == 0 ? 0 : 4;
                    break;
                case MT_HAMMING_DIST:
                    dist = hamming_distance(query_element.query_str, word.first, word.second);
                    break;
                case MT_EDIT_DIST:
                    dist = edit_distance(query_element.query_str, word.first, word.second, query_element.match_dist + 1);
                    break;
            }
            
            if (dist <= query_element.match_dist) {
                local_matches.push_back(&query_element);
            }
        }

        if (!local_matches.empty()) {
            lock_guard<mutex> lock(mtx);
            for (const auto* match : local_matches) {
                query_matches[match->query_id]++;
                matched_elements.push_back(match);
            }
        }
    };

    // Wörter auf Threads verteilen
    const size_t words_per_thread = (words.size() + num_threads - 1) / num_threads;
    
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * words_per_thread;
        size_t end = min(start + words_per_thread, words.size());
        
        threads.emplace_back([&, start, end]() {
            for (size_t i = start; i < end; ++i) {
                process_word(words[i]);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Ergebnisse sammeln
    vector<QueryID> matched_query_ids;
    for (const auto& count : query_matches) {
        if (count.second == query_map[count.first].end - query_map[count.first].start) {
            matched_query_ids.push_back(count.first);
        }
    }

    // Ergebnisdokument erstellen
    if (!matched_query_ids.empty()) {
        Document doc;
        doc.doc_id = doc_id;
        doc.num_res = matched_query_ids.size();
        doc.query_ids = (QueryID*)malloc(doc.num_res * sizeof(QueryID));
        
        // Wichtig: Sortiere die Query IDs
        sort(matched_query_ids.begin(), matched_query_ids.end());
        copy(matched_query_ids.begin(), matched_query_ids.end(), doc.query_ids);

        lock_guard<mutex> lock(results_mutex);
        documents.insert(documents.begin(), doc);  // Am Anfang einfügen
        num_available_results++;
    }

    return EC_SUCCESS;
}

ErrorCode EndQuery(QueryID query_id) {
    if (query_map.find(query_id) == query_map.end()) {
        return EC_FAIL;
    }
    Query q = query_map[query_id];

    if (!(q.start >= 0 && q.start <= queries.size()) || 
        !(q.end >= 0 && q.end <= queries.size())) {
        return EC_FAIL;
    }

    queries.erase(next(queries.begin(), q.start), next(queries.begin(), q.end));
    
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

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids) {
    lock_guard<mutex> lock(results_mutex);
    
    if (num_available_results == 0) {
        *p_doc_id = 0;
        *p_num_res = 0;
        *p_query_ids = 0;
        return EC_NO_AVAIL_RES;
    }
    
    num_available_results--;
    Document& doc = documents[num_available_results];
    
    *p_doc_id = doc.doc_id;
    *p_num_res = doc.num_res;
    *p_query_ids = doc.query_ids;
    
    documents.pop_back();
    
    return EC_SUCCESS;
}