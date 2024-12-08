#include "../include/core.h"
#include <cstring>


// computes hamming distance between a and b (both null terminated strings). 
// Returns 4 if the Hamming distance
// is greater or equal than 4 or the strings are of different length.
int hamming_distance(const char* a, const char* b){
    const char* it_b = b;
    int dist = 0;
    for (const char* it = a; *it && dist < 4; it++) {
        if (!*it_b) return 4;
        dist += (*it != *it_b) ? 1 : 0;
        it_b++;
    }
    if (*it_b) return 4;
    return dist;
}

int min(int a, int b, int c){
    return a < b ? (a < c ? a : c) : (b < c ? b : c);
}

int edit_distance(const char* a, const char* b, int maxDist){
    std::size_t length = std::strlen(a) + 1;
    size_t width = std::strlen(b) + 1;
    size_t matrix[length][width];
    int minDist;
    for (int i = 0; i < length; i++) {
        minDist = maxDist;
        for (int j = 0; j < width; j++) {
            if (i == 0) {
                matrix[i][j] = j;
            } else if (j == 0) {
                matrix[i][j] = i;
            } else {
                int entry = min(matrix[i-1][j-1] + (a[i-1] != b[j - 1]), matrix[i-1][j] + 1, matrix[i][j-1] + 1);
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
    return matrix[length-1][width-1] > maxDist ? maxDist : matrix[length-1][width-1];
}