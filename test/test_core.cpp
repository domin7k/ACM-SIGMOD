#include <gtest/gtest.h>
#include "../include/core.h"

TEST(CoreTest, HammingDistanceTest) {
    EXPECT_EQ(0, hamming_distance("0000", "0000111", 4));
    EXPECT_EQ(1, hamming_distance("0000", "0001", 4));
    EXPECT_EQ(2, hamming_distance("0000", "0011", 4));
    EXPECT_EQ(3, hamming_distance("0000", "0111", 4));
    EXPECT_EQ(4, hamming_distance("0000", "1111", 4));
    EXPECT_EQ(4, hamming_distance("0000", "11111", 5));
    EXPECT_EQ(4, hamming_distance("0000", "111", 3));
    EXPECT_EQ(1, hamming_distance("0", "1", 1));
}

TEST(CoreTest, EditDistanceTest) {
    //basic hamming functionality
    EXPECT_EQ(0, edit_distance("0000", "0000", 4, 4));
    EXPECT_EQ(1, edit_distance("0000", "0001", 4, 4));
    EXPECT_EQ(2, edit_distance("0000", "0011", 4, 4));
    EXPECT_EQ(3, edit_distance("0000", "0111", 4, 4));
    EXPECT_EQ(4, edit_distance("0000", "1111", 4, 4));
    EXPECT_EQ(4, edit_distance("0000", "11111", 5, 4));
    EXPECT_EQ(4, edit_distance("0000", "111", 3, 4));
    EXPECT_EQ(1, edit_distance("0", "1", 1, 1));
    //basic edit distance functionality
    EXPECT_EQ(1, edit_distance("0000", "000", 3, 4));
    EXPECT_EQ(2, edit_distance("0000", "001", 3, 4));
    //more complex cases using words
    // more complex cases using words
    EXPECT_EQ(0, edit_distance("hello", "hello", 5, 4));
    EXPECT_EQ(1, edit_distance("helo", "hello", 5, 4));
    EXPECT_EQ(1, edit_distance("hell", "hello", 5, 4));
    EXPECT_EQ(2, edit_distance("hel", "hello", 5, 4));
    EXPECT_EQ(3, edit_distance("he", "hello", 5, 4));
    EXPECT_EQ(4, edit_distance("h", "hello", 5, 4));
    EXPECT_EQ(4, edit_distance("", "hello", 5, 4));
    EXPECT_EQ(1, edit_distance("hello", "hell", 4, 4));
    EXPECT_EQ(2, edit_distance("hello", "hel", 3, 4));
    EXPECT_EQ(3, edit_distance("hello", "he", 2, 4));
    EXPECT_EQ(4, edit_distance("hello", "h", 1, 4));
    EXPECT_EQ(4, edit_distance("hello", "", 0, 4));

    EXPECT_EQ(3, edit_distance("sitting", "kitten", 6, 4));

}

TEST(CoreTest, StartQueryTest) {
    InitializeIndex();
    EXPECT_EQ(EC_SUCCESS, StartQuery(1, "hello world", MT_EXACT_MATCH, 0));
    DestroyIndex();
}


TEST(CoreTest, EndQueryTest) {
    InitializeIndex();
    StartQuery(1, "hello world", MT_EXACT_MATCH, 0);
    StartQuery(2, "foo bar", MT_EXACT_MATCH, 0);
    EXPECT_EQ(EC_SUCCESS, EndQuery(1));
    DestroyIndex();
}

TEST(CoreTest, MatchDocumentTest) {
    InitializeIndex();
    StartQuery(1, "hello world", MT_EXACT_MATCH, 0);
    StartQuery(2, "foo bar", MT_EXACT_MATCH, 0);
    EXPECT_EQ(EC_SUCCESS, MatchDocument(1, "hello world"));
    EXPECT_EQ(EC_SUCCESS, MatchDocument(2, "foo bar"));
    DestroyIndex();
}
