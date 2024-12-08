#include <gtest/gtest.h>
#include "../include/core.h"

TEST(CoreTest, HammingDistanceTest){
    EXPECT_EQ(0, hamming_distance("0000", "0000"));
    EXPECT_EQ(1, hamming_distance("0000", "0001"));
    EXPECT_EQ(2, hamming_distance("0000", "0011"));
    EXPECT_EQ(3, hamming_distance("0000", "0111"));
    EXPECT_EQ(4, hamming_distance("0000", "1111"));
    EXPECT_EQ(4, hamming_distance("0000", "11111"));
    EXPECT_EQ(4, hamming_distance("0000", "111"));
    EXPECT_EQ(1, hamming_distance("0", "1"));
}

TEST(CoreTest, EditDistanceTest){
    //basic hamming functionality
    EXPECT_EQ(0, edit_distance("0000", "0000", 4));
    EXPECT_EQ(1, edit_distance("0000", "0001", 4));
    EXPECT_EQ(2, edit_distance("0000", "0011", 4));
    EXPECT_EQ(3, edit_distance("0000", "0111", 4));
    EXPECT_EQ(4, edit_distance("0000", "1111", 4));
    EXPECT_EQ(4, edit_distance("0000", "11111", 4));
    EXPECT_EQ(4, edit_distance("0000", "111", 4));
    EXPECT_EQ(1, edit_distance("0", "1", 1));
    //basic edit distance functionality
    EXPECT_EQ(1, edit_distance("0000", "000", 4));
    EXPECT_EQ(2, edit_distance("0000", "001", 4));
    //more complex cases using words
    // more complex cases using words
    EXPECT_EQ(0, edit_distance("hello", "hello", 4));
    EXPECT_EQ(1, edit_distance("helo", "hello", 4));
    EXPECT_EQ(1, edit_distance("hell", "hello", 4));
    EXPECT_EQ(2, edit_distance("hel", "hello", 4));
    EXPECT_EQ(3, edit_distance("he", "hello", 4));
    EXPECT_EQ(4, edit_distance("h", "hello", 4));
    EXPECT_EQ(4, edit_distance("", "hello", 4));
    EXPECT_EQ(1, edit_distance("hello", "hell", 4));
    EXPECT_EQ(2, edit_distance("hello", "hel", 4));
    EXPECT_EQ(3, edit_distance("hello", "he", 4));
    EXPECT_EQ(4, edit_distance("hello", "h", 4));
    EXPECT_EQ(4, edit_distance("hello", "", 4));

    EXPECT_EQ(3, edit_distance("sitting", "kitten", 4));
    
}