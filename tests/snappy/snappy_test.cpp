#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <snappy.h>

TEST(SnappyTest, BasicAssertions) {
    std::string input = "Hello, world!";
    std::string compressed;
    std::string decompressed;

    // 压缩数据
    snappy::Compress(input.data(), input.size(), &compressed);

    // 解压缩数据
    snappy::Uncompress(compressed.data(), compressed.size(), &decompressed);

    std::cout << "Original: " << input << std::endl;
    std::cout << "Compressed: " << compressed << std::endl;
    std::cout << "Decompressed: " << decompressed << std::endl;
}
