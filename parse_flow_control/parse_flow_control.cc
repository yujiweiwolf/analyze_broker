#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cstdio>

// 按分隔符拆分字符串
static std::vector<std::string> split(const std::string& s, char delim)
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream ss(s);
    while (std::getline(ss, token, delim))
        tokens.push_back(token);
    return tokens;
}

// HHMMSSmmm -> 当天毫秒数
static int64_t toMillis(const std::string& t)
{
    if (t.size() < 9) return -1;
    int hh = std::stoi(t.substr(0, 2));
    int mm = std::stoi(t.substr(2, 2));
    int ss = std::stoi(t.substr(4, 2));
    int ms = std::stoi(t.substr(6, 3));
    return (int64_t)hh * 3600000 + mm * 60000 + ss * 1000 + ms;
}

// 当天毫秒数 -> HHMMSSmmm
static std::string toTimeStr(int64_t ms)
{
    int hh = ms / 3600000; ms %= 3600000;
    int mm = ms / 60000; ms %= 60000;
    int ss = ms / 1000; ms %= 1000;
    char buf[16];
    snprintf(buf, sizeof(buf), "%02d%02d%02d%03d", hh, mm, ss, (int)ms);
    return buf;
}

int main()
{
    const char* filePath = "../data/record.csv";

    std::ifstream in(filePath);
    if (!in.is_open())
    {
        std::cerr << "无法打开文件: " << filePath << std::endl;
        return 1;
    }

    // 按交易所分组收集时间戳(毫秒)
    std::map<int, std::vector<int64_t>> marketTimes;

    std::string line;
    while (std::getline(in, line))
    {
        auto cols = split(line, ',');
        if (cols.size() < 4) continue;

        int market = std::atoi(cols[3].c_str());
        if (market != 1 && market != 2 && market != 3) continue;

        int64_t ms = toMillis(cols[0]);
        if (ms < 0) continue;

        marketTimes[market].push_back(ms);
    }

    // ========== 1. 固定1秒(秒级)统计 ==========
    // key: "HHMMSS,market" -> count
    std::map<std::string, int> secStats;
    for (auto& [market, times] : marketTimes)
    {
        for (auto t : times)
        {
            std::string sec = toTimeStr(t).substr(0, 6);
            secStats[sec + "," + std::to_string(market)]++;
        }
    }

    // 按 count 降序排序，取前10
    std::vector<std::pair<std::string, int>> secSorted(secStats.begin(), secStats.end());
    std::sort(secSorted.begin(), secSorted.end(),
        [](const auto& a, const auto& b) { return a.second > b.second; });

    std::cout << "=== Per-second top 10 (second,exchange,count) ===" << std::endl;
    for (size_t i = 0; i < secSorted.size() && i < 100; ++i)
    {
        std::cout << secSorted[i].first << "," << secSorted[i].second << std::endl;
    }
    std::cout << "---------------------------------------------------------------" << std::endl;

    // ========== 2. 滑动窗口1秒统计 ==========
    std::vector<std::tuple<int64_t, int, int>> results;

    for (auto& [market, times] : marketTimes)
    {
        std::sort(times.begin(), times.end());

        size_t j = 0;
        for (size_t i = 0; i < times.size(); ++i)
        {
            if (i > 0 && times[i] == times[i - 1]) continue;
            if (j < i) j = i;
            while (j < times.size() && times[j] < times[i] + 1000)
                ++j;
            int count = (int)(j - i);
            results.push_back({times[i], market, count});
        }
    }

    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) { return std::get<2>(a) > std::get<2>(b); });

    std::cout << "\n=== Sliding 1s window top 100 (time,exchange,count) ===" << std::endl;
    for (size_t i = 0; i < results.size() && i < 100; ++i)
    {
        std::cout << toTimeStr(std::get<0>(results[i])) << ","
                  << std::get<1>(results[i]) << ","
                  << std::get<2>(results[i]) << std::endl;
    }

    return 0;
}
