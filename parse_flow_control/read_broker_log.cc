// PraseLog.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>

// 从行首 [2026-07-27 09:30:13.525] 解析出时间，返回9位 HHMMSSmmm，如 093013525
static std::string extractTime(const std::string& line)
{
    auto start = line.find('[');
    if (start == std::string::npos) return "";
    auto end = line.find(']', start);
    if (end == std::string::npos) return "";

    // 去掉 [-] 内的非数字字符，得到 "20260727093013525"（8位日期 + 9位时间）
    std::string digits;
    for (size_t i = start + 1; i < end; ++i)
    {
        if (line[i] >= '0' && line[i] <= '9')
            digits += line[i];
    }
    if (digits.size() < 9) return "";
    return digits.substr(digits.size() - 9);
}

static int countTradeOrder(const std::string& line)
{
    int count = 0;
    size_t pos = 0;
    while ((pos = line.find("TradeOrder{", pos)) != std::string::npos)
    {
        // 排除 TradeOrderMessage{
        if (pos >= 5 && line.substr(pos - 5, 5) == "Trade")
        {
            // 确认前面是 "TradeOrderMessage{" 中的前缀
            // 实际判断：若前面是 "OrderMessage{" 则跳过
            if (pos >= 12 && line.substr(pos - 12, 12) == "OrderMessage{")
            {
                pos += 10;
                continue;
            }
        }
        ++count;
        pos += 10;
    }
    return count;
}

// 提取 items 中所有 TradeOrder 的 code
static std::vector<std::string> extractAllCodes(const std::string& line)
{
    std::vector<std::string> codes;
    size_t pos = 0;
    while ((pos = line.find("TradeOrder{", pos)) != std::string::npos)
    {
        if (pos >= 12 && line.substr(pos - 12, 12) == "OrderMessage{")
        {
            pos += 10;
            continue;
        }
        // 在 TradeOrder{...} 范围内找 code: "..."
        auto codePos = line.find("code: \"", pos);
        if (codePos != std::string::npos)
        {
            codePos += 7;
            auto codeEnd = line.find('"', codePos);
            if (codeEnd != std::string::npos)
                codes.push_back(line.substr(codePos, codeEnd - codePos));
        }
        pos += 10;
    }
    return codes;
}

// 提取消息 id
static std::string extractMessageId(const std::string& line)
{
    auto pos = line.find("id: \"");
    if (pos == std::string::npos) return "";
    pos += 5;
    auto end = line.find('"', pos);
    if (end == std::string::npos) return "";
    return line.substr(pos, end - pos);
}

// 从 code 中获取市场，如 "513300.SH" -> 1, "000009.SZ" -> 2
static int64_t getMarket(const std::string& code)
{
    if (code.size() >= 3)
    {
        auto suffix = code.substr(code.size() - 3);
        if (suffix == ".SH") return 1;
        if (suffix == ".SZ") return 2;
        if (suffix == ".BJ") return 3;
    }
    return 0;
}

// 从 batch_no "1-3-1004049170031" 中提取中间的编号，如 3
static int extractBatchMid(const std::string& batchNo)
{
    auto p1 = batchNo.find('-');
    if (p1 == std::string::npos) return 0;
    auto p2 = batchNo.find('-', p1 + 1);
    if (p2 == std::string::npos) return 0;
    return std::stoi(batchNo.substr(p1 + 1, p2 - p1 - 1));
}

// 提取 order_no 或 batch_no，优先 order_no
 std::pair<std::string, std::string> extractOrderNo(const std::string& line)
{
    std::string orderNo, batchNo;

    auto pos = line.find("order_no: \"");
    if (pos != std::string::npos)
    {
        pos += 11;
        auto end = line.find('"', pos);
        if (end != std::string::npos)
            orderNo = line.substr(pos, end - pos);
    }

    pos = line.find("batch_no: \"");
    if (pos != std::string::npos)
    {
        pos += 11;
        auto end = line.find('"', pos);
        if (end != std::string::npos)
            batchNo = line.substr(pos, end - pos);
    }

    return { orderNo, batchNo };
}

int main(int argc, char* argv[])
{
    // const char* filePath = "test.log";
    const char* filePath = "../data/huatai_hts_broker_18220727_55.log";
    const char* outPath = "../data/record.csv";

    std::ifstream in(filePath, std::ios::binary);
    if (!in.is_open())
    {
        std::cerr << "无法打开文件: " << filePath << std::endl;
        return 1;
    }

    std::ofstream out(outPath, std::ios::binary);
    if (!out.is_open())
    {
        std::cerr << "无法写入文件: " << outPath << std::endl;
        return 1;
    }

    const char* keywords[] = {
        "send order:",
        "send withdraw:"
    };

    std::string line;
    long long matched = 0;

    while (std::getline(in, line))
    {
        std::string time = extractTime(line);
        if (time.empty())
            continue;

        if (line.find(keywords[0]) != std::string::npos)
        {
            // send order
            std::string msgId = extractMessageId(line);
            std::vector<std::string> codes = extractAllCodes(line);
            for (const auto& code : codes)
            {
                out << time << ",order," << code << "," << getMarket(code) << "," << msgId << std::endl;
            }
            ++matched;
        }
        else if (line.find(keywords[1]) != std::string::npos)
        {
            // send withdraw
            std::string msgId = extractMessageId(line);
            std::pair<std::string, std::string> ob = extractOrderNo(line);
            std::string orderNo = ob.first;
            std::string batchNo = ob.second;
            if (!orderNo.empty()) {
                out << time << ",withdraw," << orderNo << "," << atol(orderNo.c_str()) << "," << msgId << std::endl;
            }
            else {
                int batch_num = extractBatchMid(batchNo);
                for (int i = 0; i < extractBatchMid(batchNo); ++i) {
                    out << time << ",withdraw," << batchNo << "," << atol(batchNo.c_str()) << "," << msgId << std::endl;
                }
            }
            ++matched;
        }
    }
    std::cout << "matched: " << matched << " lines" << std::endl;
    return 0;
}
