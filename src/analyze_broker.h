#pragma once
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>
#include <x/x.h>
#include "coral/coral.h"
#include "feeder/feeder.h"
#include "coral/wal_reader.h"

namespace co {
    using namespace std;
    struct RecordParem {
        string id;
        string order_no; // 只考虑单笔委托和撤单
        int order_type; // 1 order, 2 withdraw
        int result; // 0 faild, 1 success
        int64_t original_send_time;
        int64_t api_send_time;  // 日志的时间
        // int64_t api_rsp_time;
        int64_t broker_rsp_time;  // 日志的时间
        int64_t match_type;
        int64_t api_match_time;
        int64_t broker_match_time;  // 日志的时间
    };

    class AnalyzeBroker {
    public:
        AnalyzeBroker();
        ~AnalyzeBroker();
        void Init(const string& file);
        int64_t GetLogTimeStamp(const string& line);
        void AnalyzeData();
    private:
        std::unordered_map<std::string, RecordParem> all_recode_;  // key is message_id
        // key is order_no, first is order message_id, second is withdraw message_id
        std::unordered_map<std::string, std::pair<std::string, std::string>> all_message_;
    };
}
