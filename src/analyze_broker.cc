#include "analyze_broker.h"
#include <regex>


namespace co {
    AnalyzeBroker::AnalyzeBroker() {
    }

    AnalyzeBroker::~AnalyzeBroker() {
    }

    void AnalyzeBroker::Init(const string& file) {
        std::fstream infile;
        infile.open(file, std::ios::in);
        if (!infile.is_open()) {
            std::cout << "open file " << file << " failed! " << std::endl;
            return;
        }
        std::cout << "open file " << file << std::endl;
        string fund_id, node_id, node_name, trade_day;
        std::string line;
        while (std::getline(infile, line)) {
            if (line.length() < 10) {
                continue;
            }
            // LOG_INFO << line;
            if (fund_id.empty()) {
                auto it = line.find("fund_id:");
                size_t start_index = 0;
                size_t end_index = 0;
                if (it != line.npos) {
                    for (size_t i = it + 7; i != line.npos; ++i) {
                        char temp = line[i];
                        if (temp == '\"') {
                            if (start_index == 0) {
                                start_index = i;
                            }  else {
                                end_index = i;
                                break;
                            }
                        }
                    }
                    fund_id = line.substr(start_index + 1, end_index - start_index - 1);
                }
            }
            if (node_id.empty()) {
                auto it = line.find("node_id");
                if (it != line.npos) {
                    trade_day = line.substr(1, 10);
                    size_t start_index = it + 10;
                    size_t end_index = 0;
                    for (size_t i = it + 10; i != line.npos; ++i) {
                        char temp = line[i];
                        if (temp == ',') {
                            end_index = i;
                            break;
                        }
                    }
                    node_id = line.substr(start_index, end_index - start_index);
                    node_name = line.substr(end_index + 14);
                    std::cout << "broker统计信息， 帐号: " << fund_id << ", 交易日期: " << trade_day << ", node_name: " <<  node_name << std::endl;
                }
            }
            auto it = line.find("TradeOrderMessage");
            if (it != line.npos) {
                size_t start_index = it + strlen("TradeOrderMessage{id: \"");
                size_t end_index = 0;
                for (size_t i = it + 10; i != line.npos; ++i) {
                    char temp = line[i];
                    if (temp == ',') {
                        end_index = i;
                        break;
                    }
                }
                string id = line.substr(start_index, end_index - start_index - 1);
                start_index = end_index + 13;
                string timestamp = line.substr(start_index, 17);
                it = line.find("send order:");
                if (it != line.npos) {
                    RecordParem param;
                    memset(&param, 0, sizeof(param));
                    param.id = id;
                    param.original_send_time = atoll(timestamp.c_str());
                    param.api_send_time = GetLogTimeStamp(line);
                    param.order_type = 1;
                    all_recode_.insert(std::make_pair(id, param));
                } else {
                    it = line.find("send order ok");
                    if (it != line.npos) {
                        if(auto itor = all_recode_.find(id); itor != all_recode_.end()) {
                            itor->second.result = 1;
                            itor->second.broker_rsp_time = GetLogTimeStamp(line);
                            string key = ", order_no: ";
                            if (it = line.find(key); it != line.npos) {
                                it += (key.length() + 1);
                                size_t end_index = 0;
                                for (size_t i = it; i != line.npos; ++i) {
                                    if (line[i] == '\"') {
                                        end_index = i;
                                        break;
                                    }
                                }
                                string order_no = line.substr(it, end_index - it);
                                itor->second.order_no = order_no;
                                all_message_.insert(std::make_pair(order_no, std::make_pair(id, "")));
                            }
                        }
                    } else {
                        if(auto itor = all_recode_.find(id); itor != all_recode_.end()) {
                            itor->second.result = 0;
                            itor->second.broker_rsp_time = GetLogTimeStamp(line);
                        }
                    }
                }
            } else {
                it = line.find("TradeWithdrawMessage");
                if (it != line.npos) {
                    size_t start_index = it + strlen("TradeWithdrawMessage{id: \"");
                    size_t end_index = 0;
                    for (size_t i = it + 10; i != line.npos; ++i) {
                        char temp = line[i];
                        if (temp == ',') {
                            end_index = i;
                            break;
                        }
                    }
                    string id = line.substr(start_index, end_index - start_index - 1);
                    start_index = end_index + 13;
                    string timestamp = line.substr(start_index, 17);
                    it = line.find("send withdraw:");
                    if (it != line.npos) {
                        RecordParem param;
                        memset(&param, 0, sizeof(param));
                        param.id = id;
                        param.original_send_time = atoll(timestamp.c_str());
                        param.api_send_time = GetLogTimeStamp(line);
                        param.order_type = 2;
                        string key = ", order_no: ";
                        if (it = line.find(key); it != line.npos) {
                            it += (key.length() + 1);
                            size_t end_index = 0;
                            for (size_t i = it; i != line.npos; ++i) {
                                if (line[i] == '\"') {
                                    end_index = i;
                                    break;
                                }
                            }
                            string order_no = line.substr(it, end_index - it);
                            if (!order_no.empty()) {
                                param.order_no = order_no;
                                if (auto itor = all_message_.find(order_no); itor != all_message_.end()) {
                                    itor->second.second = id;
                                }
                                all_recode_.insert(std::make_pair(id, param));
                            }
                        }
                    } else {
                        it = line.find("send withdraw ok");
                        if (it != line.npos) {
                            if(auto itor = all_recode_.find(id); itor != all_recode_.end()) {
                                itor->second.result = 1;
                                itor->second.broker_rsp_time = GetLogTimeStamp(line);
                            }
                        } else {
                            if(auto itor = all_recode_.find(id); itor != all_recode_.end()) {
                                itor->second.result = 0;
                                itor->second.broker_rsp_time = GetLogTimeStamp(line);
                            }
                        }
                    }
                } else {
                    it = line.find("KNOCK");
                    if (it != line.npos) {
                        std::regex reg("(.*)timestamp: (.*), trade_type: (.*), fund_id: (.*), username: (.*), inner_match_no: (.*), match_no: (.*), market: (.*), code: (.*), order_no: (.*), batch_no: (.*), bs_flag: (.*), oc_flag: (.*), match_type: (.*), match_volume: (.*)");
                        std::cmatch m;
                        auto ret = std::regex_search(line.c_str(), m, reg);
                        if (ret) {
                            string timestamp = m[2].str();
                            string order_no = m[10].str();
                            order_no = order_no.substr(1, order_no.length() - 2);
                            string match_type = m[14].str();
                            if (auto itor = all_message_.find(order_no); itor != all_message_.end()) {
                                std::string message_id;
                                if (match_type[0] == '1' || match_type[0] == '3') {
                                    message_id = itor->second.first;
                                } else {
                                    message_id = itor->second.second;
                                }
                                if (!message_id.empty()) {
                                    if (auto iter = all_recode_.find(message_id); iter != all_recode_.end()) {
                                        int64_t stamp = iter->second.api_send_time % 1000000000LL;
                                        if (stamp >= 93000000 && stamp <= 145700000) {
                                            iter->second.match_type = atol(match_type.c_str());
                                            iter->second.api_match_time = atol(timestamp.c_str());
                                            iter->second.broker_match_time = GetLogTimeStamp(line);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        infile.close();
        AnalyzeData();
    }

    int64_t AnalyzeBroker::GetLogTimeStamp(const string& line) {
        char temp[32] = "";
        int index = 0;
        for (auto i = 0; i < line.length(); i++) {
            if (line[i] >= '0' && line[i] <= '9') {
                temp[index++] = line[i];
            } else if (line[i] == ']') {
                break;
            }
        }
        return atoll(temp);
    }

    void ParseVector(std::vector<int64_t>& all_diff) {
        if (all_diff.empty()) {
            std::cout << "size: " << 0 << ", avg_value: " << 0
                      << ", mid_value: " << 0
                      << ", min_value: " << 0
                      << ", max_value: " << 0 << std::endl;
        } else {
            std::sort(all_diff.begin(), all_diff.end(), [](int64_t& a, int64_t& b) {
                return a < b;
            });
            int64_t sum_amout = 0;
            for (auto& it : all_diff) {
                sum_amout += it;
            }
            double avg_value = sum_amout * 1.0 / all_diff.size();
            std::cout << "size: " << all_diff.size() << ", avg_value: " << avg_value
                      << ", mid_value: " << all_diff[all_diff.size() / 2]
                      << ", min_value: " << all_diff.front()
                      << ", max_value: " << all_diff.back() << std::endl;
        }
    }

    void AnalyzeBroker::AnalyzeData() {
        std::vector<RecordParem> all_recode;
        for (auto it = all_recode_.begin(); it != all_recode_.end(); ++it) {
            all_recode.push_back(it->second);
        }
        std::sort(all_recode.begin(), all_recode.end(), [](RecordParem& a, RecordParem& b) {
            return a.api_send_time < b.api_send_time;
        });
        for (auto& record : all_recode) {
            std::string name;
            if (record.order_type == 1 && record.result == 1) {
                name = "报单成功";
            } else if (record.order_type == 1 && record.result == 0) {
                name = "报单失败";
            } else if (record.order_type == 2 && record.result == 1) {
                name = "撤单成功";
            } else if (record.order_type == 2 && record.result == 0) {
                name = "撤单失败";
            }
            std::cout << "message_id: " << record.id << ", " << name << ", order_no: " << record.order_no
                            << ", 原始发送时间: " << record.original_send_time
                            << ", api发送时间: " << record.api_send_time
                            << ", api响应时间: " << record.broker_rsp_time
                            << ", api响应消耗时间: " << x::SubRawDateTime(record.broker_rsp_time, record.api_send_time)
                            << ", match_type: " << record.match_type
                            << ", api的match_time: " << record.api_match_time
                            << ", 机器收到成交推送的时间: " << record.broker_match_time
                            << ", 从send到收到成交推送的时间间隔: " << (record.broker_match_time > 0 ? x::SubRawDateTime(record.broker_match_time, record.api_send_time) : 0)
                            << std::endl;
        }

        std::vector<int64_t> all_diff;

        {
            std::cout << "\n报单成功的响应时间统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.order_type == 1 && it.result == 1) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_rsp_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }

        {
            std::cout << "\n报单失败的响应时间统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.order_type == 1 && it.result == 0) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_rsp_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }


        {
            std::cout << "\n撤单成功的响应时间统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.order_type == 2 && it.result == 1) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_rsp_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }

        {
            std::cout << "\n撤单失败的响应时间统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.order_type == 2 && it.result == 0) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_rsp_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }

        ////////////////////////////////////////////////////////////
        {
            std::cout << "\n普通成交推送, 从api发送指令到机器收到成交数据的时间间隔统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.match_type == 1) {
                    int64_t stamp = it.api_send_time % 1000000000LL;
                     if (stamp >= 93000000 && stamp <= 145700000) {
                        all_diff.push_back(x::SubRawDateTime(it.broker_match_time, it.api_send_time));
                     }
                }
            }
            ParseVector(all_diff);
        }

        {
            std::cout << "\n撤单成交推送, 从api发送指令到机器收到成交数据的时间间隔统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.match_type == 2) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_match_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }

        {
            std::cout << "\n废单成交推送, 从api发送指令到机器收到成交数据的时间间隔统计" << std::endl;
            all_diff.clear();
            for (auto& it : all_recode) {
                if (it.match_type == 3) {
                    all_diff.push_back(x::SubRawDateTime(it.broker_match_time, it.api_send_time));
                }
            }
            ParseVector(all_diff);
        }
    }
}
