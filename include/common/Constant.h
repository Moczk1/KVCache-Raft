#pragma once
#include <iostream>

namespace mraft
{
constexpr bool DEBUG = false;
constexpr bool LOG = false;

constexpr int SCALE = 1; // 缩放倍数，默认单位为 millisecond

constexpr int HEARTBEATTIMEOUT = 25 * SCALE;

constexpr int MIN_ELECTION_INTERVAL = 300 * SCALE;
constexpr int MAX_ELECTION_INTERVAL = 500 * SCALE;

constexpr int HEARTBEAT_INTERVAL = 30 * SCALE;

const int ApplyInterval = 10 * SCALE;     //


const int CONSENSUS_TIMEOUT = 500 * SCALE; // ms

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";
} // namespace mraft
