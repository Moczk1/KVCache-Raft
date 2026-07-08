#pragma once

namespace mraft
{
    constexpr bool DEBUG = true;
    constexpr int SCALE = 1; // 缩放倍数，默认单位为 millisecond

    constexpr int HEARTBEATTIMEOUT = 25 * SCALE;


    constexpr int MIN_ELECTION_INTERVAL = 300 * SCALE;
    constexpr int MAX_ELECTION_INTERVAL = 500 * SCALE;

    constexpr int HEARTBEAT_INTERVAL = 30 * SCALE;

    const int CONSENSUS_TIMEOUT = 500 * SCALE;  // ms
}
