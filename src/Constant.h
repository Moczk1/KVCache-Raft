#pragma once

namespace mraft
{
    constexpr bool DEBUG = true;
    constexpr int SCALE = 1; // 缩放倍数，默认单位为 millisecond

    constexpr int MIN_HEARTBEAT_INTERVAL = 300 * SCALE;
    constexpr int MAX_HEARTBEAT_INTERVAL = 500 * SCALE;

    constexpr int ELECTION_INTERVAL = 30 * SCALE;
}
