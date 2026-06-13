#pragma once
#include <vector>
#include <memory>
#include <string>

namespace moczkrin
{
    class Clerk
    {
    private:
    std::vector<std::shared_ptr<void>> m_node_collection;
    std::string m_clerk_name;
    int m_lead_id = -1;
    int m_request_id = -1;
    

    public:
        Clerk();
    };

}