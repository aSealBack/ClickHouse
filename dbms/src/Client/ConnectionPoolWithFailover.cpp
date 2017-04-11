#include <Client/ConnectionPoolWithFailover.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Common/getFQDNOrHostName.h>

namespace DB
{

ConnectionPoolWithFailover::ConnectionPoolWithFailover(
        ConnectionPools & nested_pools_,
        LoadBalancing load_balancing,
        size_t max_tries_,
        time_t decrease_error_period_)
    : Base(nested_pools_, max_tries_, decrease_error_period_, &Logger::get("ConnectionPoolWithFailover"))
    , default_load_balancing(load_balancing)
{
    const std::string & local_hostname = getFQDNOrHostName();

    hostname_differences.resize(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        ConnectionPool & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i].pool);
        const std::string & host = connection_pool.getHost();

        size_t hostname_difference = 0;
        for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
            if (local_hostname[i] != host[i])
                ++hostname_difference;

        hostname_differences[i] = hostname_difference;
    }
}

ConnectionPoolWithFailover::Base::GetResult ConnectionPoolWithFailover::tryGet(
        const ConnectionPoolPtr & pool, const DB::Settings * settings, std::stringstream & fail_message)
{
    GetResult result;
    try
    {
        result.entry = pool->get(settings);
        result.entry->forceConnected();
        result.is_good = true;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT)
            throw;

        fail_message << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
        result.entry = Entry();
    }
    return result;
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getManyChecked(
        const Settings & settings, PoolMode pool_mode, const QualifiedTableName & table_to_check)
{
    auto try_getter = [&](const ConnectionPoolPtr & pool, const Settings * settings, std::stringstream & fail_message)
    {
        GetResult result;
        try
        {
            result.entry = pool->get(settings);

            /// We take only the lag of the main table behind the Distributed table into account.
            /// TODO: calculate lag for joined tables also.
            TablesStatusRequest status_request;
            status_request.tables = { table_to_check };

            auto status_response = result.entry->getTablesStatus(status_request);
            if (status_response.table_states_by_id.size() != status_request.tables.size())
                throw Exception(
                        "Bad TablesStatus response (from " + result.entry->getDescription() + ")",
                        ErrorCodes::LOGICAL_ERROR);

            UInt32 max_lag = 0;
            for (const auto & kv: status_response.table_states_by_id)
            {
                TableStatus status = kv.second;

                std::cerr << "REPLICA " << result.entry->getDescription() << " TABLE STATUS: " << kv.first.database << "." << kv.first.table << " " << status.is_replicated << " " << status.absolute_delay << std::endl;

                if (status.is_replicated)
                    max_lag = std::max(max_lag, status.absolute_delay);
            }

            if (max_lag < 1) // TODO: take from Settings
                result.is_good = true;
            else
            {
                std::cerr << "Replica " << result.entry->getDescription() << " has unacceptable lag: " << max_lag << std::endl;
                /// TODO: ProfileEvents
                result.is_good = false;
                result.badness = max_lag;
            }
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT)
                throw;

            fail_message << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
            result.entry = Entry();
        }
        return result;
    };

    applyLoadBalancing(&settings);
    return Base::getMany(&settings, pool_mode, try_getter);
}

void ConnectionPoolWithFailover::applyLoadBalancing(const Settings * settings)
{
    LoadBalancing load_balancing = default_load_balancing;
    if (settings)
        load_balancing = settings->load_balancing;

    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        if (load_balancing == LoadBalancing::NEAREST_HOSTNAME)
            nested_pools[i].state.priority = hostname_differences[i];
        else if (load_balancing == LoadBalancing::RANDOM)
            nested_pools[i].state.priority = 0;
        else if (load_balancing == LoadBalancing::IN_ORDER)
            nested_pools[i].state.priority = i;
        else
            throw Exception("Unknown load_balancing_mode: " + toString(static_cast<int>(load_balancing)), ErrorCodes::LOGICAL_ERROR);
    }
}

}
