#pragma once

#include <unordered_set>
#include <unordered_map>

#include <Core/Types.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

struct TableStatus
{
    bool is_replicated;
    UInt32 absolute_delay;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

struct TablesStatusRequest
{
    std::unordered_set<QualifiedTableName> tables;

    void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 client_protocol_revision);
};

struct TablesStatusResponse
{
    std::unordered_map<QualifiedTableName, TableStatus> table_states_by_id;

    void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
    void read(ReadBuffer & in, UInt64 server_protocol_revision);
};

}
