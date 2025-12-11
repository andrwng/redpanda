#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_vector.h"
#include "lsm/io/persistence.h"
#include "lsm/lsm.h"
#include "lsm/proto/manifest.proto.h"
#include "model/fundamental.h"

#include <expected>
#include <filesystem>

namespace cloud_topics::l1 {

class stm;

// Leader-only interface for interacting with a replicated LSM database.
//
// There is no concurrency control applied by this class at the row level.
// Callers are expected to coordinate (e.g. via locking) to ensure updates to
// the same rows are performed in the desired order.
class replicated_database {
public:
    enum class errc {
        io_error,
        replication_error,
        not_leader,
        shutting_down,
    };

    // Opens a replicated database for the leader of this term.
    //
    // This method:
    // 1. Syncs the STM to ensure we have the latest state, assuming this is
    //    the first time opening the database as leader in the current term
    // 2. Creates cloud persistence for data and metadata
    // 3. Opens the LSM database using the persisted manifest from the STM
    // 4. Applies any volatile_buffer writes to the database, catching up the
    //    database to the state as of the end of the previous leadership
    static ss::future<std::expected<std::unique_ptr<replicated_database>, errc>>
    open(
      stm* s,
      const std::filesystem::path& staging_directory,
      cloud_io::remote* remote,
      const cloud_storage_clients::bucket_name& bucket,
      ss::abort_source& as);

    replicated_database(replicated_database&&) = default;
    ~replicated_database() = default;
    ss::future<std::expected<void, errc>> close();
    bool needs_reopen() const;

    // Builds a write batch for the given rows, replicates it to the STM, and
    // upon success, applies it to the local database.
    //
    // If a replication error is returned, it's possible that the replication
    // call timed out but the write was still replicated to the STM. To ensure
    // the update actually happened, callers should either retry, or step down
    // as leader to ensure the next leader picks up any potentially timed out
    // writes.
    ss::future<std::expected<void, errc>>
    write(chunked_vector<write_batch_row> rows);

    // Resets the underlying STM to the given manifest.
    // NOTE: once this is called, this database must not be used.
    ss::future<std::expected<void, errc>>
    reset(domain_uuid, std::optional<lsm::proto::manifest> manifest);
    domain_uuid get_domain_uuid() const;

    lsm::database& db() { return db_; }

    ss::future<std::expected<void, errc>> flush();

private:
    replicated_database(
      model::term_id term,
      stm* s,
      lsm::database db,
      std::unique_ptr<lsm::io::data_persistence> data_persist,
      std::unique_ptr<lsm::io::metadata_persistence> meta_persist,
      ss::abort_source& as)
      : term_(term)
      , stm_(s)
      , db_(std::move(db))
      , data_persistence_(std::move(data_persist))
      , metadata_persistence_(std::move(meta_persist))
      , as_(as) {}

    // All replication happens with this term as the invariant.
    const model::term_id term_;

    // Pointer to the STM for replication and state access.
    stm* stm_;

    // The underlying LSM database.
    lsm::database db_;

    // Cloud data persistence that this database owns.
    std::unique_ptr<lsm::io::data_persistence> data_persistence_;

    // Cloud metadata persistence that this database owns.
    std::unique_ptr<lsm::io::metadata_persistence> metadata_persistence_;

    // Abort source for cancellation.
    ss::abort_source& as_;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::replicated_database::errc> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const cloud_topics::l1::replicated_database::errc& k,
      FormatContext& ctx) const {
        switch (k) {
            using enum cloud_topics::l1::replicated_database::errc;
        case io_error:
            return formatter<string_view>::format("io_error", ctx);
        case replication_error:
            return formatter<string_view>::format("replication_error", ctx);
        case not_leader:
            return formatter<string_view>::format("not_leader", ctx);
        case shutting_down:
            return formatter<string_view>::format("shutting_down", ctx);
        }
    }
};
