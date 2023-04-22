/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

namespace ssx {

template<typename F>
concept ReturnsFutureHolder = requires(F f) {
    { f() } -> std::same_as<ss::future<ss::rwlock::holder>>;
};

// Logs an error if the lock has waited a long time to take.
class logging_rwlock {
public:
    using Clock = ss::lowres_clock;
    using acquisition_id = int64_t;
    static const Clock::duration default_log_time = 10s;
    struct holder {
        logging_rwlock* lock;
        ss::rwlock::holder h;
        acquisition_id id;

        ~holder() {
            vlog(lock->_logger.trace, "Dropping lock ({}) on {}", id, lock->_resource_str_gen());
        }
    };

    explicit logging_rwlock(ss::logger& logger)
      : _logger(logger)
      , _resource_str_gen([this] { return default_resource_str_gen(); }) {}
    using resource_str_func = std::function<ss::sstring()>;
    ss::sstring default_resource_str_gen() const {
        return fmt::format("{}", static_cast<const void*>(&_lock));
    }

    ss::future<holder> read_lock(
      ss::semaphore::time_point take_timeout
      = ss::semaphore::time_point::max(),
      Clock::duration log_timeout = default_log_time) {
        return lock_with_log(
          [this, take_timeout]() { return _lock.hold_read_lock(take_timeout); },
          "Read", log_timeout);
    }

    ss::future<holder> write_lock(
      ss::semaphore::time_point take_timeout
      = ss::semaphore::time_point::max(),
      Clock::duration log_timeout = default_log_time) {
        return lock_with_log(
          [this, take_timeout]() {
              return _lock.hold_write_lock(take_timeout);
          },
          "Write", log_timeout);
    }

    template<typename F>
    requires ReturnsFutureHolder<F> ss::future<holder>
    lock_with_log(F lock_func, const char* acquire_type, Clock::duration log_timeout) {
        ss::timer<Clock> log_timer;
        const auto start_time = ss::lowres_clock::now();
        auto id = _next_id++;
        log_timer.set_callback([id, start_time, acquire_type, this] {
            vlog(
              _logger.error,
              "{} lock ({}) on {} has taken {} so far",
              acquire_type,
              id,
              _resource_str_gen(),
              ss::lowres_clock::now() - start_time);
        });
        log_timer.arm(log_timeout);
        auto stop_timer = ss::defer([&log_timer] { log_timer.cancel(); });
        auto h = co_await lock_func();
        auto acquire_duration = ss::lowres_clock::now() - start_time;
        vlog(
          _logger.trace,
          "{} lock ({}) on {} acquired after {}",
          acquire_type,
          id,
          _resource_str_gen(),
          acquire_duration);
        co_return holder{this, std::move(h), id};
    }

private:
    friend struct holder;

    ss::logger& _logger;
    ss::rwlock _lock;
    const resource_str_func _resource_str_gen;
    acquisition_id _next_id{0};
};

} // namespace ssx
