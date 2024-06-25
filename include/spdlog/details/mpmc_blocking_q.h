// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

// multi producer-multi consumer blocking queue.
// enqueue(..) - will block until room found to put the new message.
// enqueue_nowait(..) - will return immediately with false if no room left in
// the queue.
// dequeue_for(..) - will block until the queue is not empty or timeout have
// passed.

#include <spdlog/details/circular_q.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <iostream>

namespace spdlog {
namespace details {

/*
MIT License

Copyright (c) 2018 Meng Rao <raomeng1@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// PubSubQueue is a single publisher(source) multiple subscriber(receiver) message queue.
// Publisher is not affected(e.g. blocked) by or even aware of subscribers.
// Subscriber is a pure reader, if it's not reading fast enough and falls far behind the publisher
// it'll lose message. PubSubQueue can be zero initialized without calling constructor, which
// facilitates allocating in shared memory It is also "crash safe" which means crash of either
// publisher or subscribers will not corrupt the queue

// alloc and push are not atomic, so SPSCVarQueueOPT should not be used in shared-memory IPC(use
// SPSCVarQueue instead)
template<uint32_t Bytes>
class SPSCVarQueueOPT
{
public:
    struct MsgHeader
    {
        // size of this msg, including header itself
        // auto set by lib, can be read by user
        uint16_t size;
        uint16_t msg_type;
        // userdata can be used by caller, e.g. save timestamp or other stuff
        // we assume that user_msg is 8 types alligned so there'll be 4 bytes padding anyway,
        // otherwise we can choose to eliminate userdata
        uint32_t userdata;
    };
    static constexpr uint32_t BLK_CNT = Bytes / sizeof(MsgHeader);

    MsgHeader *alloc(uint16_t size_)
    {
        size = size_ + sizeof(MsgHeader);
        uint32_t blk_sz = (size + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
        if (blk_sz >= free_write_cnt)
        {
            uint32_t read_idx_cache = *(volatile uint32_t *)&read_idx;
            if (read_idx_cache <= write_idx)
            {
                free_write_cnt = BLK_CNT - write_idx;
                if (blk_sz >= free_write_cnt && read_idx_cache != 0)
                { // wrap around
                    blk[0].size = 0;
                    std::atomic_thread_fence(std::memory_order_release);
                    blk[write_idx].size = 1;
                    write_idx = 0;
                    free_write_cnt = read_idx_cache;
                }
            }
            else
            {
                free_write_cnt = read_idx_cache - write_idx;
            }
            if (free_write_cnt <= blk_sz)
            {
                return nullptr;
            }
        }
        return &blk[write_idx];
    }

    void push()
    {
        uint32_t blk_sz = (size + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
        blk[write_idx + blk_sz].size = 0;
        std::atomic_thread_fence(std::memory_order_release);

        blk[write_idx].size = size;
        write_idx += blk_sz;
        free_write_cnt -= blk_sz;
    }

    template<typename Writer>
    bool tryPush(uint16_t sz, Writer writer)
    {
        MsgHeader *header = alloc(sz);
        if (!header)
            return false;
        writer(header);
        push();
        return true;
    }

    template<typename Writer>
    void blockPush(uint16_t sz, Writer writer)
    {
        while (!tryPush(sz, writer))
            ;
    }

    MsgHeader *front()
    {
        uint16_t sz = blk[read_idx].size;
        if (sz == 1)
        { // wrap around
            read_idx = 0;
            sz = blk[0].size;
        }
        if (sz == 0)
            return nullptr;
        return &blk[read_idx];
    }

    void pop()
    {
        uint32_t blk_sz = (blk[read_idx].size + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
        *(volatile uint32_t *)&read_idx = read_idx + blk_sz;
    }

    template<typename Reader>
    bool tryPop(Reader reader)
    {
        MsgHeader *header = front();
        if (!header)
            return false;
        reader(header);
        pop();
        return true;
    }

private:
    alignas(64) MsgHeader blk[BLK_CNT] = {};

    alignas(128) uint32_t write_idx = 0;
    uint32_t free_write_cnt = BLK_CNT;
    uint16_t size;

    alignas(128) uint32_t read_idx = 0;
};

template<typename T>
class mpmc_blocking_queue
{
public:
    using item_type = T;
    explicit mpmc_blocking_queue(size_t max_items)
        : q_(max_items)
    {}

#ifndef __MINGW32__
    // try to enqueue and block if no room left
    void enqueue(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) { new (header + 1) T(std::forward<T>(item)); });
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) { new (header + 1) T(std::forward<T>(item)); });
    }

    void enqueue_if_have_room(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) { new (header + 1) T(std::forward<T>(item)); });
    }

    // dequeue with a timeout.
    // Return true, if succeeded dequeue item, false otherwise
    bool dequeue_for(T &popped_item, std::chrono::milliseconds wait_duration)
    {
        bool has_update = spsc_q_.tryPop([&popped_item](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
            popped_item = std::move(*const_cast<T *>(reinterpret_cast<const T *>(header + 1)));
        });
        if (!has_update)
            return false;
        return true;
    }

#else
    // apparently mingw deadlocks if the mutex is released before cv.notify_one(),
    // so release the mutex at the very end each function.

    // try to enqueue and block if no room left
    void enqueue(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
            // std::memcpy(header + 1, reinterpret_cast<const uint8_t *>(&item), sizeof(T));
            new (header + 1) T(std::forward<T>(item));
        });
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
            // std::memcpy(header + 1, reinterpret_cast<const uint8_t *>(&item), sizeof(T));
            new (header + 1) T(std::forward<T>(item));
        });
    }

    void enqueue_if_have_room(T &&item)
    {
        spsc_q_.blockPush(sizeof(item), [&](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
            // std::memcpy(header + 1, reinterpret_cast<const uint8_t *>(&item), sizeof(T));
            new (header + 1) T(std::forward<T>(item));
        });
    }

    // dequeue with a timeout.
    // Return true, if succeeded dequeue item, false otherwise
    bool dequeue_for(T &popped_item, std::chrono::milliseconds wait_duration)
    {
        bool has_update = spsc_q_.tryPop([&popped_item](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
            popped_item = std::move(*const_cast<T *>(reinterpret_cast<const T *>(header + 1)));
        });
        if (!has_update)
            return false;
        return true;
    }

    // blocking dequeue without a timeout.
    void dequeue(T &popped_item)
    {
        while (true)
        {
            bool has_update = spsc_q_.tryPop([&popped_item](SPSCVarQueueOPT<QUEUE_SIZE>::MsgHeader *header) {
                popped_item = std::move(*const_cast<T *>(reinterpret_cast<const T *>(header + 1)));
            });
            if (!has_update)
                continue;
            break;
        }
    }

#endif

    size_t overrun_counter()
    {
        // std::unique_lock<std::mutex> lock(queue_mutex_);
        return q_.overrun_counter();
    }

    size_t size()
    {
        // std::unique_lock<std::mutex> lock(queue_mutex_);
        return q_.size();
    }

private:
    static constexpr int QUEUE_SIZE = 16 * 1024 * 1024; // 16MB
    SPSCVarQueueOPT<QUEUE_SIZE> spsc_q_ = SPSCVarQueueOPT<QUEUE_SIZE>();
    std::mutex queue_mutex_;
    std::condition_variable push_cv_;
    std::condition_variable pop_cv_;
    spdlog::details::circular_q<T> q_;
    std::atomic<size_t> discard_counter_{0};
};
} // namespace details
} // namespace spdlog
