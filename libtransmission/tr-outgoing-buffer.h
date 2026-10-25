// Outgoing byte queue: existing data never moves; bulk chunks are 256 KiB.
#pragma once
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

namespace libtransmission
{
class ChunkedOutgoingBuffer
{
    struct Chunk
    {
        std::unique_ptr<std::byte[]> bytes;
        size_t capacity = 0, begin = 0, end = 0;
    };
public:
    explicit ChunkedOutgoingBuffer(size_t chunk_size = 256U * 1024U) : chunk_size_{chunk_size} { assert(chunk_size != 0); }
    ChunkedOutgoingBuffer(ChunkedOutgoingBuffer const&) = delete;
    ChunkedOutgoingBuffer& operator=(ChunkedOutgoingBuffer const&) = delete;
    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    std::byte const* data() const noexcept { return empty() ? nullptr : chunks_.front().bytes.get()+chunks_.front().begin; }
    size_t contiguous_size() const noexcept { return empty() ? 0 : chunks_.front().end-chunks_.front().begin; }
    std::pair<std::byte*,size_t> reserve_space(size_t n)
    {
        if (n == 0) return {nullptr, 0};
        if (chunks_.empty() || chunks_.back().capacity-chunks_.back().end < n)
        {
            auto it = std::find_if(spares_.begin(), spares_.end(), [n](auto const& c) { return c.capacity >= n; });
            Chunk next;
            if (it != spares_.end())
            {
                next = std::move(*it);
                spares_.erase(it);
            }
            else
            {
                // Handshake-only peers should not allocate a full bulk chunk.
                next.capacity = std::max(initial_allocation_ ? std::min(chunk_size_,size_t{16400}) : chunk_size_, n);
                next.bytes.reset(new std::byte[next.capacity]);
                initial_allocation_ = false;
            }
            chunks_.push_back(std::move(next));
        }
        auto& c = chunks_.back();
        return {c.bytes.get()+c.end, n};
    }
    void commit_space(size_t n)
    {
        if (n == 0) return;
        assert(!chunks_.empty() && n <= chunks_.back().capacity-chunks_.back().end);
        chunks_.back().end += n;
        size_ += n;
    }
    void drain(size_t n)
    {
        n = std::min(n, size_);
        while (n != 0)
        {
            auto& c = chunks_.front();
            auto const used = std::min(n, c.end-c.begin);
            c.begin += used;
            size_ -= used;
            n -= used;
            if (c.begin == c.end)
            {
                Chunk done = std::move(c);
                chunks_.pop_front();
                done.begin = done.end = 0;
                // Retain the initial small chunk too, so handshake/keepalive
                // peers can keep reusing 16 KiB instead of allocating a bulk chunk.
                if (spares_.size() < 2 && done.capacity <= 4*chunk_size_)
                    spares_.push_back(std::move(done));
            }
        }
    }
    void clear() { drain(size_); }
private:
    size_t const chunk_size_;
    size_t size_=0;
    bool initial_allocation_=true;
    std::deque<Chunk> chunks_;
    std::vector<Chunk> spares_;
};
}
