#include <iostream>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <cstdint>
#include <algorithm>
#include <chrono>

// ============================================================================
// 1. IN-MEMORY ORDER EVENT MESSAGE
// ============================================================================
enum class MsgType : uint8_t { ADD = 'A', CANCEL = 'X' };
enum class Side    : uint8_t { BUY = 'B', SELL   = 'S' };

// Fixed-size 24-byte in-memory struct passed through the SPSC queue.
// Uses integer ticks for price ($100.25 -> 10025) to avoid floating-point precision issues.
struct OrderEvent {
    MsgType  type     = MsgType::ADD;
    Side     side     = Side::BUY;
    uint32_t price    = 0;
    uint32_t quantity = 0;
    uint64_t order_id = 0;
};

// ============================================================================
// 2. CACHE-LINE ALIGNED LOCK-FREE SPSC RING BUFFER
// ============================================================================
// Design Notes:
// - Single producer modifies head_; single consumer modifies tail_.
// - Power-of-2 capacity enables fast bitwise modulo '& (Capacity - 1)'.
// - 64-byte cache-line alignment prevents false sharing between threads.
template <typename T, size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");

private:
    alignas(64) std::atomic<size_t> head_{0}; // Producer index
    alignas(64) std::atomic<size_t> tail_{0}; // Consumer index
    alignas(64) std::array<T, Capacity> buffer_{};

public:
    bool push(const T& item) noexcept {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t next_head = (current_head + 1) & (Capacity - 1);

        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false; // Buffer full
        }

        buffer_[current_head] = item;
        head_.store(next_head, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);

        if (current_tail == head_.load(std::memory_order_acquire)) {
            return false; // Buffer empty
        }

        item = buffer_[current_tail];
        tail_.store((current_tail + 1) & (Capacity - 1), std::memory_order_release);
        return true;
    }
};

// ============================================================================
// 3. INTRUSIVE ORDER NODE & PRE-ALLOCATED OBJECT POOL
// ============================================================================
struct Order {
    uint64_t order_id = 0;
    uint32_t price    = 0;
    uint32_t quantity = 0;
    Side     side     = Side::BUY;

    // Intrusive doubly linked list pointers to avoid dynamic node allocations.
    Order* prev = nullptr;
    Order* next = nullptr;
};

class OrderPool {
private:
    std::vector<Order> pool_; // Contiguous memory block allocated at initialization
    Order* free_head_ = nullptr;

public:
    explicit OrderPool(size_t max_orders) : pool_(max_orders) {
        for (size_t i = 0; i < max_orders - 1; ++i) {
            pool_[i].next = &pool_[i + 1];
        }
        pool_[max_orders - 1].next = nullptr;
        free_head_ = &pool_[0];
    }

    Order* allocate() noexcept {
        if (!free_head_) return nullptr;
        Order* order = free_head_;
        free_head_ = free_head_->next;
        order->prev = nullptr;
        order->next = nullptr;
        return order;
    }

    void deallocate(Order* order) noexcept {
        order->next = free_head_;
        free_head_ = order;
    }
};

// ============================================================================
// 4. INTRUSIVE DOUBLY LINKED LIST FOR A SINGLE PRICE LEVEL (FIFO Queue)
// ============================================================================
struct PriceLevel {
    Order*   head = nullptr;
    Order*   tail = nullptr;
    uint64_t total_volume = 0;

    // O(1) append to maintain time priority
    void push_back(Order* order) noexcept {
        order->prev = tail;
        order->next = nullptr;
        if (tail) {
            tail->next = order;
        } else {
            head = order;
        }
        tail = order;
        total_volume += order->quantity;
    }

    // O(1) unlink from any position in the queue
    void remove(Order* order) noexcept {
        if (order->prev) {
            order->prev->next = order->next;
        } else {
            head = order->next;
        }

        if (order->next) {
            order->next->prev = order->prev;
        } else {
            tail = order->prev;
        }

        order->prev = nullptr;
        order->next = nullptr;
        total_volume -= order->quantity;
    }

    bool empty() const noexcept { return head == nullptr; }
};

// ============================================================================
// 5. DETERMINISTIC LIMIT ORDER BOOK & MATCHING ENGINE
// ============================================================================
class LimitOrderBook {
private:
    static constexpr uint32_t MAX_PRICE_TICKS = 100000;
    static constexpr size_t   MAX_ORDERS      = 1000000;

    OrderPool pool_;
    std::vector<PriceLevel> bids_;         // Direct O(1) array indexed by price tick
    std::vector<PriceLevel> asks_;         // Direct O(1) array indexed by price tick
    std::vector<Order*>     order_lookup_; // Direct O(1) lookup by order_id

    uint32_t best_bid_ = 0;
    uint32_t best_ask_ = MAX_PRICE_TICKS;

public:
    uint64_t total_trades_executed = 0;
    uint64_t total_shares_traded   = 0;

    LimitOrderBook()
        : pool_(MAX_ORDERS),
          bids_(MAX_PRICE_TICKS),
          asks_(MAX_PRICE_TICKS),
          order_lookup_(MAX_ORDERS, nullptr) {}

    void add_order(uint64_t order_id, Side side, uint32_t price, uint32_t quantity) noexcept {
        if (side == Side::BUY) {
            // 1. Aggressive Buy: match against lowest resting Asks if price crosses the spread
            while (quantity > 0 && best_ask_ <= price && best_ask_ < MAX_PRICE_TICKS) {
                PriceLevel& ask_level = asks_[best_ask_];
                Order* resting = ask_level.head;

                while (resting && quantity > 0) {
                    const uint32_t traded_qty = std::min(quantity, resting->quantity);
                    quantity               -= traded_qty;
                    resting->quantity      -= traded_qty;
                    ask_level.total_volume -= traded_qty;

                    total_trades_executed++;
                    total_shares_traded += traded_qty;

                    Order* next_resting = resting->next;
                    if (resting->quantity == 0) {
                        ask_level.remove(resting);
                        order_lookup_[resting->order_id] = nullptr;
                        pool_.deallocate(resting);
                    }
                    resting = next_resting;
                }

                while (best_ask_ < MAX_PRICE_TICKS && asks_[best_ask_].empty()) {
                    best_ask_++;
                }
            }

            // 2. Rest any remaining quantity in the Bid book
            if (quantity > 0) {
                Order* order    = pool_.allocate();
                order->order_id = order_id;
                order->side     = Side::BUY;
                order->price    = price;
                order->quantity = quantity;

                bids_[price].push_back(order);
                order_lookup_[order_id] = order;
                if (price > best_bid_) best_bid_ = price;
            }
        } else {
            // 1. Aggressive Sell: match against highest resting Bids if price crosses the spread
            while (quantity > 0 && best_bid_ >= price && best_bid_ > 0) {
                PriceLevel& bid_level = bids_[best_bid_];
                Order* resting = bid_level.head;

                while (resting && quantity > 0) {
                    const uint32_t traded_qty = std::min(quantity, resting->quantity);
                    quantity               -= traded_qty;
                    resting->quantity      -= traded_qty;
                    bid_level.total_volume -= traded_qty;

                    total_trades_executed++;
                    total_shares_traded += traded_qty;

                    Order* next_resting = resting->next;
                    if (resting->quantity == 0) {
                        bid_level.remove(resting);
                        order_lookup_[resting->order_id] = nullptr;
                        pool_.deallocate(resting);
                    }
                    resting = next_resting;
                }

                while (best_bid_ > 0 && bids_[best_bid_].empty()) {
                    best_bid_--;
                }
            }

            // 2. Rest any remaining quantity in the Ask book
            if (quantity > 0) {
                Order* order    = pool_.allocate();
                order->order_id = order_id;
                order->side     = Side::SELL;
                order->price    = price;
                order->quantity = quantity;

                asks_[price].push_back(order);
                order_lookup_[order_id] = order;
                if (price < best_ask_) best_ask_ = price;
            }
        }
    }

    // O(1) Order Cancellation by Order ID
    void cancel_order(uint64_t order_id) noexcept {
        Order* order = order_lookup_[order_id];
        if (!order) return;

        const uint32_t price = order->price;
        if (order->side == Side::BUY) {
            bids_[price].remove(order);
            while (best_bid_ > 0 && bids_[best_bid_].empty()) {
                best_bid_--;
            }
        } else {
            asks_[price].remove(order);
            while (best_ask_ < MAX_PRICE_TICKS && asks_[best_ask_].empty()) {
                best_ask_++;
            }
        }

        order_lookup_[order_id] = nullptr;
        pool_.deallocate(order);
    }

    uint32_t get_best_bid() const noexcept { return best_bid_; }
    uint32_t get_best_ask() const noexcept { return best_ask_; }
};

// ============================================================================
// 6. TWO-THREAD SPSC REPLAY & LATENCY BENCHMARK
// ============================================================================
int main() {
    LimitOrderBook book;
    SPSCRingBuffer<OrderEvent, 1024> event_queue;
    std::atomic<bool> producer_done{false};

    // Synthetic workload of order events
    const std::vector<OrderEvent> workload = {
        {MsgType::ADD,    Side::BUY,  10000, 50,  1}, // Order #1: Buy 50 @ $100.00
        {MsgType::ADD,    Side::BUY,  10000, 100, 2}, // Order #2: Buy 100 @ $100.00
        {MsgType::ADD,    Side::SELL, 10050, 200, 3}, // Order #3: Sell 200 @ $100.50
        {MsgType::CANCEL, Side::BUY,  0,     0,   1}, // Cancel Order #1
        {MsgType::ADD,    Side::SELL, 10000, 120, 4}  // Order #4: Aggressive Sell 120 @ $100.00
    };

    // Producer Thread: Dispatches order events into the SPSC ring buffer
    std::thread producer([&]() {
        for (const auto& ev : workload) {
            while (!event_queue.push(ev)) {
                // Spin-wait on full queue
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    // Consumer Thread: Deterministic matching engine loop
    size_t processed = 0;
    auto start_time = std::chrono::steady_clock::now();

    OrderEvent ev;
    while (!producer_done.load(std::memory_order_acquire) || event_queue.pop(ev)) {
        if (processed < workload.size() && (ev.order_id != 0 || event_queue.pop(ev))) {
            if (ev.type == MsgType::ADD) {
                book.add_order(ev.order_id, ev.side, ev.price, ev.quantity);
            } else if (ev.type == MsgType::CANCEL) {
                book.cancel_order(ev.order_id);
            }
            processed++;
            ev.order_id = 0;
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    producer.join();

    auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

    std::cout << "--- Matching Engine Execution Summary ---\n";
    std::cout << "Events Processed   : " << processed << "\n";
    std::cout << "Trades Executed    : " << book.total_trades_executed << "\n";
    std::cout << "Shares Traded      : " << book.total_shares_traded << " (Expected: 100)\n";
    std::cout << "Current Best Ask   : " << book.get_best_ask() << " ticks ($100.00 with 20 residual shares)\n";
    std::cout << "Average Latency    : " << (processed ? total_ns / processed : 0) << " ns per event\n";

    return 0;
}
