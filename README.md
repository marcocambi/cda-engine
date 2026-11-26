# Continuous Double Auction (CDA) Matching Engine & Zero-Intelligence Market Simulator
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Table of Contents

* [About](#about)
* [Why](#why)
* [Formal Model & Mechanics](#formal-model--mechanics)
  * [Order Representation & Domain Types](#order-representation--domain-types)
  * [Price-Time (FIFO) Priority Relations](#price-time-fifo-priority-relations)
  * [Crossing Conditions & Matching Loop](#crossing-conditions--matching-loop)
  * [Execution Pricing, Partial Fills & Volume Conservation](#execution-pricing-partial-fills--volume-conservation)
* [Order Book State & Structural Invariants](#order-book-state--structural-invariants)
* [Market Microstructure & Allocative Metrics](#market-microstructure--allocative-metrics)
  * [Spread, Mid-Price & Depth](#spread-mid-price--depth)
  * [Volume-Weighted Average Price (VWAP)](#volume-weighted-average-price-vwap)
  * [Surplus Accounting & Smith's Allocative Efficiency](#surplus-accounting--smiths-allocative-efficiency)
* [Data Pipeline & Validation Contract](#data-pipeline--validation-contract)
  * [Raw Input Schema & Validation Rules](#raw-input-schema--validation-rules)
  * [Sanitized Streams, Rejection Logs & Execution Artifacts](#sanitized-streams-rejection-logs--execution-artifacts)
* [Zero-Intelligence Agent Simulation](#zero-intelligence-agent-simulation)
* [Repository Architecture & Module Responsibilities](#repository-architecture--module-responsibilities)
* [Incremental Roadmap & Workflow](#incremental-roadmap--workflow)
* [Execution & Testing Commands](#execution--testing-commands)
* [Limitations, Assumptions & Future Extensions](#limitations-assumptions--future-extensions)

---

## About

`cda-engine` is a deterministic, discrete-event Continuous Double Auction (CDA) matching engine and Zero-Intelligence (ZI) market microstructure simulator written in pure Python[span_0](start_span)[span_0](end_span)[span_1](start_span)[span_1](end_span). Rather than delegating order routing or trade clearing to an opaque third-party library, the system constructs a two-sided Limit Order Book (LOB) from first principles and separates **order-stream sanitization**, **deterministic order matching**, **microstructure and surplus metrics**, and **agent-based simulation** into distinct, independently verifiable components[span_2](start_span)[span_2](end_span)[span_3](start_span)[span_3](end_span).

The repository is built to evolve progressively alongside core Python milestones—starting from modular structure and standard/third-party libraries, moving through automated testing (`pytest`), file I/O, regular expressions, and object-oriented programming (`Order`, book queues, and matching loops), and culminating in Zero-Intelligence market experiments across multiple horizons[span_4](start_span)[span_4](end_span)[span_5](start_span)[span_5](end_span).

### Scope Boundaries

| Dimension | Included in Current System | Deliberately Out of Scope |
| :--- | :--- | :--- |
| **Instruments** | Single-instrument two-sided order book (`BUY` / `SELL`) | Multi-asset books, cross-margining, synthetic spreads |
| **Order Types** | `LIMIT` (positive limit price) and `MARKET` (unpriced aggressive) | `IOC`, `FOK`, `GTC` variants, `STOP`, `ICEBERG`, `POST-ONLY` |
| **Order Lifecycle** | Arrival, matching/partial fill, resting on book, full depletion | Explicit `CANCEL`, `AMEND` / `REPLACE`, expiration timers |
| **Priority Rule** | Strict price-time priority (FIFO) within each side | Pro-rata allocation, size-priority, auction call periods |
| **Concurrency & I/O** | Single-threaded deterministic batch & event-step processing | FIX/ITCH wire protocols, lock-free queues, kernel bypass |
| **Agent Behavior** | Constrained Zero-Intelligence (ZI) agents with private bounds | Reinforcement learning, order-book spoofing, strategic MM |

---

## Why

Production matching engines are optimized for microsecond tail latency, hardware affinity, and network framing—concerns that frequently obscure the underlying mathematical rules of order-book state transitions. When building a matching simulator from scratch, relying on an external black box makes it easy to miss subtle defects: inverted tie-breakers on equal prices, floating-point drift on price comparisons, silent quantity leaks during partial fills, or division-by-zero errors on empty books and zero-trade sessions[span_6](start_span)[span_6](end_span).

At the same time, experimental economics and econophysics show that a substantial share of double-auction allocative efficiency emerges directly from the trading institution itself rather than from sophisticated trader optimization. Vernon Smith's double-auction experiments and constrained Zero-Intelligence models demonstrate that unstrategic traders operating within private valuation and cost bounds can capture high allocative efficiency. Keeping the deterministic matching engine and the ZI simulation in one repository makes it possible to inspect every state transition, test boundary conditions with `pytest`, and verify economic metrics quantitatively[span_7](start_span)[span_7](end_span)[span_8](start_span)[span_8](end_span).

---

## Formal Model & Mechanics

### Order Representation & Domain Types

Let $\mathcal{U}$ denote the set of orders processed by the engine. Each order $u \in \mathcal{U}$ is represented by a six-tuple:

$$\omega_u = \big(\text{id}_u,\, \text{side}_u,\, \text{type}_u,\, p_u,\, q_u,\, t_u\big)$$

where:

* $\text{id}_u$ is a unique non-empty order identifier.
* $\text{side}_u \in \{\text{BUY}, \text{SELL}\}$ identifies whether the order seeks to buy or sell the instrument.
* $\text{type}_u \in \{\text{LIMIT}, \text{MARKET}\}$ specifies whether execution is constrained by a limit price.
* $p_u$ is the limit price: a strictly positive numeric value ($p_u > 0$) when $\text{type}_u = \text{LIMIT}$, and absent/null when $\text{type}_u = \text{MARKET}$.
* $q_u \in \mathbb{Z}_{\ge 0}$ denotes remaining unexecuted quantity (with initial quantity $Q_{u,0} \in \mathbb{Z}_{>0}$ upon valid arrival).
* $t_u \in \mathbb{Z}_{>0}$ denotes the discrete arrival timestamp.

To prevent binary floating-point rounding drift during price comparisons, spread calculations, and VWAP accumulation, prices should be parsed and stored using exact decimal arithmetic (`decimal.Decimal`) or integer tick multiples.

### Price-Time (FIFO) Priority Relations

Resting orders on each side of the book follow **price-time priority (FIFO)**: better prices take precedence over worse prices, and among orders at the same price level, an earlier arrival timestamp takes precedence over a later timestamp[span_9](start_span)[span_9](end_span)[span_10](start_span)[span_10](end_span).

For two `BUY` orders $i$ and $j$ with prices $p_i, p_j$ and timestamps $t_i, t_j$, order $i$ has strictly higher priority than order $j$ (written $(p_i, t_i) \succ_{\text{buy}} (p_j, t_j)$) if and only if:

$$(p_i, t_i) \succ_{\text{buy}} (p_j, t_j) \iff (p_i > p_j) \lor \big(p_i = p_j \land t_i < t_j\big)$$

For two `SELL` orders $i$ and $j$, lower ask prices have higher priority, with earlier timestamps breaking ties:

$$(p_i, t_i) \succ_{\text{sell}} (p_j, t_j) \iff (p_i < p_j) \lor \big(p_i = p_j \land t_i < t_j\big)$$

If two orders on the same side share both the same price and the same timestamp, deterministic tie-breaking (such as original sequence index or `order_id`) preserves a total strict order independent of hash iteration order.

Let $\mathcal{B}_{\text{buy}}$ and $\mathcal{B}_{\text{sell}}$ denote the active `BUY` and `SELL` sets at a given state. Whenever $\mathcal{B}_{\text{buy}} \neq \emptyset$, the best bid price is:

$$P_{\text{bid}}^{\text{best}} = \max_{u \in \mathcal{B}_{\text{buy}}} p_u$$

Whenever $\mathcal{B}_{\text{sell}} \neq \emptyset$, the best ask price is:

$$P_{\text{ask}}^{\text{best}} = \min_{v \in \mathcal{B}_{\text{sell}}} p_v$$

### Crossing Conditions & Matching Loop

When a new order arrives, it acts as an **aggressive** order attempting to match against resting **passive** orders on the opposite side of the book:

* **Aggressive `BUY` (`LIMIT` with limit price $P_{\text{agg}}$):** Crosses against the top passive `SELL` order at price $P_{\text{ask}}$ if and only if:
  $$P_{\text{agg}} \ge P_{\text{ask}}$$

* **Aggressive `SELL` (`LIMIT` with limit price $P_{\text{agg}}$):** Crosses against the top passive `BUY` order at price $P_{\text{bid}}$ if and only if:
  $$P_{\text{agg}} \le P_{\text{bid}}$$

* **Aggressive `MARKET` (`BUY` or `SELL`):** Carries no price limit on the aggressive side and matches against any resting order on the opposite side while the opposite side is non-empty and $q_{\text{agg}, \text{rem}} > 0$. Any unexecuted remainder once the opposite book is empty or non-crossable is not placed as a passive order on the book.

The matching loop for an incoming aggressive order runs as follows:

1. Select the highest-priority passive order on the opposite side (lowest `SELL` price then earliest timestamp for an incoming `BUY`; highest `BUY` price then earliest timestamp for an incoming `SELL`)[span_11](start_span)[span_11](end_span).
2. Verify that the crossing condition holds and $q_{\text{agg}, \text{rem}} > 0$; otherwise stop matching[span_12](start_span)[span_12](end_span).
3. Compute executed quantity $q_k$ and execution price $P_{\text{exec}, k}$, record the trade, and update both remaining quantities[span_13](start_span)[span_13](end_span).
4. If the passive order reaches $q_{\text{pas}, \text{rem}} = 0$, remove it from the passive side and continue to the next passive order[span_14](start_span)[span_14](end_span).
5. Once matching stops, if the incoming order is `LIMIT` with $q_{\text{agg}, \text{rem}} > 0$, add it to its own side of the book with remaining quantity $q_{\text{agg}, \text{rem}}$[span_15](start_span)[span_15](end_span).

### Execution Pricing, Partial Fills & Volume Conservation

In continuous double-auction matching with passive price discovery, each trade executes at the **price of the resting passive order** already in the book, not the price of the incoming aggressive order[span_16](start_span)[span_16](end_span):

$$P_{\text{exec}, k} = P_{\text{passive}, k}$$

For fill $k$ between an aggressive order with remaining quantity $q_{\text{agg}, \text{rem}}$ and the top passive order with remaining quantity $q_{\text{pas}, \text{rem}}$, the executed volume is:

$$q_k = \min\big(q_{\text{agg}, \text{rem}},\, q_{\text{pas}, \text{rem}}\big)$$

Immediately after the fill, both remaining quantities are decremented:

$$q_{\text{agg}, \text{rem}} \leftarrow q_{\text{agg}, \text{rem}} - q_k$$

$$q_{\text{pas}, \text{rem}} \leftarrow q_{\text{pas}, \text{rem}} - q_k$$

This rule handles three execution cases uniformly:

* **Full passive fill ($q_{\text{agg}, \text{rem}} \ge q_{\text{pas}, \text{rem}}$):** The passive order is depleted ($q_{\text{pas}, \text{rem}} = 0$) and removed; if $q_{\text{agg}, \text{rem}} > 0$, the aggressive order continues walking the book to the next passive order.
* **Partial passive fill ($q_{\text{agg}, \text{rem}} < q_{\text{pas}, \text{rem}}$):** The aggressive order is fully filled ($q_{\text{agg}, \text{rem}} = 0$), while the passive order stays at the head of its side with reduced quantity $q_{\text{pas}, \text{rem}} - q_k$ and retains its original timestamp.
* **Exact match ($q_{\text{agg}, \text{rem}} = q_{\text{pas}, \text{rem}}$):** Both sides reach $0$ remaining quantity and neither adds resting liquidity.

Across the full lifecycle of any order $u$ with initial quantity $Q_{u,0}$ participating in trade set $\mathcal{T}(u)$, volume is strictly conserved:

$$\sum_{k \in \mathcal{T}(u)} q_k + q_{u, \text{rem}} = Q_{u,0}$$

---

## Order Book State & Structural Invariants

After every incoming order is processed, the engine maintains the following invariants:

* **Non-Crossing Book Invariant:** Whenever both $\mathcal{B}_{\text{buy}} \neq \emptyset$ and $\mathcal{B}_{\text{sell}} \neq \emptyset$ after matching completes, the best bid is strictly below the best ask:
  $$P_{\text{bid}}^{\text{best}} < P_{\text{ask}}^{\text{best}}$$

* **Positive Resting Quantities:** Every order $u \in \mathcal{B}_{\text{buy}} \cup \mathcal{B}_{\text{sell}}$ currently in the book has $q_u > 0$; orders with $q_u = 0$ are removed immediately.

* **Aggregate Traded Volume:** Across all $N$ trades executed in a run, total traded quantity equals:
  $$Q_{\text{total}} = \sum_{k=1}^{N} q_k$$

* **Deterministic Replay:** Executing the same ordered input stream produces identical trades, prices, volumes, and final book state.

---

## Market Microstructure & Allocative Metrics

### Spread, Mid-Price & Depth

At any discrete timestamp or snapshot $t$[span_17](start_span)[span_17](end_span):

* **Absolute Bid-Ask Spread ($S_t$):** Defined when both $\mathcal{B}_{\text{buy}, t} \neq \emptyset$ and $\mathcal{B}_{\text{sell}, t} \neq \emptyset$; reported as `N/A` if either side is empty[span_18](start_span)[span_18](end_span):
  $$S_t = P_{\text{ask}, t}^{\text{best}} - P_{\text{bid}, t}^{\text{best}}$$

* **Mid-Price ($M_t$):** Defined when both sides are non-empty:
  $$M_t = \frac{P_{\text{ask}, t}^{\text{best}} + P_{\text{bid}, t}^{\text{best}}}{2}$$

* **Relative Spread ($s_{\text{rel}, t}$):** Spread normalized by mid-price when $M_t > 0$:
  $$s_{\text{rel}, t} = \frac{S_t}{M_t}$$

* **Book Depth at Price $p$ ($\text{Depth}_{\text{side}}(p)$):** Total resting quantity on a side at price level $p$, letting $\mathcal{B}_{\text{side}}(p) = \{u \in \mathcal{B}_{\text{side}} : p_u = p\}$:
  $$\text{Depth}_{\text{side}}(p) = \sum_{u \in \mathcal{B}_{\text{side}}(p)} q_u$$

### Volume-Weighted Average Price (VWAP)

Over $N$ executed trades with execution prices $P_k$ and quantities $q_k$ ($k = 1, \dots, N$), the volume-weighted average price is:

$$\text{VWAP} = \frac{\sum_{k=1}^{N} P_k \, q_k}{\sum_{k=1}^{N} q_k}, \qquad \text{for } \sum_{k=1}^{N} q_k > 0$$

If no trades occur ($N = 0$ or $\sum_{k=1}^{N} q_k = 0$), VWAP is undefined and reported as `N/A` rather than dividing by zero[span_19](start_span)[span_19](end_span).

### Surplus Accounting & Smith's Allocative Efficiency

Suppose buyer $b_k$ has private valuation $V_{b_k}$ and seller $s_k$ has private cost $C_{s_k}$. When $q_k$ units trade between them at price $P_k$[span_20](start_span)[span_20](end_span):

* **Buyer surplus on trade $k$:** $(V_{b_k} - P_k)\, q_k$[span_21](start_span)[span_21](end_span)
* **Seller surplus on trade $k$:** $(P_k - C_{s_k})\, q_k$[span_22](start_span)[span_22](end_span)
* **Joint surplus generated by trade $k$:**
  $$\pi_k = \big[(V_{b_k} - P_k) + (P_k - C_{s_k})\big]\, q_k = (V_{b_k} - C_{s_k})\, q_k$$

The execution price $P_k$ determines how surplus is divided between buyer and seller, while cancelling out of total trade surplus $\pi_k$[span_23](start_span)[span_23](end_span). Over $N$ trades, **realized surplus** is:

$$\Pi_{\text{real}} = \sum_{k=1}^{N} (V_{b_k} - C_{s_k})\, q_k$$

To compute the static theoretical upper bound, sort buyer valuations in non-increasing order and seller costs in non-decreasing order[span_24](start_span)[span_24](end_span):

$$V_{(1)} \ge V_{(2)} \ge \dots \ge V_{(N_b)}, \qquad C_{(1)} \le C_{(2)} \le \dots \le C_{(N_s)}$$

For unit-sized pairings $m = 1, \dots, \min(N_b, N_s)$, **maximum theoretical surplus** is[span_25](start_span)[span_25](end_span):

$$\Pi_{\max} = \sum_{m=1}^{\min(N_b, N_s)} \max\big(0,\, V_{(m)} - C_{(m)}\big)$$

**Smith's Allocative Efficiency** measures the percentage of maximum theoretical surplus captured by realized trades[span_26](start_span)[span_26](end_span):

$$E_{\text{Smith}} = \frac{\Pi_{\text{real}}}{\Pi_{\max}} \times 100\%, \qquad \text{for } \Pi_{\max} > 0$$

When $\Pi_{\max} = 0$, division by zero is avoided and handled explicitly as `N/A`[span_27](start_span)[span_27](end_span).

---

## Data Pipeline & Validation Contract

### Raw Input Schema & Validation Rules

Raw order streams (`raw_orders.csv`) use the header[span_28](start_span)[span_28](end_span):

```csv
timestamp,order_id,side,order_type,price,quantity
```

Each record is checked against seven rules before entering the book[span_29](start_span)[span_29](end_span):

| Field | Condition for Acceptance | Typical Rejection Trigger |
| :--- | :--- | :--- |
| `timestamp` | Integer with `timestamp > 0` | Non-integer string, `0`, negative integer, blank |
| `order_id` | Non-empty string, unique across the file | Empty string, whitespace-only, duplicate ID |
| `side` | Exact match in `{"BUY", "SELL"}` | Invalid side such as `"HOLD"`, lowercase, blank |
| `order_type` | Exact match in `{"LIMIT", "MARKET"}` | Unsupported order type or blank value |
| `price` (`LIMIT`) | Parseable positive number (`price > 0`) | Missing price, `0`, negative number, non-numeric text |
| `price` (`MARKET`) | Empty / absent (`""` or `None`) | Non-empty numeric price supplied on a market order |
| `quantity` | Integer with `quantity > 0` | `0`, negative integer, decimal/non-integer, blank |

### Sanitized Streams, Rejection Logs & Execution Artifacts

* **`clean_orders.csv`:** Validated rows sorted by ascending `timestamp` (preserving deterministic relative order on ties) under the standard column schema[span_30](start_span)[span_30](end_span).
* **`rejected_orders.log`:** Rejected rows with row reference, `order_id` (when present), and a clear explanation of the failed validation rule[span_31](start_span)[span_31](end_span).
* **Validation Summary Report:** Printed to stdout with total rows read, valid count, rejected count, total `BUY` and `SELL` quantities, and `min`, `max`, and `mean` prices across valid `LIMIT` orders[span_32](start_span)[span_32](end_span).
* **`trades.csv`:** Executed trade log with columns `trade_id,buy_order_id,sell_order_id,execution_price,executed_quantity`[span_33](start_span)[span_33](end_span).
* **`book_final_state.txt`:** Unfilled resting `BUY` and `SELL` orders in priority order plus the final bid-ask spread (or `N/A` if either side is empty)[span_34](start_span)[span_34](end_span).
* **`zi_results.csv`:** Trade series, price path, and allocative efficiency outputs across simulation horizons[span_35](start_span)[span_35](end_span).

---

## Zero-Intelligence Agent Simulation

In the Zero-Intelligence experiment, $N_b$ buyers and $N_s$ sellers interact over a price range $[P_{\min}, P_{\max}]$[span_36](start_span)[span_36](end_span):

1. Each buyer $i \in \{1, \dots, N_b\}$ is assigned a private valuation $V_i \in [P_{\min}, P_{\max}]$, and each seller $j \in \{1, \dots, N_s\}$ is assigned a private cost $C_j \in [P_{\min}, P_{\max}]$[span_37](start_span)[span_37](end_span).
2. Across $T$ turns, an agent (or side) is selected and submits a budget-constrained order[span_38](start_span)[span_38](end_span):
   * A buyer submits a `BUY` price within $[P_{\min}, V_i]$ (never bidding above $V_i$)[span_39](start_span)[span_39](end_span).
   * A seller submits a `SELL` price within $[C_j, P_{\max}]$ (never asking below $C_j$)[span_40](start_span)[span_40](end_span).
3. Each order is processed through the deterministic CDA matching engine[span_41](start_span)[span_41](end_span).
4. At the end of a run of $T$ turns, the simulator reports traded volume, VWAP, realized surplus $\Pi_{\text{real}}$, maximum theoretical surplus $\Pi_{\max}$, and Smith's allocative efficiency $E_{\text{Smith}}$, comparing across horizons such as $T = 50$, $T = 200$, and $T = 1000$[span_42](start_span)[span_42](end_span).

---

## Repository Architecture & Module Responsibilities

```text
cda-engine/
├── README.md
├── data/
│   ├── raw_orders.csv          # Unvalidated input records
│   └── incoming_orders.csv     # Validated order stream for matching
├── src/
│   ├── __init__.py
│   ├── validator.py            # CSV parsing, validation rules, deduplication, summary stats
│   ├── order.py                # Order model & price-time (FIFO) comparison logic
│   ├── book.py                 # Two-si
