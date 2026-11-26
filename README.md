# Continuous Double Auction (CDA) Matching Engine & Zero-Intelligence Market Simulator
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Table of Contents

* [About](#about)
* [Why](#why)
* [Formal Model & Mechanics](#formal-model--mechanics)
  * [Order Representation & FIFO Priority](#order-representation--fifo-priority)
  * [Crossing Conditions, Execution Price & Volume Conservation](#crossing-conditions-execution-price--volume-conservation)
* [Market Microstructure & Allocative Metrics](#market-microstructure--allocative-metrics)
  * [Book & Trade Metrics](#book--trade-metrics)
  * [Surplus & Smith's Allocative Efficiency](#surplus-and-smiths-allocative-efficiency)
* [Data Pipeline & Validation](#data-pipeline--validation)
  * [Input Validation Rules](#input-validation-rules)
  * [Outputs & Artifacts](#outputs--artifacts)
* [Zero-Intelligence Simulation](#zero-intelligence-simulation)
* [Repository Structure](#repository-structure)
* [Incremental Workflow](#incremental-workflow)
* [Usage & Testing](#usage--testing)
* [Limitations & Notes](#limitations--notes)

---

## About

`cda engine` is a deterministic, discrete event Continuous Double Auction (CDA) matching engine and Zero Intelligence (ZI) market simulator written in Python. Rather than relying on an opaque third party matching library, it builds a single instrument two sided Limit Order Book (LOB) from first principles and separates order stream sanitization, deterministic order matching, microstructure and surplus metrics, and ZI market simulation into distinct, independently testable modules.

| Dimension | Included in Current Architecture | Excluded |
| :--- | :--- | :--- |
| **Instruments** | Single instrument two sided book (`BUY` / `SELL`) | Multi asset books, cross margining, synthetic spreads |
| **Order Types** | `LIMIT` (positive limit price) and `MARKET` (unpriced aggressive) | `IOC`, `FOK`, stop loss, iceberg, post only |
| **Priority Rule** | Strict price time (FIFO) priority with deterministic tie breaking | Pro rata allocation, size priority, call auctions |
| **Order Lifecycle** | Arrival, matching/partial fill, resting on book, full depletion | Explicit `CANCEL`, `AMEND` / `REPLACE`, order expiration |
| **Infrastructure** | CSV ingestion, rejection logging, `pytest` tests, ZI runs | FIX/ITCH network protocols, lock free queues, colocation |

---

## Why

Production matching engines optimize for sub microsecond latency, hardware affinity, and network framing, details that often obscure the exact mathematical transitions of an order book. Implementing a matching engine from first principles makes every rule explicit and helps catch subtle defects early, such as inverted tie breakers on equal prices, binary floating point drift in price comparisons, quantity leaks on partial fills, or division by zero errors on empty books and zero trade sessions.

At the same time, experimental market microstructure shows that continuous double auctions capture high allocative efficiency even when traders are unstrategic and budget constrained (Zero Intelligence agents, following Vernon Smith's double auction experiments). Keeping the deterministic matching core and the ZI simulation in one repository makes it possible to inspect every state transition, verify boundary conditions with `pytest`, and measure economic efficiency across horizons.

---

## Formal Model & Mechanics

### Order Representation & FIFO Priority

Each order $u$ is defined by $(id_u, \text{side}_u, \text{type}_u, p_u, q_u, t_u)$, where $\text{side}_u \in \{\text{BUY}, \text{SELL}\}$, $\text{type}_u \in \{\text{LIMIT}, \text{MARKET}\}$, $p_u > 0$ is a strictly positive limit price for `LIMIT` orders (empty/none for `MARKET`), $q_u \ge 0$ is remaining quantity (with valid initial quantity $Q_{u,0} > 0$), and $t_u > 0$ is the discrete arrival timestamp. Parse and compare prices using `decimal.Decimal` or integer tick multiples rather than binary `float` to avoid rounding drift.

Resting `LIMIT` orders on each side follow **price time (FIFO) priority**: better price first, then earlier arrival timestamp on ties (with a deterministic tie breaker such as input sequence index or `order_id` if price and timestamp match):

$$(p_i, t_i) \succ_{\text{buy}} (p_j, t_j) \iff (p_i > p_j) \lor (p_i = p_j \land t_i < t_j)$$

$$(p_i, t_i) \succ_{\text{sell}} (p_j, t_j) \iff (p_i < p_j) \lor (p_i = p_j \land t_i < t_j)$$

When a side is non empty, best bid and best ask are:

$$P_{\text{bid}}^{\text{best}} = \max_{u \in \mathcal{B}_{\text{buy}}} p_u, \qquad P_{\text{ask}}^{\text{best}} = \min_{v \in \mathcal{B}_{\text{sell}}} p_v$$

Once no crossable orders remain, the book satisfies $P_{\text{bid}}^{\text{best}} < P_{\text{ask}}^{\text{best}}$.

### Crossing Conditions, Execution Price & Volume Conservation

An incoming aggressive order crosses against the highest priority passive order on the opposite side while remaining quantity is positive and the crossing condition holds:

* **Aggressive `BUY` (`LIMIT` at $P_{\text{agg}}$):** crosses top passive `SELL` at $P_{\text{ask}}$ iff $P_{\text{agg}} \ge P_{\text{ask}}$.
* **Aggressive `SELL` (`LIMIT` at $P_{\text{agg}}$):** crosses top passive `BUY` at $P_{\text{bid}}$ iff $P_{\text{agg}} \le P_{\text{bid}}$.
* **Aggressive `MARKET` (`BUY` or `SELL`):** crosses against resting liquidity on the opposite side until its remaining quantity reaches $0$ or no crossable opposite liquidity remains. Unfilled `MARKET` remainders never rest on the book; any remaining `LIMIT` quantity ($q_{\text{agg,rem}} > 0$) rests on its own side.

Each trade $k$ executes at the **passive** order's price:

$$P_{\text{exec}, k} = P_{\text{passive}, k}$$

For fill $k$ between an aggressive order with remaining quantity $q_{\text{agg,rem}}$ and the top passive order with remaining quantity $q_{\text{pas,rem}}$:

$$q_k = \min(q_{\text{agg,rem}}, q_{\text{pas,rem}})$$

$$q_{\text{agg,rem}} \leftarrow q_{\text{agg,rem}} - q_k, \qquad q_{\text{pas,rem}} \leftarrow q_{\text{pas,rem}} - q_k$$

Depleted passive orders ($q_{\text{pas,rem}} = 0$) are removed immediately. Across all trades $\mathcal{T}(u)$ involving order $u$ with initial quantity $Q_{u,0}$:

$$\sum_{k \in \mathcal{T}(u)} q_k + q_{u,\text{rem}} = Q_{u,0}, \qquad Q_{\text{total}} = \sum_{k=1}^{N} q_k$$

---

## Market Microstructure & Allocative Metrics

### Book & Trade Metrics

At snapshot or step $t$ (when both sides are non empty; otherwise `N/A` for spread and mid price):

$$S_t = P_{\text{ask}, t}^{\text{best}} - P_{\text{bid}, t}^{\text{best}}, \qquad M_t = \frac{P_{\text{ask}, t}^{\text{best}} + P_{\text{bid}, t}^{\text{best}}}{2}, \qquad s_{\text{rel}, t} = \frac{S_t}{M_t} \quad (M_t > 0)$$

Book depth at price level $p$ on a side is the sum of remaining quantities of active orders on that side at price $p$:

$$\text{Depth}_{\text{side}}(p) = \sum_{u \in \mathcal{B}_{\text{side}}(p)} q_u$$

Volume Weighted Average Price over $N$ trades (`N/A` when total volume is $0$):

$$\text{VWAP} = \frac{\sum_{k=1}^{N} P_k q_k}{\sum_{k=1}^{N} q_k}, \qquad \sum_{k=1}^{N} q_k > 0$$

### Surplus & Smith's Allocative Efficiency

For buyers with private valuations $V_i \in [P_{\min}, P_{\max}]$ and sellers with private costs $C_j \in [P_{\min}, P_{\max}]$, joint surplus on trade $k$ of quantity $q_k$ at price $P_k$ between buyer $b_k$ and seller $s_k$ is:

$$\pi_k = \big[(V_{b_k} - P_k) + (P_k - C_{s_k})\big] q_k = (V_{b_k} - C_{s_k}) q_k$$

Realized surplus over $N$ executed trades:

$$\Pi_{\text{real}} = \sum_{k=1}^{N} (V_{b_k} - C_{s_k}) q_k$$

Sorting buyer valuations non increasingly ($V_{(1)} \ge V_{(2)} \ge \dots$) and seller costs non decreasingly ($C_{(1)} \le C_{(2)} \le \dots$), maximum theoretical surplus for unit pairings is:

$$\Pi_{\max} = \sum_{m=1}^{\min(N_b, N_s)} \max(0, V_{(m)} - C_{(m)})$$

Smith's Allocative Efficiency (for $\Pi_{\max} > 0$; reported as `N/A` when $\Pi_{\max} = 0$):

$$E_{\text{Smith}} = \frac{\Pi_{\text{real}}}{\Pi_{\max}} \times 100\%$$

---

## Data Pipeline & Validation

### Input Validation Rules

Raw order files (`raw_orders.csv`) use the header `timestamp,order_id,side,order_type,price,quantity`. Every record is checked against seven deterministic rules before matching:

* `timestamp`: integer strictly greater than `0`.
* `order_id`: non empty string, strictly unique within the file.
* `side`: exact match in `{"BUY", "SELL"}`.
* `order_type`: exact match in `{"LIMIT", "MARKET"}`.
* `price` (`LIMIT`): valid number strictly greater than `0`.
* `price` (`MARKET`): empty/none (numeric price on a market order is rejected).
* `quantity`: integer strictly greater than `0`.

### Outputs & Artifacts

* `clean_orders.csv`: valid orders sorted by ascending `timestamp` (with deterministic tie breaking).
* `rejected_orders.log`: rejected rows with row/order reference and failure reason (plus stdout summary stats on row counts, volumes, and `LIMIT` min/max/mean prices).
* `trades.csv`: sequential execution log (`trade_id,buy_order_id,sell_order_id,execution_price,executed_quantity`).
* `book_final_state.txt`: remaining resting orders in priority order and final bid ask spread (or `N/A`).
* `zi_results.csv`: trade series, price trajectory, and Smith efficiency across horizons.

---

## Zero-Intelligence Simulation

1. Assign private valuations $V_i \in [P_{\min}, P_{\max}]$ to $N_b$ buyers and private costs $C_j \in [P_{\min}, P_{\max}]$ to $N_s$ sellers.
2. Over $T$ turns, select an agent and generate a budget constrained order: `BUY` price in $[P_{\min}, V_i]$ or `SELL` price in $[C_j, P_{\max}]$.
3. Submit each generated order to the deterministic CDA matching engine.
4. Record trade/price trajectories and compute VWAP, $\Pi_{\text{real}}$, $\Pi_{\max}$, and $E_{\text{Smith}}$ across horizons (such as $T = 50, 200, 1000$).

---

## Repository Structure

```text
cda-engine/
├── README.md
├── data/
│   ├── raw_orders.csv
│   └── incoming_orders.csv
├── src/
│   ├── __init__.py
│   ├── validator.py
│   ├── order.py
│   ├── book.py
│   ├── matching.py
│   ├── metrics.py
│   └── zi_simulation.py
└── tests/
    ├── test_validator.py
    ├── test_order_priority.py
    └── test_matching.py
```

## Incremental Workflow

1. **Stream Validation (`validator.py`):** Parse `raw_orders.csv`, enforce rules and unique IDs, sort valid rows into `clean_orders.csv`, log rejections to `rejected_orders.log`, and print summary statistics.
2. **Automated Testing (`pytest`):** Test validation boundaries, empty/malformed inputs, and price time priority using temporary files and unit tests.
3. **Book & Matching Engine (`order.py`, `book.py`, `matching.py`):** Model `Order` and two sided queues with FIFO priority, handle partial/multi level fills and volume conservation, and write `trades.csv` and `book_final_state.txt`.
4. **ZI Microstructure Study (`metrics.py`, `zi_simulation.py`):** Run constrained ZI simulations across horizons $T$ and evaluate VWAP, realized surplus, maximum surplus, and Smith efficiency.

## Usage & Testing

```bash
pytest -v
python3 -m src.validator data/raw_orders.csv
python3 -m src.matching data/incoming_orders.csv
python3 -m src.zi_simulation --turns 50 200 1000 --seed 42
```

## Limitations & Notes

* Single instrument scope; no explicit order cancellations or amendments; discrete step timestamps without network latency.
* Zero intelligence agents have no learning or strategic behavior; run multiple random seeds to evaluate statistical dispersion across horizons $T$.
* Keep this `README.md` updated incrementally as each module and test suite is completed.

4. ZI Microstructure Study (metrics.py, zi_simulation.py): Run constrained ZI simulations across horizons T and evaluate VWAP, realized surplus, maximum surplus, and Smith efficiency.