# ARBITRAGE AGENT TECHNICAL SPECS

## 1. Tech Stack
- **Language**: Python 3.10+
- **Core Library**: `py-clob-client` (Official Polymarket SDK)
- **Concurrency**: `asyncio` for non-blocking WebSocket listeners and order placement.
- **Reporting**: Webhook integration for the "Mr. $ Bot" (Discord/Telegram).

## 2. Secure Auth Flow (Anti-Malware)
*   **Key Storage**: Use an `.env` file for API credentials and Private Keys. **DO NOT** commit this to version control.
*   **Permissions**: The Bot Wallet will be a dedicated address with exactly $2.00 capacity initially.
*   **Signing**: Order signing is done locally via `Eip712Manager`. No external services handle the keys.

## 3. High-Frequency Strategy
*   **Market Filter**: `gamma-api` query for markets with `BTC` + `price` + `active`.
*   **Data Input**: WebSocket subscription to `order_book_v2` for active market pairs.
*   **Execution Trigger**:
    ```python
    if (lowest_ask_yes + lowest_ask_no) < 0.995: # 0.5% profit margin
        execute_limit_orders(yes_token, no_token, amount)
    ```
*   **Order Type**: `LIMIT` with `time_in_force="FOK"` (Fill-or-Kill) to ensure atomicity.

## 4. Mr. $ Bot Bridge
*   The agent will maintain a `session_log.json`.
*   Every 10 successful trades, or on a 10% balance milestone, it will trigger a `POST` request to the Mr. $ Bot endpoint.
*   **Message Format**: 
    - 🚀 *Vibe Arbitrage Check-in: +$0.20 (Current Balance: $2.20)*
    - 🎯 *Markets Monitored: BTC-5min-Vol*

## 5. Scaling Logic
*   **Initial Stake**: $2.00
*   **Compounding**: Each successful arb increases the bankroll. 
*   **Level 2**: Once balance hits $10, enable multi-market monitoring (ETH, SOL, etc).
*   **Level 3**: Once balance hits $100, trigger "YouTube Studio" unlock sequence.
