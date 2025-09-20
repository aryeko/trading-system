# Minimal Quantitative Trading Workflow  
**Version 1.0 — Reference Document**

---

## 1. Strategy Scope and Philosophy
- Objective: Build and deploy **one robust mid/long-range trading strategy** with daily risk alerts and weekly/monthly rebalancing.
- Capital Context: ~$70k portfolio, retail execution, liquid equities/ETFs.
- Core Philosophy: Select a single systematic approach (trend following, momentum, or mean reversion) and encode it into precise rules.

---

## 2. Data
- **Prototype Stage**: Use free sources (e.g., `yfinance`, Alpha Vantage).  
- **Validation Stage**: Upgrade to more reliable datasets (e.g., EODHD, Finnhub, CRSP if feasible).  
- **Rule**: Always document the data source and whether it is survivorship-bias-free and point-in-time.

---

## 3. Backtesting
- Framework: Backtesting.py or Backtrader.  
- Include realistic **costs**:
  - Slippage: ~0.1%  
  - Commissions: broker equivalent  
- **Metrics to capture**:
  - CAGR (annualized return)  
  - Sharpe Ratio  
  - Sortino Ratio  
  - Maximum Drawdown  
- Exclude academic or overly complex metrics (e.g., VaR, CVaR) in v1.  

---

## 4. Out-of-Sample Validation
- Split dataset into:  
  - **In-Sample (IS)**: ~70–80% for development.  
  - **Out-of-Sample (OOS)**: ~20–30%, untouched until strategy finalized.  
- OOS run = one-shot validation.  
- If performance fails, **do not tweak parameters on OOS data**; return to hypothesis stage.

---

## 5. Guardrails (Daily Defense)
System must check these conditions each trading day (after close):
1. **Single-day crash**: Stock down ≥8% → flag.  
2. **Drawdown alert**: Holding down ≥20% from recent high → flag.  
3. **Market filter**: SPY below 200-day moving average → reduce exposure.  

Flags may trigger alerts only; selling decisions remain discretionary in v1.

---

## 6. Portfolio Rebalancing (Weekly/Monthly Offense)
- Frequency: Weekly or monthly.  
- Process:  
  1. Rank candidates by strategy signal.  
  2. Buy top X names, equal-weighted.  
  3. Exit any position violating exit rules.  
- Simplicity rule: avoid over-optimization; use round numbers for parameters.

---

## 7. Deployment Phases
- **Phase 1 (Prototype)**:  
  - Run backtests.  
  - Generate daily reports with recommended actions (alerts + rebalances).  
- **Phase 2 (Paper Trading)**:  
  - Use broker paper accounts.  
  - Compare real-time signals to expected results.  
- **Phase 3 (Partial Live)**:  
  - Deploy with small portion of capital (~5–10%).  
  - Verify execution vs. backtest assumptions.  
- **Phase 4 (Full Live)**:  
  - Automate execution via broker API if strategy proves robust.  

---

## 8. Monitoring and Review
- Daily: Check system output and flagged alerts.  
- Monthly: Review realized returns, drawdowns, Sharpe, Sortino vs. backtest expectations.  
- Adjustment Protocol: Only adjust strategy after a minimum 3–6 months of live/paper evidence.

---

## 9. Out of Scope (For Future Phases)
- Multi-strategy portfolios.  
- Advanced position sizing (Kelly, volatility targeting, risk parity).  
- High-frequency or intraday execution modeling.  
- Statistical significance testing beyond simple OOS validation.

---

## 10. Principles
- **Keep it simple, explicit, and testable.**  
- **Avoid curve-fitting.** Parameters are tools, not toys.  
- **Respect drawdowns.** Risk management is as important as alpha.  
- **Paper trade first, scale later.**

---

This is the **baseline workflow**. Every build, test, or live deployment decision should be checked against this document.  
