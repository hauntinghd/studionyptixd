import { useState, useEffect, useRef } from 'react';
import {
  Zap,
  Terminal as TerminalIcon,
  Activity,
  TrendingUp,
  TrendingDown,
  Target,
  Box,
  BarChart3,
  Clock,
  CheckCircle,
  XCircle,
  ArrowUpCircle,
  ArrowDownCircle,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

type LogEntry = {
  id: string;
  timestamp: string;
  message: string;
  level: 'info' | 'warn' | 'error' | 'success';
};

type Position = {
  direction: string;
  entry_price: number;
  capital: number;
  remaining_seconds: number;
  entry_time: number;
};

export default function App() {
  const [btcPrice, setBtcPrice] = useState(0);
  const [lastBtcUpdate, setLastBtcUpdate] = useState('');
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [balance, setBalance] = useState(2.0);
  const [totalProfit, setTotalProfit] = useState(0.0);
  const [totalLoss, setTotalLoss] = useState(0.0);
  const [currentGoal, setCurrentGoal] = useState(100.0);
  const [dataPoints, setDataPoints] = useState(0);
  const [markets, setMarkets] = useState<Record<string, any>>({});
  const [connected, setConnected] = useState(false);

  const [winRate, setWinRate] = useState(0);
  const [totalTrades, setTotalTrades] = useState(0);
  const [wins, setWins] = useState(0);
  const [losses, setLosses] = useState(0);
  const [streak, setStreak] = useState(0);
  const [highWaterMark, setHighWaterMark] = useState(2.0);
  const [hourlyReturn, setHourlyReturn] = useState(0);
  const [projected5hr, setProjected5hr] = useState(2.0);
  const [btcMomentum30, setBtcMomentum30] = useState(0);
  const [btcMomentum60, setBtcMomentum60] = useState(0);
  const [openPositions, setOpenPositions] = useState<Record<string, Position>>({});
  const [marketsTracked, setMarketsTracked] = useState(0);
  const [tokensPriced, setTokensPriced] = useState(0);
  const [settlements, setSettlements] = useState<any[]>([]);
  const [tradeAlert, setTradeAlert] = useState<any>(null);
  const [balanceHistory, setBalanceHistory] = useState<number[]>([2.0]);

  const [isAutoScrollEnabled, setIsAutoScrollEnabled] = useState(true);
  const logEndRef = useRef<HTMLDivElement>(null);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    const connect = () => {
      ws.current = new WebSocket('ws://localhost:8765');
      ws.current.onopen = () => {
        setConnected(true);
        console.log('Connected to Shadow Edge V10');
      };
      ws.current.onmessage = (event) => {
        const payload = JSON.parse(event.data);
        switch (payload.type) {
          case 'log':
            setLogs(prev => [...prev.slice(-149), payload.data]);
            break;
          case 'btc_price':
            setBtcPrice(payload.data);
            setLastBtcUpdate(new Date().toLocaleTimeString());
            break;
          case 'market_update':
            setDataPoints(payload.data.ticks);
            setMarkets(prev => ({ ...prev, [payload.data.slug]: payload.data }));
            break;
          case 'trade_entry':
            setTradeAlert(payload.data);
            setBalance(payload.data.new_balance);
            setTimeout(() => setTradeAlert(null), 6000);
            break;
          case 'settlement': {
            const d = payload.data;
            setBalance(d.new_balance);
            setWinRate(d.win_rate);
            setTotalTrades(d.total_trades);
            setTotalProfit(d.total_profit);
            setTotalLoss(d.total_loss);
            setStreak(d.streak);
            setSettlements(prev => [...prev.slice(-29), d]);
            setBalanceHistory(prev => [...prev.slice(-99), d.new_balance]);
            break;
          }
          case 'balance_update':
            setBalance(payload.data.balance);
            break;
          case 'sync_state': {
            const s = payload.data;
            setBalance(s.balance);
            setCurrentGoal(s.current_goal);
            setTotalProfit(s.total_profit);
            setTotalLoss(s.total_loss);
            setWinRate(s.win_rate);
            setTotalTrades(s.total_trades);
            setWins(s.wins);
            setLosses(s.losses);
            setStreak(s.streak);
            setHighWaterMark(s.high_water_mark);
            if (s.positions) setOpenPositions(s.positions);
            break;
          }
          case 'core_metrics': {
            const m = payload.data;
            setDataPoints(m.ticks);
            setBtcMomentum30(m.btc_momentum_30s);
            setBtcMomentum60(m.btc_momentum_60s);
            setBalance(m.balance);
            setWinRate(m.win_rate);
            setTotalTrades(m.total_trades);
            setWins(m.wins);
            setLosses(m.losses);
            setTotalProfit(m.total_profit);
            setTotalLoss(m.total_loss);
            setStreak(m.streak);
            setHighWaterMark(m.high_water_mark);
            setCurrentGoal(m.current_goal);
            setHourlyReturn(m.hourly_return);
            setProjected5hr(m.projected_5hr);
            if (m.open_positions) setOpenPositions(m.open_positions);
            setMarketsTracked(m.markets_tracked);
            setTokensPriced(m.tokens_priced);
            break;
          }
        }
      };
      ws.current.onclose = () => {
        setConnected(false);
        setTimeout(connect, 3000);
      };
    };
    connect();
    return () => ws.current?.close();
  }, []);

  useEffect(() => {
    if (isAutoScrollEnabled) {
      logEndRef.current?.scrollIntoView({ behavior: 'auto' });
    }
  }, [logs, isAutoScrollEnabled]);

  const netPnl = totalProfit - totalLoss;

  return (
    <div className="min-h-screen bg-terminal-bg text-gray-300 font-sans selection:bg-terminal-text selection:text-black flex flex-col p-4 md:p-6 gap-4 overflow-hidden">

      {/* Trade Alert Overlay */}
      <AnimatePresence>
        {tradeAlert && (
          <motion.div
            initial={{ opacity: 0, y: -50 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -50 }}
            className={`fixed top-20 left-1/2 -translate-x-1/2 z-50 ${tradeAlert.direction === 'YES' ? 'bg-emerald-900/90 border-emerald-500' : 'bg-red-900/90 border-red-500'} border-2 p-4 rounded-xl shadow-lg flex items-center gap-4 backdrop-blur-md`}
          >
            {tradeAlert.direction === 'YES'
              ? <ArrowUpCircle className="w-8 h-8 text-emerald-400 animate-pulse" />
              : <ArrowDownCircle className="w-8 h-8 text-red-400 animate-pulse" />
            }
            <div>
              <h3 className="text-white font-bold uppercase tracking-tight font-mono text-sm">
                {tradeAlert.direction} Entry @ ${tradeAlert.entry_price?.toFixed(4)}
              </h3>
              <p className="text-gray-300 text-xs font-mono">
                {tradeAlert.slug?.slice(0, 35)} | ${tradeAlert.capital?.toFixed(4)} deployed | Conf: {(tradeAlert.confidence * 100)?.toFixed(1)}%
              </p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Header */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-black/40 p-4 rounded-xl border border-terminal-border glow-border relative overflow-hidden">
        {!connected && (
          <div className="absolute inset-0 bg-red-900/20 flex items-center justify-center backdrop-blur-sm z-10">
            <span className="text-red-500 font-mono font-bold animate-pulse">DISCONNECTED -- RECONNECTING...</span>
          </div>
        )}
        <div className="flex items-center gap-3">
          <div className="p-2 bg-terminal-text/10 rounded-lg"><Target className="w-7 h-7 text-terminal-text" /></div>
          <div>
            <h1 className="text-lg font-bold text-white flex items-center gap-2 font-mono uppercase tracking-tighter">
              Shadow Edge <span className="text-terminal-text underline decoration-2 underline-offset-4">V10</span>
              <span className="text-[10px] bg-yellow-500/20 text-yellow-400 border border-yellow-500/30 px-1.5 py-0.5 rounded ml-2 no-underline normal-case tracking-normal">PAPER SIM</span>
            </h1>
            <p className="text-[10px] text-terminal-text/60 font-mono uppercase">Statistical Edge Engine | BTC Momentum {'→'} Polymarket 5m</p>
          </div>
        </div>

        <div className="flex gap-4 items-center bg-black/60 px-4 py-2 rounded-lg border border-terminal-border">
          <div className="flex flex-col items-end">
            <span className="text-[10px] uppercase font-mono text-gray-500 tracking-wider">BTC (Coinbase)</span>
            <span className="text-lg font-mono font-bold glow-text leading-none">
              ${btcPrice > 0 ? btcPrice.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '---'}
            </span>
            {lastBtcUpdate && <span className="text-[8px] font-mono text-gray-600 mt-0.5">{lastBtcUpdate}</span>}
          </div>
          <div className="w-px h-10 bg-terminal-border" />
          <div className="flex flex-col items-end">
            <span className="text-[10px] uppercase font-mono text-gray-500 tracking-wider">Balance</span>
            <span className="text-lg font-mono font-bold text-white leading-none">${balance.toFixed(4)}</span>
            <span className={`text-[10px] font-mono ${netPnl >= 0 ? 'text-terminal-text' : 'text-red-500'}`}>
              {netPnl >= 0 ? '+' : ''}{netPnl.toFixed(4)} PnL
            </span>
          </div>
        </div>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 flex-1 min-h-0 overflow-hidden">

        {/* Left Sidebar */}
        <aside className="lg:col-span-3 flex flex-col gap-4 overflow-y-auto pr-1 terminal-scroll">

          {/* Momentum Signal */}
          <div className="bg-black/40 border border-terminal-border rounded-xl p-4 space-y-3">
            <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest border-b border-terminal-border pb-2 flex items-center gap-2">
              <Zap className="w-3 h-3" /> BTC Momentum Signal
            </h2>
            <div className="flex justify-between items-center">
              <span className="text-xs font-mono text-gray-400">30s Momentum</span>
              <span className={`font-mono font-bold text-sm ${btcMomentum30 > 0 ? 'text-terminal-text' : btcMomentum30 < 0 ? 'text-red-500' : 'text-gray-600'}`}>
                {(btcMomentum30 * 100).toFixed(4)}%
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-xs font-mono text-gray-400">60s Momentum</span>
              <span className={`font-mono font-bold text-sm ${btcMomentum60 > 0 ? 'text-terminal-text' : btcMomentum60 < 0 ? 'text-red-500' : 'text-gray-600'}`}>
                {(btcMomentum60 * 100).toFixed(4)}%
              </span>
            </div>
            <div className="h-2 bg-terminal-border rounded-full overflow-hidden flex">
              <div className="h-full bg-red-500/60 transition-all duration-300" style={{ width: `${Math.max(0, -btcMomentum30 * 5000)}%` }} />
              <div className="flex-1" />
              <div className="h-full bg-terminal-text/60 transition-all duration-300" style={{ width: `${Math.max(0, btcMomentum30 * 5000)}%` }} />
            </div>
            <div className="flex justify-between text-[10px] font-mono text-gray-600">
              <span>Bearish</span>
              <span>{btcMomentum30 > 0.001 ? 'SIGNAL ACTIVE' : btcMomentum30 < -0.001 ? 'SIGNAL ACTIVE' : 'Neutral'}</span>
              <span>Bullish</span>
            </div>
          </div>

          {/* Win Rate & Stats */}
          <div className="bg-black/40 border border-terminal-border rounded-xl p-4 space-y-3">
            <div className="flex justify-between items-center border-b border-terminal-border pb-2">
              <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest flex items-center gap-2">
                <BarChart3 className="w-3 h-3" /> Performance
              </h2>
              <button
                onClick={() => ws.current?.send(JSON.stringify({ type: 'reset' }))}
                className="text-[9px] uppercase font-mono text-red-500/50 hover:text-red-500 transition-colors border border-red-500/20 px-1.5 rounded"
              >
                Reset
              </button>
            </div>
            <div className="text-center py-2">
              <span className="text-3xl font-mono font-bold text-white">{(winRate * 100).toFixed(1)}%</span>
              <p className="text-[10px] font-mono text-gray-500 uppercase">Win Rate ({totalTrades} trades)</p>
            </div>
            <div className="grid grid-cols-2 gap-3 text-center">
              <div className="bg-terminal-text/5 rounded-lg p-2">
                <span className="text-sm font-mono font-bold text-terminal-text">{wins}</span>
                <p className="text-[9px] font-mono text-gray-500 uppercase">Wins</p>
              </div>
              <div className="bg-red-500/5 rounded-lg p-2">
                <span className="text-sm font-mono font-bold text-red-500">{losses}</span>
                <p className="text-[9px] font-mono text-gray-500 uppercase">Losses</p>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 uppercase font-mono">Profit</span>
                <span className="text-sm font-mono font-bold text-terminal-text">+${totalProfit.toFixed(4)}</span>
              </div>
              <div className="flex flex-col items-end">
                <span className="text-[10px] text-gray-500 uppercase font-mono">Loss</span>
                <span className="text-sm font-mono font-bold text-red-500">-${totalLoss.toFixed(4)}</span>
              </div>
            </div>
            <div className="h-1.5 bg-terminal-border rounded-full overflow-hidden">
              <div
                className="h-full bg-terminal-text transition-all duration-500 rounded-full"
                style={{ width: `${Math.min(100, (totalProfit / (totalProfit + totalLoss + 0.0001)) * 100)}%` }}
              />
            </div>
            <div className="flex justify-between text-[10px] font-mono text-gray-600">
              <span>Streak: {streak > 0 ? `+${streak}W` : streak < 0 ? `${streak}L` : '0'}</span>
              <span>HWM: ${highWaterMark.toFixed(2)}</span>
            </div>
          </div>

          {/* Compounding Progress */}
          <div className="bg-black/40 border border-terminal-border rounded-xl p-4 space-y-3">
            <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest border-b border-terminal-border pb-2">Compounder</h2>
            <div className="flex justify-between items-center">
              <span className="text-[10px] font-bold py-1 px-2 uppercase rounded-full text-terminal-text bg-terminal-text/10 font-mono">
                Goal: ${currentGoal.toLocaleString()}
              </span>
              <span className="text-xs font-mono font-bold text-terminal-text">
                {((balance / currentGoal) * 100).toFixed(1)}%
              </span>
            </div>
            <div className="overflow-hidden h-2 rounded bg-terminal-border">
              <motion.div
                initial={{ width: 0 }}
                animate={{ width: `${Math.min(100, (balance / currentGoal) * 100)}%` }}
                className="h-full bg-terminal-text rounded"
              />
            </div>
            <div className="grid grid-cols-2 gap-2 text-[10px] font-mono text-gray-500">
              <div>Hourly: {(hourlyReturn * 100).toFixed(2)}%</div>
              <div className="text-right">5hr Proj: ${projected5hr.toFixed(2)}</div>
            </div>
          </div>

          {/* System Status */}
          <div className="bg-black/40 border border-terminal-border rounded-xl p-4 space-y-2">
            <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest border-b border-terminal-border pb-2 flex items-center gap-2">
              <Activity className="w-3 h-3" /> System
            </h2>
            <div className="flex justify-between text-[11px] font-mono">
              <span className="text-gray-400">Ticks Processed</span>
              <span className="text-white font-bold">{dataPoints.toLocaleString()}</span>
            </div>
            <div className="flex justify-between text-[11px] font-mono">
              <span className="text-gray-400">Markets Tracked</span>
              <span className="text-white font-bold">{marketsTracked}</span>
            </div>
            <div className="flex justify-between text-[11px] font-mono">
              <span className="text-gray-400">Tokens Priced</span>
              <span className={`font-bold ${tokensPriced > 0 ? 'text-terminal-text' : 'text-red-500'}`}>{tokensPriced}</span>
            </div>
          </div>
        </aside>

        {/* Main Content */}
        <main className="lg:col-span-9 flex flex-col gap-4 min-h-0 overflow-hidden">

          {/* Open Positions */}
          {Object.keys(openPositions).length > 0 && (
            <div className="bg-black/40 border border-terminal-border rounded-xl p-4">
              <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest mb-3 flex items-center gap-2">
                <Clock className="w-3 h-3" /> Open Positions ({Object.keys(openPositions).length})
              </h2>
              <div className="space-y-2">
                {Object.entries(openPositions).map(([slug, pos]) => (
                  <div key={slug} className="flex items-center justify-between bg-black/60 rounded-lg px-3 py-2 border border-terminal-border/50">
                    <div className="flex items-center gap-3">
                      {pos.direction === 'YES'
                        ? <ArrowUpCircle className="w-4 h-4 text-terminal-text" />
                        : <ArrowDownCircle className="w-4 h-4 text-red-500" />
                      }
                      <div>
                        <span className="text-[11px] font-mono text-gray-300 font-bold">{slug.slice(0, 40)}</span>
                        <div className="flex gap-3 text-[10px] font-mono text-gray-500">
                          <span>{pos.direction} @ ${pos.entry_price.toFixed(4)}</span>
                          <span>${pos.capital.toFixed(4)} deployed</span>
                        </div>
                      </div>
                    </div>
                    <div className="text-right">
                      <span className="text-xs font-mono font-bold text-yellow-400">
                        {Math.floor(pos.remaining_seconds / 60)}:{String(Math.floor(pos.remaining_seconds % 60)).padStart(2, '0')}
                      </span>
                      <p className="text-[9px] font-mono text-gray-600">remaining</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Recent Settlements */}
          {settlements.length > 0 && (
            <div className="bg-black/40 border border-terminal-border rounded-xl p-4 max-h-40 overflow-y-auto terminal-scroll">
              <h2 className="text-xs uppercase font-mono text-gray-500 tracking-widest mb-2 flex items-center gap-2">
                <CheckCircle className="w-3 h-3" /> Recent Settlements
              </h2>
              <div className="space-y-1">
                {[...settlements].reverse().slice(0, 10).map((s, i) => (
                  <div key={i} className="flex items-center justify-between text-[11px] font-mono py-1 border-b border-terminal-border/30">
                    <div className="flex items-center gap-2">
                      {s.won
                        ? <CheckCircle className="w-3 h-3 text-terminal-text" />
                        : <XCircle className="w-3 h-3 text-red-500" />
                      }
                      <span className="text-gray-400">{s.slug?.slice(0, 30)}</span>
                    </div>
                    <span className={`font-bold ${s.pnl >= 0 ? 'text-terminal-text' : 'text-red-500'}`}>
                      {s.pnl >= 0 ? '+' : ''}{s.pnl?.toFixed(4)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Terminal Log */}
          <div className="flex-1 min-h-0 flex flex-col bg-black/60 border border-terminal-border rounded-xl overflow-hidden shadow-2xl backdrop-blur-md">
            <div className="bg-terminal-border/50 px-4 py-2 flex items-center justify-between border-b border-terminal-border">
              <div className="flex items-center gap-2">
                <TerminalIcon className="w-4 h-4 text-terminal-text" />
                <span className="text-xs font-mono text-gray-400 uppercase tracking-widest font-bold">Live Feed</span>
              </div>
              <div className="flex items-center gap-4">
                <button
                  onClick={() => setIsAutoScrollEnabled(!isAutoScrollEnabled)}
                  className={`text-[10px] font-mono px-2 py-0.5 rounded border transition-colors ${isAutoScrollEnabled
                    ? 'bg-terminal-text/20 border-terminal-text/50 text-terminal-text'
                    : 'bg-white/5 border-white/10 text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {isAutoScrollEnabled ? 'AUTO-SCROLL: ON' : 'AUTO-SCROLL: OFF'}
                </button>
                <div className="flex gap-1.5">
                  <div className="w-2.5 h-2.5 rounded-full bg-red-500/20 border border-red-500/50" />
                  <div className="w-2.5 h-2.5 rounded-full bg-yellow-500/20 border border-yellow-500/50" />
                  <div className={`w-2.5 h-2.5 rounded-full ${connected ? 'bg-terminal-text/40 border-terminal-text/70' : 'bg-red-500/40 border-red-500/70'} border`} />
                </div>
              </div>
            </div>
            <div
              ref={logContainerRef}
              className="flex-1 overflow-y-auto p-4 font-mono text-[11px] terminal-scroll bg-[#030303]"
            >
              <div className="space-y-1">
                {logs.map((log) => (
                  <div key={log.id} className="flex gap-4 border-l border-white/5 pl-2 hover:bg-white/5 transition-colors">
                    <span className="text-gray-600 shrink-0 select-none">[{log.timestamp}]</span>
                    <span className={`
                      ${log.level === 'success' ? 'text-terminal-text font-bold' : ''}
                      ${log.level === 'error' ? 'text-red-500 font-bold' : ''}
                      ${log.level === 'warn' ? 'text-yellow-500 italic' : ''}
                      ${log.level === 'info' ? 'text-gray-400' : ''}
                    `}>
                      {log.message}
                    </span>
                  </div>
                ))}
                <div ref={logEndRef} />
              </div>
            </div>
          </div>

          {/* Market Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 h-28 shrink-0">
            <AnimatePresence>
              {Object.values(markets).slice(0, 3).map((m: any) => (
                <motion.div
                  layout
                  key={m.slug}
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="bg-black/40 border border-terminal-border rounded-xl p-3 flex flex-col gap-1 relative overflow-hidden"
                >
                  <span className="text-[9px] font-mono text-gray-500 uppercase font-bold truncate">{m.slug}</span>
                  <div className="flex justify-between items-end mt-auto font-mono">
                    <div>
                      <span className="text-[9px] uppercase text-gray-600 font-bold">YES+NO</span>
                      <p className="text-base text-white font-bold leading-none">${m.sum?.toFixed(3)}</p>
                    </div>
                    <div className="text-right">
                      <span className="text-[9px] uppercase text-gray-600">UP: {m.up?.toFixed(3)}</span>
                      <p className="text-[9px] uppercase text-gray-600">DN: {m.down?.toFixed(3)}</p>
                    </div>
                  </div>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        </main>
      </div>

      <footer className="text-[10px] font-mono text-gray-600 flex justify-between items-center border-t border-terminal-border pt-3 px-2">
        <span className="flex items-center gap-1 uppercase text-yellow-500/60">
          <Activity className="w-3 h-3" /> Paper Simulation Only -- No Real Money At Risk
        </span>
        <span className="text-terminal-text/40 uppercase">Shadow Edge V10.0</span>
      </footer>
    </div>
  );
}
