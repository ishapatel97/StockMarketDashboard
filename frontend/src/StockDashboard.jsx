import { useEffect, useState } from "react";
import axios from "axios";
import "./StockDashboard.css";

import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  CategoryScale
} from "chart.js";

import { Line } from "react-chartjs-2";

ChartJS.register(LineElement, PointElement, LinearScale, CategoryScale);

function StockDashboard() {

  const [stocks, setStocks] = useState([]);
  const [displayCount, setDisplayCount] = useState(10);
  const [threshold, setThreshold] = useState(1.5);
  const [chartData, setChartData] = useState(null);
  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [loadingIngest, setLoadingIngest] = useState(false);
  const [loadingStocks, setLoadingStocks] = useState(false);
  const [statusMsg, setStatusMsg] = useState("");
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState("");
  const [aiData, setAiData] = useState(null);

  const refreshStocks = () => {
    setLoadingStocks(true);
    axios.get("http://127.0.0.1:8000/stocks", { params: { threshold, limit: displayCount } })
      .then((res) => setStocks(res.data))
      .catch((err) => console.error(err))
      .finally(() => setLoadingStocks(false));
  };

  const ingestBatch = async () => {
    setLoadingIngest(true);
    setStatusMsg("Starting full ingestion…");

    // Start ingestion
    let intervalId;
    try {
      const poll = async () => {
        try {
          const p = await axios.get("http://127.0.0.1:8000/ingest-progress");
          const prog = p.data || {};
          const {
            total_tickers = 0,
            processed_tickers = 0,
            rows_inserted = 0,
            current_chunk = 0,
            total_chunks = 0,
            done = false,
          } = prog;
          setStatusMsg(`Processed ${processed_tickers}/${total_tickers} tickers, inserted ${rows_inserted} rows (chunk ${current_chunk}/${total_chunks})`);
          if (done) {
            clearInterval(intervalId);
            setLoadingIngest(false);
          }
        } catch (e) {
          // ignore polling errors
        }
      };

      // Start polling first to display 0/0 if needed
      intervalId = setInterval(poll, 1000);
      await axios.post("http://127.0.0.1:8000/ingest-all");
      // Final poll to capture completed numbers
      await poll();
    } catch (err) {
      console.error(err);
      setStatusMsg("Error during ingestion");
      if (intervalId) clearInterval(intervalId);
      setLoadingIngest(false);
    }
  };

  useEffect(() => {
    // Initial load from DB only
    refreshStocks();
  }, []);

  // Refresh list in real time when threshold or displayCount changes
  useEffect(() => {
    refreshStocks();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threshold, displayCount]);

  // Auto-fetch AI summary when modal opens or when symbol/threshold changes
  useEffect(() => {
    if (!isModalOpen || !selectedSymbol) return;
    let cancelled = false;
    setAiLoading(true);
    setAiError("");
    setAiData(null);
    axios.get(`http://127.0.0.1:8000/reason/${selectedSymbol}`, { params: { threshold } })
      .then((res) => { if (!cancelled) setAiData(res.data); })
      .catch(() => { if (!cancelled) setAiError("Failed to get AI reason."); })
      .finally(() => { if (!cancelled) setAiLoading(false); });
    return () => { cancelled = true; };
  }, [isModalOpen, selectedSymbol, threshold]);

  const loadChart = (symbol) => {
    axios.get(`http://127.0.0.1:8000/chart/${symbol}`)
      .then((res) => {
        const dates = res.data.dates || [];
        const volumes = res.data.volumes || [];

        const formattedDates = dates.map((date) => {
          const d = new Date(date);
          return `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        });

        setChartData({
          labels: formattedDates,
          datasets: [
            {
              type: 'line',
              label: 'Price (Close)',
              data: (res.data.prices || []).map(p => parseFloat(p.toFixed(2))),
              borderColor: '#1d4ed8',
              backgroundColor: 'rgba(29, 78, 216, 0.25)',
              yAxisID: 'y',
              borderWidth: 2,
              tension: 0.3,
              fill: true,
              pointRadius: 2,
              pointBackgroundColor: '#1d4ed8',
              pointBorderColor: '#fff',
              pointBorderWidth: 1
            },
            {
              type: 'line',
              label: 'Volume (20d)',
              data: volumes,
              borderColor: '#f59e0b',
              backgroundColor: 'rgba(245, 158, 11, 0.25)',
              yAxisID: 'y1',
              borderWidth: 2,
              tension: 0.3,
              fill: true,
              pointRadius: 2,
              pointBackgroundColor: '#f59e0b',
              pointBorderColor: '#fff',
              pointBorderWidth: 1
            }
          ]
        });

        // Reset AI state for new modal open
        setAiLoading(true);
        setAiError("");
        setAiData(null);

        setSelectedSymbol(symbol);
        setIsModalOpen(true);
      })
      .catch((err) => console.error("Error loading chart:", err));
  };

  return (
    <div className="dashboard">

      {/* HEADER WITH TITLE AND DROPDOWN */}
      <div className="header-section">
        <h1>Stock Market Dashboard</h1>

        <div className="controls">
          <button onClick={ingestBatch} disabled={loadingIngest}>
            {loadingIngest ? 'Loading All…' : 'Load New Data'}
          </button>
          <button onClick={refreshStocks} disabled={loadingStocks}>
            {loadingStocks ? 'Refreshing…' : 'Refresh Stocks Data'}
          </button>

          <label htmlFor="threshold" style={{ marginLeft: '12px' }}>Threshold %:</label>
          <input
            id="threshold"
            type="number"
            step="0.1"
            min="0"
            value={threshold}
            onChange={(e) => setThreshold(parseFloat(e.target.value) || 0)}
            style={{ width: 90, padding: '8px', borderRadius: 6, border: '2px solid #2c3e50' }}
          />

          <label htmlFor="recordCount" style={{ marginLeft: '12px' }}>Show Records:</label>
          <select
            id="recordCount"
            value={displayCount}
            onChange={(e) => setDisplayCount(Number(e.target.value))}
          >
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={15}>15</option>
            <option value={20}>20</option>
          </select>
        </div>
      </div>

      {statusMsg && (
        <div style={{ marginBottom: '10px', color: loadingIngest ? '#0a58ca' : '#666' }}>{statusMsg}</div>
      )}


      {/* CARDS FIRST */}

      <div className="card-container">
        {stocks.slice(0, displayCount).map((stock) => (
          <div
            key={stock.symbol}
            className="stock-card"
            onClick={() => loadChart(stock.symbol)}
          >
            <div className="stock-card-body">
              <div className="stock-card-header">
                <div>
                  <h3>{stock.symbol}</h3>
                  {stock.company && <div style={{ color: '#555', fontSize: 13, marginTop: 2 }}>{stock.company}</div>}
                </div>
                <span className={stock.price_change > 0 ? "positive" : "negative"}>
                  {stock.price_change}%
                </span>
              </div>
              <div className="stock-card-grid">
                <div className="row"><span>Price</span><strong>${stock.price}</strong></div>
                <div className="row"><span>Today's Volume</span><strong>{stock.today_volume.toLocaleString()}</strong></div>
                <div className="row"><span>Avg Volume (20d)</span><strong>{stock.avg_volume.toLocaleString()}</strong></div>
                <div className="row"><span>Volume Surge</span><strong>{stock.volume_surge}%</strong></div>
                <div className="row"><span>Market Cap (B)</span><strong>{stock.market_cap_billion}</strong></div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {isModalOpen && chartData && (
        <div className="modal-overlay" onClick={() => { setIsModalOpen(false); setAiData(null); setAiError(""); setAiLoading(false); }}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>{selectedSymbol} - 20-Day Volume Trend</h3>
              <div style={{ display: 'flex', gap: 8 }}>
                <button className="close-btn" onClick={() => {
                  setIsModalOpen(false);
                  setAiData(null);
                  setAiError("");
                  setAiLoading(false);
                }}>×</button>
              </div>
            </div>
            <div className="modal-body" style={{ overflowY: 'auto' }}>
              <div style={{ height: 340 }}>
                <div style={{ display: 'flex', gap: 16, marginBottom: 8 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <div style={{ width: 14, height: 14, backgroundColor: '#1d4ed8', borderRadius: 3 }}></div>
                        <span style={{ fontSize: 13, color: '#1d4ed8', fontWeight: 600 }}>Price</span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <div style={{ width: 14, height: 14, backgroundColor: '#f59e0b', borderRadius: 3 }}></div>
                        <span style={{ fontSize: 13, color: '#f59e0b', fontWeight: 600 }}>Volume</span>
                    </div>
                </div>
                <Line
                  data={chartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: { display: false },
                    },
                    scales: {
                      x: { title: { display: true, text: "Date (MM-DD)" } },
                      y: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'Price ($)' },
                        ticks: { callback: (v) => `${v}` }
                      },
                      y1: {
                        type: 'linear',
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Volume' },
                        ticks: {
                          callback: function(value) {
                            if (value >= 1000000) return (value/1000000).toFixed(1) + 'M';
                            if (value >= 1000) return (value/1000).toFixed(0) + 'K';
                            return value;
                          }
                        }
                      }
                    }
                  }}
                />
              </div>

              <div style={{ marginTop: 16 }}>
                <h4 style={{ marginBottom: 8 }}>AI Summary</h4>
                {aiLoading && <div>Generating summary…</div>}
                {aiError && <div style={{ color: '#dc2626' }}>{aiError}</div>}
                {aiData && (
                  <div style={{ whiteSpace: 'pre-wrap', lineHeight: 1.4 }}>{aiData.reason}</div>
                )}
              </div>

              <div style={{ marginTop: 16 }}>
                <h4 style={{ marginBottom: 8 }}>Sources</h4>
                {aiData && aiData.sources && aiData.sources.length > 0 ? (
                  <ul style={{ paddingLeft: 16 }}>
                    {aiData.sources.map((s, i) => (
                      <li key={i} style={{ marginBottom: 6 }}>
                        <a href={s.url} target="_blank" rel="noreferrer">{s.title || s.url}</a>
                        {s.source ? <span style={{ marginLeft: 6, color: '#666' }}>— {s.source}</span> : null}
                        {s.publishedAt ? <span style={{ marginLeft: 6, color: '#999' }}>({s.publishedAt})</span> : null}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <div style={{ color: '#666' }}>No sources yet. Click AI Explain to fetch recent news.</div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}


      {/* TABLE BELOW */}

      <h2 className="table-title">Stock Table</h2>

      <table className="stock-table">

        <thead>
          <tr>
            <th>Symbol</th>
            <th>Company</th>
            <th>Price</th>
            <th>Change %</th>
            <th>Today's Volume</th>
            <th>Avg Volume (20d)</th>
            <th>Market Cap (B)</th>
            <th>Volume Surge %</th>
          </tr>
        </thead>

        <tbody>

          {stocks.slice(0, displayCount).map((stock) => (

            <tr
              key={stock.symbol}
              onClick={() => loadChart(stock.symbol)}
            >

              <td>{stock.symbol}</td>

              <td>{stock.company || '-'}</td>

              <td>${stock.price}</td>

              <td className={stock.price_change > 0 ? "positive" : "negative"}>
                {stock.price_change}%
              </td>

              <td>{stock.today_volume.toLocaleString()}</td>

              <td>{stock.avg_volume.toLocaleString()}</td>

              <td>{stock.market_cap_billion}</td>

              <td>{stock.volume_surge}%</td>

            </tr>

          ))}

        </tbody>

      </table>

    </div>
  );
}

export default StockDashboard;