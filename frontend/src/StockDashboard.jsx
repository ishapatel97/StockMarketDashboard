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
const API = "https://stockmarketdashboard-727w.onrender.com"; //productions frontend URL
//const API = "http://127.0.0.1:8000"; //local frontned URL

function StockDashboard() {
  const [stocks, setStocks] = useState([]);
  const [displayCount, setDisplayCount] = useState(10);
  const [threshold, setThreshold] = useState(1.5);
  const [chartData, setChartData] = useState(null);
  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [loadingStocks, setLoadingStocks] = useState(true);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState("");
  const [aiData, setAiData] = useState(null);

  const refreshStocks = () => {
    setLoadingStocks(true);
    axios.get(`${API}/stocks`, { params: { threshold, limit: displayCount } })
      .then((res) => setStocks(res.data))
      .catch((err) => console.error(err))
      .finally(() => setLoadingStocks(false));
  };

  useEffect(() => {
    refreshStocks();
  }, []);

  useEffect(() => {
    refreshStocks();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threshold, displayCount]);

  // Auto-fetch AI summary when modal opens
  useEffect(() => {
    if (!isModalOpen || !selectedSymbol) return;
    let cancelled = false;
    setAiLoading(true);
    setAiError("");
    setAiData(null);
    axios.get(`${API}/reason/${selectedSymbol}`, { params: { threshold } })
      .then((res) => { if (!cancelled) setAiData(res.data); })
      .catch(() => { if (!cancelled) setAiError("Failed to get AI reason."); })
      .finally(() => { if (!cancelled) setAiLoading(false); });
    return () => { cancelled = true; };
  }, [isModalOpen, selectedSymbol, threshold]);

  const loadChart = (symbol) => {
    axios.get(`${API}/chart/${symbol}`)
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

        setAiLoading(true);
        setAiError("");
        setAiData(null);
        setSelectedSymbol(symbol);
        setIsModalOpen(true);
      })
      .catch((err) => console.error("Error loading chart:", err));
  };

  const dataReady = !loadingStocks && stocks.length > 0;

  return (
    <div className="dashboard">

      {/* LOADING OVERLAY */}
      {loadingStocks && (
        <div className="loading-overlay">
          <div className="loading-popup">
            <div className="spinner"></div>
            <h2>Loading Stock Data</h2>
            <p>Fetching the latest market data…</p>
          </div>
        </div>
      )}

      {/* HEADER */}
      <div className="header-section">
        <h1>Stock Market Dashboard</h1>

        <div className="controls">
          <button onClick={refreshStocks} disabled={loadingStocks}>
            {loadingStocks ? 'Refreshing…' : 'Refresh'}
          </button>

          <div className="control-group">
            <label htmlFor="threshold">Threshold %</label>
            <input
              id="threshold"
              type="number"
              step="0.1"
              min="0"
              value={threshold}
              onChange={(e) => setThreshold(parseFloat(e.target.value) || 0)}
            />
          </div>

          <div className="control-group">
            <label htmlFor="recordCount">Records</label>
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
      </div>

      {/* NO DATA STATE */}
      {!loadingStocks && stocks.length === 0 && (
        <div className="empty-state">
          <div className="empty-state-icon">📊</div>
          <h2>No Stocks Found</h2>
          <p>No stocks match the current threshold. Try lowering the threshold or refreshing.</p>
        </div>
      )}

      {/* CARDS — only show when data is ready */}
      {dataReady && (
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
                    {stock.company && <div className="stock-card-company">{stock.company}</div>}
                  </div>
                  <span className={stock.price_change > 0 ? "badge positive" : "badge negative"}>
                    {stock.price_change > 0 ? '+' : ''}{stock.price_change}%
                  </span>
                </div>
                <div className="stock-card-grid">
                  <div className="row"><span>Price</span><strong>${stock.price}</strong></div>
                  <div className="row"><span>Today's Volume</span><strong>{stock.today_volume.toLocaleString()}</strong></div>
                  <div className="row"><span>Avg Volume (20d)</span><strong>{stock.avg_volume.toLocaleString()}</strong></div>
                  <div className="row"><span>Volume Surge</span><strong className="surge-value">{stock.volume_surge}%</strong></div>
                  <div className="row"><span>Market Cap (B)</span><strong>${stock.market_cap_billion}</strong></div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* CHART MODAL */}
      {isModalOpen && chartData && (
        <div className="modal-overlay" onClick={() => { setIsModalOpen(false); setAiData(null); setAiError(""); setAiLoading(false); }}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>{selectedSymbol} — 20-Day Volume Trend</h3>
              <button className="close-btn" onClick={() => {
                setIsModalOpen(false);
                setAiData(null);
                setAiError("");
                setAiLoading(false);
              }}>×</button>
            </div>
            <div className="modal-body">
              <div className="chart-wrapper">
                <div className="chart-legend">
                  <div className="legend-item">
                    <div className="legend-swatch" style={{ backgroundColor: '#1d4ed8' }}></div>
                    <span style={{ color: '#1d4ed8' }}>Price</span>
                  </div>
                  <div className="legend-item">
                    <div className="legend-swatch" style={{ backgroundColor: '#f59e0b' }}></div>
                    <span style={{ color: '#f59e0b' }}>Volume</span>
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
                      x: {
                        title: { display: true, text: "Date (MM-DD)" },
                        ticks: { maxRotation: 45, minRotation: 0, font: { size: 11 } }
                      },
                      y: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'Price ($)' },
                        ticks: { callback: (v) => `$${v}` }
                      },
                      y1: {
                        type: 'linear',
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Volume' },
                        ticks: {
                          callback: function(value) {
                            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
                            if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
                            return value;
                          }
                        }
                      }
                    }
                  }}
                />
              </div>

              <div className="ai-section">
                <h4>AI Summary</h4>
                {aiLoading && (
                  <div className="ai-loading">
                    <div className="spinner-small"></div>
                    <span>Generating summary…</span>
                  </div>
                )}
                {aiError && <div className="ai-error">{aiError}</div>}
                {aiData && (
                  <div className="ai-reason">{aiData.reason}</div>
                )}
              </div>

              <div className="sources-section">
                <h4>Sources</h4>
                {aiData && aiData.sources && aiData.sources.length > 0 ? (
                  <ul className="sources-list">
                    {aiData.sources.map((s, i) => (
                      <li key={i}>
                        <a href={s.url} target="_blank" rel="noreferrer">{s.title || s.url}</a>
                        {s.source && <span className="source-name">— {s.source}</span>}
                        {s.publishedAt && <span className="source-date">({s.publishedAt})</span>}
                      </li>
                    ))}
                  </ul>
                ) : (
                  !aiLoading && <div className="no-sources">No sources yet.</div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TABLE — only show when data is ready */}
      {dataReady && (
        <>
          <h2 className="table-title">Stock Table</h2>
          <div className="table-wrapper">
            <table className="stock-table">
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Company</th>
                  <th>Price</th>
                  <th>Change %</th>
                  <th>Today's Vol</th>
                  <th>Avg Vol (20d)</th>
                  <th>Mkt Cap (B)</th>
                  <th>Vol Surge %</th>
                </tr>
              </thead>
              <tbody>
                {stocks.slice(0, displayCount).map((stock) => (
                  <tr key={stock.symbol} onClick={() => loadChart(stock.symbol)}>
                    <td className="symbol-cell">{stock.symbol}</td>
                    <td>{stock.company || '—'}</td>
                    <td>${stock.price}</td>
                    <td className={stock.price_change > 0 ? "positive" : "negative"}>
                      {stock.price_change > 0 ? '+' : ''}{stock.price_change}%
                    </td>
                    <td>{stock.today_volume.toLocaleString()}</td>
                    <td>{stock.avg_volume.toLocaleString()}</td>
                    <td>${stock.market_cap_billion}</td>
                    <td className="surge-value">{stock.volume_surge}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

export default StockDashboard;
