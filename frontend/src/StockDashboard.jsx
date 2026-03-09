import { useEffect, useState, useRef, useCallback } from "react";
import axios from "axios";
import "./StockDashboard.css";

import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  CategoryScale,
  Filler
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(LineElement, PointElement, LinearScale, CategoryScale, Filler);

const API = "https://stockmarketdashboard-727w.onrender.com";
//const API = "http://127.0.0.1:8000";

const SECTOR_COLORS = {
  "Technology":             "#6366f1",
  "Healthcare":             "#10b981",
  "Financial Services":     "#f59e0b",
  "Consumer Cyclical":      "#f97316",
  "Industrials":            "#3b82f6",
  "Communication Services": "#8b5cf6",
  "Consumer Defensive":     "#14b8a6",
  "Energy":                 "#ef4444",
  "Basic Materials":        "#84cc16",
  "Real Estate":            "#ec4899",
  "Utilities":              "#06b6d4",
};

function getSectorColor(sector) {
  return SECTOR_COLORS[sector] || "#6b7280";
}

// ── Multi-select sector dropdown ──────────────────────────────────────────────
function SectorDropdown({ allSectors, selectedSectors, onChange, onApply, hasChanges }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const toggleSector = (sector) => {
    if (selectedSectors.includes(sector)) {
      onChange(selectedSectors.filter(s => s !== sector));
    } else {
      onChange([...selectedSectors, sector]);
    }
  };

  const clearAll  = () => onChange([]);
  const selectAll = () => onChange([...allSectors]);

  const handleApplyClick = () => {
    onApply();
    setOpen(false);
  };

  const label = selectedSectors.length === 0
    ? "All Sectors"
    : selectedSectors.length === 1
      ? selectedSectors[0]
      : `${selectedSectors.length} sectors`;

  return (
    <div className="sector-dropdown" ref={ref}>
      <button className="sector-dropdown-trigger" onClick={() => setOpen(o => !o)}>
        <span>{label}</span>
        <span className="sector-dropdown-arrow">{open ? "▲" : "▼"}</span>
      </button>

      {open && (
        <div className="sector-dropdown-menu">
          <div className="sector-dropdown-actions">
            <button onClick={selectAll}>All</button>
            <button onClick={clearAll}>Clear</button>
          </div>
          <div className="sector-dropdown-list">
            {allSectors.map(sector => (
              <label key={sector} className="sector-option">
                <input
                  type="checkbox"
                  checked={selectedSectors.includes(sector)}
                  onChange={() => toggleSector(sector)}
                />
                <span className="sector-dot" style={{ backgroundColor: getSectorColor(sector) }} />
                {sector}
              </label>
            ))}
          </div>
          <div className="sector-dropdown-footer">
            <button
              className={`sector-apply-btn ${hasChanges ? "sector-apply-btn-active" : ""}`}
              onClick={handleApplyClick}
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Sort dropdown (single-select with radio buttons) ──────────────────────
const SORT_OPTIONS = [
  { key: "symbol",             label: "Symbol" },
  { key: "price",              label: "Price" },
  { key: "price_change",       label: "Change %" },
  { key: "today_volume",       label: "Today's Volume" },
  { key: "avg_volume",         label: "Avg Volume (20d)" },
  { key: "market_cap_billion", label: "Market Cap" },
  { key: "volume_surge",       label: "Volume Surge %" },
];

function SortDropdown({ sortColumn, sortDirection, onSortChange }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const handleSelect = (key) => {
    onSortChange(key, sortDirection);
  };

  const handleToggleDirection = (e) => {
    e.stopPropagation();
    const newDir = sortDirection === "asc" ? "desc" : "asc";
    onSortChange(sortColumn, newDir);
  };

  const handleClear = () => {
    onSortChange(null, "asc");
    setOpen(false);
  };

  const activeOption = SORT_OPTIONS.find(o => o.key === sortColumn);
  const label = activeOption
    ? `${activeOption.label} ${sortDirection === "asc" ? "↑" : "↓"}`
    : "Sort By";

  return (
    <div className="sort-dropdown" ref={ref}>
      <button className="sort-dropdown-trigger" onClick={() => setOpen(o => !o)}>
        <span>{label}</span>
        <span className="sort-dropdown-arrow">{open ? "▲" : "▼"}</span>
      </button>

      {open && (
        <div className="sort-dropdown-menu">
          <div className="sort-dropdown-header">
            <span>Sort By</span>
            <button className="sort-clear-btn" onClick={handleClear}>Clear</button>
          </div>

          {/* ASC / DESC toggle */}
          <div className="sort-direction-toggle">
            <button
              className={`sort-dir-btn ${sortDirection === "asc" ? "sort-dir-btn-active" : ""}`}
              onClick={() => onSortChange(sortColumn, "asc")}
            >
              ↑ Ascending
            </button>
            <button
              className={`sort-dir-btn ${sortDirection === "desc" ? "sort-dir-btn-active" : ""}`}
              onClick={() => onSortChange(sortColumn, "desc")}
            >
              ↓ Descending
            </button>
          </div>

          <div className="sort-dropdown-list">
            {SORT_OPTIONS.map(opt => {
              const isActive = sortColumn === opt.key;
              return (
                <label key={opt.key}
                  className={`sort-option ${isActive ? "sort-option-active" : ""}`}
                  onClick={() => handleSelect(opt.key)}>
                  <input
                    type="radio"
                    name="sort-column"
                    checked={isActive}
                    readOnly
                  />
                  <span className="sort-option-label">{opt.label}</span>
                </label>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Stock card ────────────────────────────────────────────────────────────────
function StockCard({ stock, onClick, index }) {
  const [insight, setInsight]               = useState("");
  const [insightLoading, setInsightLoading] = useState(false);
  const [insightFetched, setInsightFetched] = useState(false);

  useEffect(() => {
    if (insightFetched) return;
    const delay = index * 2000;
    const timer = setTimeout(() => {
      setInsightLoading(true);
      axios.get(`${API}/brief-insight/${stock.symbol}`, {
        params: {
          price:              stock.price,
          price_change:       stock.price_change,
          volume_surge:       stock.volume_surge,
          market_cap_billion: stock.market_cap_billion,
        }
      })
        .then((res) => setInsight(res.data.insight || ""))
        .catch(() => setInsight(""))
        .finally(() => { setInsightLoading(false); setInsightFetched(true); });
    }, delay);
    return () => clearTimeout(timer);
  }, [stock.symbol, insightFetched]);

  return (
    <div className="stock-card" onClick={onClick}>
      <div className="stock-card-body">
        <div className="stock-card-header">
          <div>
            <h3>{stock.symbol}</h3>
            {stock.company && <div className="stock-card-company">{stock.company}</div>}
          </div>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "4px" }}>
            <span className={stock.price_change > 0 ? "badge positive" : "badge negative"}>
              {stock.price_change > 0 ? '+' : ''}{stock.price_change}%
            </span>
            {stock.sector && (
              <span className="sector-badge" style={{ backgroundColor: getSectorColor(stock.sector) }}>
                {stock.sector}
              </span>
            )}
          </div>
        </div>

        <div className="stock-card-grid">
          <div className="row"><span>Price</span><strong>${stock.price}</strong></div>
          <div className="row"><span>Today's Volume</span><strong>{stock.today_volume.toLocaleString()}</strong></div>
          <div className="row"><span>Avg Volume (20d)</span><strong>{stock.avg_volume.toLocaleString()}</strong></div>
          <div className="row"><span>Volume Surge</span><strong className="surge-value">{stock.volume_surge}%</strong></div>
          <div className="row"><span>Market Cap (B)</span><strong>${stock.market_cap_billion}</strong></div>
        </div>

        <div className="card-insight">
          {insightLoading ? (
            <div className="card-insight-loading">
              <div className="spinner-tiny"></div>
              <span>Generating insight…</span>
            </div>
          ) : insight ? (
            <div className="card-insight-text">
              {insight}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────────
function StockDashboard() {
  const [stocks, setStocks]                     = useState([]);
  const [allSectors, setAllSectors]             = useState([]);
  // pendingSectors = what user is selecting in dropdown (not yet applied)
  const [pendingSectors, setPendingSectors]     = useState([]);
  // appliedSectors = what was last applied via Apply button
  const [appliedSectors, setAppliedSectors]     = useState([]);
  const [displayCount, setDisplayCount]         = useState(10);
  const [threshold, setThreshold]               = useState(1.5);
  const [chartData, setChartData]               = useState(null);
  const [selectedSymbol, setSelectedSymbol]     = useState(null);
  const [isModalOpen, setIsModalOpen]           = useState(false);
  const [loadingStocks, setLoadingStocks]       = useState(true);
  const [aiLoading, setAiLoading]               = useState(false);
  const [aiError, setAiError]                   = useState("");
  const [aiData, setAiData]                     = useState(null);
  const [sortColumn, setSortColumn]             = useState(null);
  const [sortDirection, setSortDirection]        = useState("asc");

  // Load sectors from DB once on mount
  useEffect(() => {
    axios.get(`${API}/sectors`)
      .then(res => setAllSectors(res.data || []))
      .catch(() => {});
  }, []);

  // Fetch stocks — only re-runs when threshold or appliedSectors changes
  // (NOT pendingSectors — dropdown changes don't trigger refresh)
  const fetchStocks = useCallback((sectorsToApply) => {
    setLoadingStocks(true);
    const searchParams = new URLSearchParams();
    searchParams.append("threshold", threshold);
    searchParams.append("limit", 500); // fetch all matching, slice in frontend
    sectorsToApply.forEach(s => searchParams.append("sectors", s));

    axios.get(`${API}/stocks?${searchParams.toString()}`)
      .then((res) => setStocks(res.data))
      .catch((err) => console.error(err))
      .finally(() => setLoadingStocks(false));
  }, [threshold]);

  // Initial load and threshold change
  useEffect(() => {
    fetchStocks(appliedSectors);
  }, [threshold]); // eslint-disable-line react-hooks/exhaustive-deps

  // Apply button handler — applies sector selection and refreshes
  const handleApply = () => {
    setAppliedSectors([...pendingSectors]);
    fetchStocks(pendingSectors);
  };

  // Refresh button — re-fetches with currently applied sectors
  const handleRefresh = () => {
    fetchStocks(appliedSectors);
  };

  // Check if pending differs from applied (to highlight Apply button)
  const hasUnappliedChanges =
    JSON.stringify(pendingSectors.sort()) !== JSON.stringify(appliedSectors.sort());

  // Auto-fetch full AI reason when modal opens
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
        const formattedDates = dates.map((date) => {
          const d = new Date(date);
          return `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
        });
        setChartData({
          labels: formattedDates,
          datasets: [
            {
              type: 'line', label: 'Price (Close)',
              data: (res.data.prices || []).map(p => parseFloat(p.toFixed(2))),
              borderColor: '#1d4ed8', backgroundColor: 'rgba(29,78,216,0.25)',
              yAxisID: 'y', borderWidth: 2, tension: 0.3, fill: true,
              pointRadius: 2, pointBackgroundColor: '#1d4ed8',
            },
            {
              type: 'line', label: 'Volume',
              data: res.data.volumes || [],
              borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,0.25)',
              yAxisID: 'y1', borderWidth: 2, tension: 0.3, fill: true,
              pointRadius: 2, pointBackgroundColor: '#f59e0b',
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

  const closeModal = () => {
    setIsModalOpen(false);
    setAiData(null);
    setAiError("");
    setAiLoading(false);
  };

  // ── Sorting logic ──────────────────────────────────────────────────────────
  const handleSortChange = (column, direction) => {
    setSortColumn(column);
    setSortDirection(direction);
  };

  // Slice first, then sort the visible slice
  const sliced = stocks.slice(0, displayCount);
  const visibleStocks = sortColumn
    ? [...sliced].sort((a, b) => {
        let valA = a[sortColumn];
        let valB = b[sortColumn];
        // For symbol, do string compare
        if (sortColumn === "symbol") {
          valA = (valA || "").toString().toLowerCase();
          valB = (valB || "").toString().toLowerCase();
          return sortDirection === "asc"
            ? valA.localeCompare(valB)
            : valB.localeCompare(valA);
        }
        // Numeric compare for everything else
        valA = Number(valA) || 0;
        valB = Number(valB) || 0;
        return sortDirection === "asc" ? valA - valB : valB - valA;
      })
    : sliced;

  const dataReady = !loadingStocks && stocks.length > 0;

  return (
    <div className="dashboard">

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

          <button onClick={handleRefresh} disabled={loadingStocks}>
            {loadingStocks ? 'Refreshing…' : 'Refresh'}
          </button>

          <div className="control-group">
            <label htmlFor="threshold">Threshold %</label>
            <input
              id="threshold" type="number" step="0.1" min="0" value={threshold}
              onChange={(e) => setThreshold(parseFloat(e.target.value) || 0)}
            />
          </div>

          <div className="control-group">
            <label htmlFor="recordCount">Records</label>
            <select id="recordCount" value={displayCount}
              onChange={(e) => setDisplayCount(Number(e.target.value))}>
              <option value={5}>5</option>
              <option value={10}>10</option>
              <option value={15}>15</option>
              <option value={20}>20</option>
              <option value={50}>50</option>
            </select>
          </div>

          <div className="control-group">
            <label></label>
            <SectorDropdown
              allSectors={allSectors}
              selectedSectors={pendingSectors}
              onChange={setPendingSectors}
              onApply={handleApply}
              hasChanges={hasUnappliedChanges}
            />
          </div>

          <div className="control-group">
            <label></label>
            <SortDropdown
              sortColumn={sortColumn}
              sortDirection={sortDirection}
              onSortChange={handleSortChange}
            />
          </div>

        </div>

        {/* Applied sector tags */}
        {appliedSectors.length > 0 && (
          <div className="active-sector-tags">
            <span className="active-filter-label">Filtered by:</span>
            {appliedSectors.map(s => (
              <span key={s} className="sector-tag"
                style={{ borderColor: getSectorColor(s), color: getSectorColor(s) }}>
                {s}
                <button onClick={() => {
                  const updated = appliedSectors.filter(x => x !== s);
                  setAppliedSectors(updated);
                  setPendingSectors(updated);
                  fetchStocks(updated);
                }}>×</button>
              </span>
            ))}
          </div>
        )}

        {!loadingStocks && (
          <div className="result-count">
          </div>
        )}
      </div>

      {/* NO DATA STATE */}
      {!loadingStocks && stocks.length === 0 && (
        <div className="empty-state">
          <div className="empty-state-icon">📊</div>
          <h2>No Stocks Found</h2>
          <p>No stocks match the current filters. Try lowering the threshold or selecting different sectors.</p>
        </div>
      )}

      {/* CARDS */}
      {dataReady && (
        <div className="card-container">
          {visibleStocks.map((stock, index) => (
            <StockCard key={stock.symbol} stock={stock} index={index} onClick={() => loadChart(stock.symbol)} />
          ))}
        </div>
      )}

      {/* CHART MODAL */}
      {isModalOpen && chartData && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>{selectedSymbol} — 20-Day Volume Trend</h3>
              <button className="close-btn" onClick={closeModal}>×</button>
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
                <Line data={chartData} options={{
                  responsive: true, maintainAspectRatio: false,
                  plugins: { legend: { display: false } },
                  scales: {
                    x: { title: { display: true, text: "Date (MM-DD)" }, ticks: { maxRotation: 45, minRotation: 0, font: { size: 11 } } },
                    y: { type: 'linear', position: 'left', title: { display: true, text: 'Price ($)' }, ticks: { callback: (v) => `$${v}` } },
                    y1: {
                      type: 'linear', position: 'right', grid: { drawOnChartArea: false },
                      title: { display: true, text: 'Volume' },
                      ticks: { callback: (v) => v >= 1000000 ? (v/1000000).toFixed(1)+'M' : v >= 1000 ? (v/1000).toFixed(0)+'K' : v }
                    }
                  }
                }} />
              </div>
              <div className="ai-section">
                <h4>AI Summary</h4>
                {aiLoading && <div className="ai-loading"><div className="spinner-small"></div><span>Generating summary…</span></div>}
                {aiError && <div className="ai-error">{aiError}</div>}
                {aiData && <div className="ai-reason">{aiData.reason}</div>}
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
                ) : (!aiLoading && <div className="no-sources">No sources yet.</div>)}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TABLE */}
      {dataReady && (
        <div className="table-wrapper">
          <table className="stock-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Company</th>
                <th>Sector</th>
                <th>Price</th>
                <th>Change %</th>
                <th>Today's Vol</th>
                <th>Avg Vol (20d)</th>
                <th>Mkt Cap (B)</th>
                <th>Vol Surge %</th>
              </tr>
            </thead>
            <tbody>
              {visibleStocks.map((stock) => (
                <tr key={stock.symbol} onClick={() => loadChart(stock.symbol)}>
                  <td className="symbol-cell">{stock.symbol}</td>
                  <td>{stock.company || '—'}</td>
                  <td>
                    {stock.sector ? (
                      <span className="sector-badge" style={{ backgroundColor: getSectorColor(stock.sector) }}>
                        {stock.sector}
                      </span>
                    ) : '—'}
                  </td>
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
      )}
    </div>
  );
}

export default StockDashboard;