import { useMemo, useState, useEffect } from "react";
import { StationSelector } from "./StationSelector";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
} from "recharts";

export default function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [stationId, setStationId] = useState("");
  const [dailyData, setDailyData] = useState([]);
  const [summary, setSummary] = useState(null);
  const chartData = dailyData.map((r) => ({
    ...r,
    temp_c: r.temp_c == null ? null : Number(r.temp_c),
    precip_mm: r.precip_mm == null ? null : Number(r.precip_mm),
  }));

  const fileName = useMemo(() => selectedFile?.name ?? "", [selectedFile]);

  function onFileChange(e) {
    setError("");
    setResult(null);

    const f = e.target.files?.[0] ?? null;
    if (!f) return setSelectedFile(null);

    if (!f.name.toLowerCase().endsWith(".csv")) {
      setSelectedFile(null);
      setError("Please select a .csv file.");
      return;
    }

    setSelectedFile(f);
  }

  async function uploadCsv() {
    setError("");
    setResult(null);

    if (!selectedFile) {
      setError("Choose a CSV file first.");
      return;
    }

    const form = new FormData();
    form.append("file", selectedFile);

    try {
      setUploading(true);
      const resp = await fetch("/api/import/", {
        method: "POST",
        body: form,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`Upload failed (${resp.status}): ${text}`);
      }

      const data = await resp.json();
      setResult(data);
    } catch (e) {
      setError(e?.message ?? "Upload failed.");
    } finally {
      setUploading(false);
    }
  }

  useEffect(() => {
    if (!stationId) {
      setDailyData([]);
      setSummary(null);
      return;
    }

    fetch(`/api/metrics/daily/?station_id=${encodeURIComponent(stationId)}`)
      .then((r) => r.json())
      .then((data) => {
        console.log("daily metrics response:", data); // 👈
        setDailyData(data.data ?? []);
      })
      .catch((e) => {
        console.error("daily metrics fetch failed:", e);
        setDailyData([]);
      });

    fetch(`/api/metrics/summary/?station_id=${encodeURIComponent(stationId)}`)
      .then((r) => r.json())
      .then((data) => setSummary(data))
      .catch(() => setSummary(null));
  }, [stationId]);

  return (
    <div style={{ padding: 24, fontFamily: "sans-serif", maxWidth: 900 }}>
      <h1>Dashboard</h1>

      <StationSelector value={stationId} onChange={setStationId} />

      {summary && (
        <div style={{ display: "flex", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
          <Stat label="Average Temp (°C)" value={summary.avg_temp_c} />
          <Stat label="Total Precip (mm)" value={summary.total_precip_mm} />
          <Stat label="Total Rows" value={summary.total_rows} />
        </div>
      )}

      {stationId && (
        <div style={{ marginTop: 24 }}>
          <h2>Daily Metrics 📊</h2>
      
          {dailyData.length === 0 ? (
            <div style={{ opacity: 0.7 }}>No data available.</div>
          ) : (
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                marginTop: 12,
              }}
            >
              <thead>
                <tr style={{ background: "#f3f3f3" }}>
                  <th style={cellHeader}>Date</th>
                  <th style={cellHeader}>Temp (°C)</th>
                  <th style={cellHeader}>Precip (mm)</th>
                </tr>
              </thead>
              <tbody>
                {dailyData.map((row) => (
                  <tr key={row.date}>
                    <td style={cell}>{row.date}</td>
                    <td style={cell}>{row.temp_c}</td>
                    <td style={cell}>{row.precip_mm}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      <section style={{ marginTop: 24, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <h2>Upload CSV 📤</h2>

        <input type="file" accept=".csv,text/csv" onChange={onFileChange} />

        <div style={{ marginTop: 12, display: "flex", gap: 12 }}>
          <button onClick={uploadCsv} disabled={uploading || !selectedFile}>
            {uploading ? "Uploading..." : "Upload"}
          </button>

          {fileName && (
            <span>
              Selected: <strong>{fileName}</strong>
            </span>
          )}
        </div>

        {error && <div style={{ marginTop: 12, color: "crimson" }}>❌ {error}</div>}

        {result && (
          <div style={{ marginTop: 16, display: "flex", gap: 16, flexWrap: "wrap" }}>
            <Stat label="Rows in file" value={result.rows_in_file} />
            <Stat label="Rows valid" value={result.rows_valid} />
            <Stat label="Rows invalid" value={result.rows_invalid} />
            <Stat label="Rows upserted" value={result.rows_upserted} />
          </div>
        )}
      </section>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div style={{ padding: 12, border: "1px solid #eee", borderRadius: 12, minWidth: 160 }}>
      <div style={{ fontSize: 12, opacity: 0.7 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value ?? "-"}</div>
    </div>
  );
}

const cellHeader = {
  padding: "8px",
  border: "1px solid #ddd",
  textAlign: "left",
};

const cell = {
  padding: "8px",
  border: "1px solid #ddd",
};

const thStyle = {
  padding: 10,
  borderBottom: "1px solid #ddd",
  textAlign: "left",
  fontWeight: 600,
};

const tdStyle = {
  padding: 10,
  borderBottom: "1px solid #eee",
};

