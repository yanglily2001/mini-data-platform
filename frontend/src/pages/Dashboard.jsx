import { useMemo, useState, useEffect } from "react";
import { StationSelector } from "./StationSelector";

export default function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [stationId, setStationId] = useState("");
  const [dailyData, setDailyData] = useState([]);

  const fileName = useMemo(() => selectedFile?.name ?? "", [selectedFile]);

  function onFileChange(e) {
    setError("");
    setResult(null);

    const f = e.target.files?.[0] ?? null;
    if (!f) {
      setSelectedFile(null);
      return;
    }

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
    form.append("file", selectedFile); // MUST match backend key: request.FILES.get("file")

    try {
      setUploading(true);
      const resp = await fetch("/api/import/", {
        method: "POST",
        body: form,
      });

      // If you do NOT have proxy, use:
      // const resp = await fetch("http://localhost:8000/api/import/", { method:"POST", body: form });

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

  // Daily metrics
  fetch(`/api/metrics/daily/?station_id=${encodeURIComponent(stationId)}`)
    .then(async (r) => {
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    })
    .then((data) => setDailyData(data.data ?? []))
    .catch((e) => {
      console.error("Failed to load daily metrics:", e);
      setDailyData([]);
    });

  // Summary metrics
  fetch(`/api/metrics/summary/?station_id=${encodeURIComponent(stationId)}`)
    .then(async (r) => {
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    })
    .then((data) => setSummary(data))
    .catch((e) => {
      console.error("Failed to load summary:", e);
      setSummary(null);
    });

}, [stationId]);

  return (
    <div style={{ padding: 24, fontFamily: "sans-serif", maxWidth: 900 }}>
      <h1>Dashboard</h1>

      <StationSelector value={stationId} onChange={setStationId} />

       {stationId ? (
        <div style={{ marginTop: 16 }}>
          <h2>Daily metrics 📈</h2>
          <pre
            style={{
              background: "#f7f7f7",
              padding: 12,
              borderRadius: 8,
              overflowX: "auto",
            }}
          >
            {JSON.stringify(dailyData, null, 2)}
          </pre>
        </div>
      ) : (
        <div style={{ marginTop: 16, opacity: 0.7 }}>
          Select a station to load daily metrics 👆
        </div>
      )}

      {stationId ? (
        <section style={{ marginTop: 16 }}>
          <h2 style={{ marginBottom: 8 }}>Summary metrics 🧮</h2>
      
          {summary ? (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12 }}>
              <Stat
                label="Avg temp (°C)"
                value={summary.avg_temp_c != null ? summary.avg_temp_c.toFixed(2) : "-"}
              />
              <Stat
                label="Total precip (mm)"
                value={summary.total_precip_mm != null ? summary.total_precip_mm.toFixed(2) : "0.00"}
              />
              <Stat
                label="Total rows"
                value={summary.total_rows ?? 0}
              />
            </div>
          ) : (
            <div style={{ opacity: 0.7 }}>Loading summary… ⏳</div>
          )}
        </section>
      ) : null}

      {/* ======================
          CSV upload section
         ====================== */}
      <section
        style={{
          marginTop: 24,
          padding: 16,
          border: "1px solid #ddd",
          borderRadius: 12,
        }}
      >
        <h2 style={{ marginTop: 0 }}>Upload CSV 📤</h2>

        <input
          type="file"
          accept=".csv,text/csv"
          onChange={onFileChange}
        />

        <div
          style={{
            marginTop: 12,
            display: "flex",
            gap: 12,
            alignItems: "center",
          }}
        >
          <button
            onClick={uploadCsv}
            disabled={uploading || !selectedFile}
          >
            {uploading ? "Uploading..." : "Upload"}
          </button>

          {fileName ? (
            <span>
              Selected: <strong>{fileName}</strong>
            </span>
          ) : (
            <span>No file selected</span>
          )}
        </div>

        {error && (
          <div style={{ marginTop: 12, color: "crimson" }}>
            ❌ {error}
          </div>
        )}

        {result && (
          <div style={{ marginTop: 16 }}>
            <h3>Import Result</h3>

            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                gap: 12,
              }}
            >
              <Stat label="Rows in file" value={result.rows_in_file} />
              <Stat label="Rows valid" value={result.rows_valid} />
              <Stat label="Rows invalid" value={result.rows_invalid} />
              <Stat label="Rows upserted" value={result.rows_upserted} />
            </div>

            <details style={{ marginTop: 12 }}>
              <summary>Raw JSON</summary>
              <pre
                style={{
                  background: "#f7f7f7",
                  padding: 12,
                  borderRadius: 8,
                  overflowX: "auto",
                }}
              >
                {JSON.stringify(result, null, 2)}
              </pre>
            </details>
          </div>
        )}
      </section>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div
      style={{
        padding: 12,
        border: "1px solid #eee",
        borderRadius: 12,
      }}
    >
      <div style={{ fontSize: 12, opacity: 0.7 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>
        {value ?? "-"}
      </div>
    </div>
  );
}

  return (
    <div style={{ display: "flex", gap: 12, alignItems: "center", marginTop: 12 }}>
      <label style={{ fontWeight: 600 }}>Station</label>

      {loading ? (
        <span>Loading…</span>
      ) : error ? (
        <span style={{ color: "crimson" }}>❌ {error}</span>
      ) : (
        <select value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">-- Select a station --</option>
          {stations.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      )}
    </div>
  );
}
