import { useMemo, useState } from "react";

export default function Dashboard() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const fileName = useMemo(() => selectedFile?.name ?? "", [selectedFile]);

  function onFileChange(e) {
    setError("");
    setResult(null);

    const f = e.target.files?.[0] ?? null;
    if (!f) {
      setSelectedFile(null);
      return;
    }

    // Optional guardrails ✅
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
    form.append("file", selectedFile); // MUST match backend: request.FILES.get("file")

    try {
      setUploading(true);

      // If you have Vite proxy, this works:
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

  return (
    <div style={{ padding: 24, fontFamily: "sans-serif", maxWidth: 900 }}>
      <h1>Dashboard</h1>

      <section style={{ marginTop: 16, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <h2 style={{ marginTop: 0 }}>Upload CSV 📤</h2>

        <input type="file" accept=".csv,text/csv" onChange={onFileChange} />

        <div style={{ marginTop: 12, display: "flex", gap: 12, alignItems: "center" }}>
          <button onClick={uploadCsv} disabled={uploading || !selectedFile}>
            {uploading ? "Uploading..." : "Upload"}
          </button>

          {fileName ? <span>Selected: <strong>{fileName}</strong></span> : <span>No file selected</span>}
        </div>

        {error ? (
          <div style={{ marginTop: 12, color: "crimson" }}>
            ❌ {error}
          </div>
        ) : null}

        {result ? (
          <div style={{ marginTop: 16 }}>
            <h3>Import Result ✅</h3>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
              <Stat label="Rows in file" value={result.rows_in_file} />
              <Stat label="Rows valid" value={result.rows_valid} />
              <Stat label="Rows invalid" value={result.rows_invalid} />
              <Stat label="Rows upserted" value={result.rows_upserted} />
            </div>

            <details style={{ marginTop: 12 }}>
              <summary>Raw JSON</summary>
              <pre style={{ background: "#f7f7f7", padding: 12, borderRadius: 8, overflowX: "auto" }}>
                {JSON.stringify(result, null, 2)}
              </pre>
            </details>
          </div>
        ) : null}
      </section>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div style={{ padding: 12, border: "1px solid #eee", borderRadius: 12 }}>
      <div style={{ fontSize: 12, opacity: 0.7 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value ?? "-"}</div>
    </div>
  );
}
