import { useEffect, useState } from "react";

export function StationSelector({ value, onChange }) {
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    async function loadStations() {
      try {
        setLoading(true);
        setError("");

        const res = await fetch("/api/stations/");
        if (!res.ok) throw new Error(`Failed to load stations (${res.status})`);

        const data = await res.json();
        if (!cancelled) setStations(data.stations ?? []);
      } catch (e) {
        if (!cancelled) setError(e?.message ?? "Failed to load stations");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadStations();
    return () => {
      cancelled = true;
    };
  }, []);

  if (loading) return <div style={{ marginTop: 12 }}>Loading stations…</div>;
  if (error) return <div style={{ marginTop: 12, color: "crimson" }}>❌ {error}</div>;

  return (
    <div style={{ marginTop: 12, display: "flex", gap: 12, alignItems: "center" }}>
      <label style={{ fontWeight: 600 }}>Station</label>
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="">-- Select a station --</option>
        {stations.map((s) => (
          <option key={s} value={s}>{s}</option>
        ))}
      </select>
    </div>
  );
}
