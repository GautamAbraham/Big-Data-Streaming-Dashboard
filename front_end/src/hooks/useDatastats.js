import { useRef, useEffect } from "react";

export function useDataStats(geojson, setDataStats, intervalMs = 5000) {
  const geojsonRef = useRef(geojson);

  // keep the ref pointing at the latest geojson
  useEffect(() => {
    geojsonRef.current = geojson;
  }, [geojson]);

  useEffect(() => {
    const id = setInterval(() => {
      const feats = geojsonRef.current.features || [];
      
      // Early return if no features to avoid unnecessary processing
      if (feats.length === 0) return;

      const { high, medium, low } = feats.reduce(
        (acc, { properties: { level } }) => {
          switch (level) {
            case "very-high":
            case "high": acc.high++; break;
            case "moderate": acc.medium++; break;
            default: acc.low++;
          }
          return acc;
        },
        { high: 0, medium: 0, low: 0 }
      );

      const timestamp = feats[feats.length - 1].properties?.timestamp || null;

      setDataStats((prev) => ({ ...prev, high, medium, low, lastUpdate: timestamp }));
    }, intervalMs);

    return () => clearInterval(id);
  }, [setDataStats, intervalMs]);
}
