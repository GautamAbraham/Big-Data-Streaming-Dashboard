import { useRef, useEffect } from "react";

export function useDataStats(geojson, setDataStats, intervalMs = 5000) {
  const geojsonRef = useRef(geojson);

  // keep the ref pointing at the latest geojson
  useEffect(() => {
    geojsonRef.current = geojson;
  }, [geojson]);

  useEffect(() => {
    const id = setInterval(() => {

      let timestamp = null;
      const feats = geojsonRef.current.features || [];

      const { high, medium, low } = feats.reduce(
        (acc, { properties: { level } }) => {
          if (level === "very-high" || level === "high") {
            acc.high++;
          } 
          else if (level === "moderate") {
            acc.medium++;
          } 
          else acc.low++;
          return acc;
        },
        { high: 0, medium: 0, low: 0 }
      );

      if (feats.length > 0) {
        timestamp = feats[feats.length - 1].properties?.timestamp || null;
      }

      setDataStats((prev) => ({ ...prev, high, medium, low, lastUpdate: timestamp }));
    }, intervalMs);

    return () => clearInterval(id);
  }, [setDataStats, intervalMs]);
}
