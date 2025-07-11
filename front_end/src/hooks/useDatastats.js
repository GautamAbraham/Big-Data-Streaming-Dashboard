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

            const { high, medium, low, totalRadiation, count } = feats.reduce(
                (acc, { properties }) => {
                    const { level, value } = properties;
                    switch (level) {
                        case "very-high":
                        case "high":
                            acc.high++;
                            break;
                        case "moderate":
                            acc.medium++;
                            break;
                        default:
                            acc.low++;
                    }
                    // Add radiation value for average calculation
                    if (typeof value === "number" && !isNaN(value)) {
                        acc.totalRadiation += value;
                        acc.count++;
                    }
                    return acc;
                },
                { high: 0, medium: 0, low: 0, totalRadiation: 0, count: 0 }
            );

            const averageRadiation = count > 0 ? totalRadiation / count : 0;
            const timestamp =
                feats[feats.length - 1].properties?.timestamp || null;

            setDataStats((prev) => ({
                ...prev,
                high,
                medium,
                low,
                averageRadiation,
                totalPoints: feats.length,
                dangerousPoints: high,
                lastUpdate: timestamp,
            }));
        }, intervalMs);

        return () => clearInterval(id);
    }, [setDataStats, intervalMs]);
}
