import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { Source, Layer, Marker, Map } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { useWebSocket } from "../hooks/useWebsocket";
import { getCircleColor, getCircleRadius, getLevelFromValue } from "../utils/mapStyles";
import { useDataStats } from "../hooks/useDatastats";
import { useMapClick, usePointerCursor } from "../utils/mapInteractions";
import InfoPopup from "./InfoPopup";
import axios from "axios";

export default function MapView({
  userLocation,
  setUserLocation,
  threshold,
  playbackSpeed,
  setAlertMessages,
  levelFilter,
  filterLevel,
  onAlert,
  setConnectionStatus,
  setDataStats
}) {
  const mapRef = useRef();

  const [geojson, setGeojson] = useState({
    type: "FeatureCollection",
    features: [],
  });

  const [viewState, setViewState] = useState({
    latitude: 0,
    longitude: 0,
    zoom: 2,
  });

  const thresholdRef = useRef(threshold);
  useEffect(() => {
    thresholdRef.current = threshold;
  }, [threshold]);

  // Only use the utility once (no duplicate color code)
  const circleColor = useMemo(() => getCircleColor(), []);
  const circleRadius = useMemo(() => getCircleRadius(), []);
  const [selectedPoint, setSelectedPoint] = useState(null);

  const onMouseMove = usePointerCursor(mapRef);
  const onMapClick  = useMapClick(setSelectedPoint);
  useDataStats(geojson, setDataStats);

  // --- CLUSTERING + FILTER BY LEVEL ---
  const filteredGeojson = useMemo(() => {
    if (filterLevel === "all" || !filterLevel) return geojson;
    return {
      ...geojson,
      features: geojson.features.filter(feature => {
        const level = feature.properties.level;
        switch (filterLevel) {
          case "high":
            return level === "very-high" || level === "high";
          case "medium":
            return level === "moderate";
          case "low":
            return level === "low" || level === "very-low";
          default: return true;
        }
      })
    };
  }, [geojson, filterLevel]);

  // Buffer and add incoming data points - SHOW ALL POINTS ON MAP
  const handleDataPoints = useCallback((points) => {
    // Create features for ALL points (don't filter by threshold for map display)
    const newFeatures = points.map((d) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [d.lon, d.lat] },
      properties: {
        ...d,
        level: getLevelFromValue(d.value),
        timestamp: new Date().toISOString(),
      },
    }));

    setGeojson((gj) => {
      const updated = {
        ...gj,
        features: [...gj.features, ...newFeatures].slice(-2000),
      };
      return updated;
    });

    // Alert ONLY for above-threshold points (separate from map display)
    const alertPoints = points.filter(d => d.value >= thresholdRef.current);
    alertPoints.forEach(({ value, lat, lon }) => {
      const severity = value >= thresholdRef.current * 2 ? 'critical' : 'warning';
      const latStr = typeof lat === 'number' ? lat.toFixed(2) : 'unknown';
      const lonStr = typeof lon === 'number' ? lon.toFixed(2) : 'unknown';
      onAlert?.(`CPM ${value} at [${latStr}, ${lonStr}]`, severity, { lat, lon });
    });
  }, [getLevelFromValue, onAlert]);
  // -----------------------------------------------------------

  const handleConnectionStatus = useCallback((status) => {
    setConnectionStatus(status);
  }, [setConnectionStatus]);

  // --- WebSocket integration (no change) ---
  useWebSocket({
    wsUrl: import.meta.env.VITE_WS_URL,
    playbackSpeed,
    threshold,
    onDataPoints: handleDataPoints,
    onConnectionStatus: handleConnectionStatus,
    onAlert,
  });

  // --- Playback speed sent to backend ---
  useEffect(() => {
    async function updateBackendSpeed() {
      try {
        await axios.post("http://localhost:8000/api/playback_speed", {
          playback_speed: playbackSpeed
        });
      } catch (err) {
        console.warn("Failed to update playback speed:", err);
      }
    }
    updateBackendSpeed();
  }, [playbackSpeed]);
  // ------------------------------------------------------------

  // --- User location logic (no change) ---
  useEffect(() => {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          setUserLocation({
            latitude: position.coords.latitude,
            longitude: position.coords.longitude,
          });
        },
        (error) => {
          console.warn("Geolocation error:", error.message);
        },
        { enableHighAccuracy: true }
      );
    }
  }, [setUserLocation]);

  useEffect(() => {
    if (userLocation) {
      setViewState({
        latitude: userLocation.latitude,
        longitude: userLocation.longitude,
        zoom: 8,
      });
    }
  }, [userLocation]);

  // ---------------------- RETURN --------------------------
  return (
    <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
      <Map
        ref={mapRef}
        initialViewState={viewState}
        onMoveEnd={(evt) => setViewState(evt.viewState)}
        onMouseMove={onMouseMove}
        onClick={onMapClick}
        style={{ width: "100vw", height: "100vh" }}
        mapStyle="mapbox://styles/mapbox/light-v10"
        mapboxAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
        interactiveLayerIds={["clusters", "radiation-points"]}
      >
       <Source
          id="radiation"
          type="geojson"
          data={filteredGeojson}
          cluster={true}
          clusterMaxZoom={7}
          clusterRadius={50}
          clusterProperties={{
            // max_cpm will be the highest CPM value among points in the cluster
            max_cpm: ["max", ["get", "value"]]
          }}
        >
          {/* --- Clustered circles --- */}
          <Layer
            id="clusters"
            type="circle"
            filter={["has", "point_count"]}
            paint={{
              // Color clusters by max CPM (severity)
              "circle-color": [
                "step",
                ["get", "max_cpm"],
                "#22c55e",   // < 20 CPM (green)
                50, "#ea580c", // 50-100 CPM (orange)
                200, "#dc2626", // >100 CPM (red)
                1000, "#7f1d1d" // >1000 CPM (dark red, critical)
              ],
              "circle-radius": [
                "step",
                ["get", "point_count"],
                20, 100, 30, 750, 40
              ],
              "circle-stroke-width": 1,
              "circle-stroke-color": "#fff",
              "circle-opacity": 0.7
            }}
          />
          {/* --- Cluster counts --- */}
          <Layer
            id="cluster-count"
            type="symbol"
            filter={["has", "point_count"]}
            layout={{
              "text-field": "{point_count_abbreviated}",
              "text-font": ["DIN Offc Pro Medium", "Arial Unicode MS Bold"],
              "text-size": 14,
            }}
          />
          {/* --- Unclustered points --- */}
          <Layer
            id="radiation-points"
            type="circle"
            filter={["!", ["has", "point_count"]]}
            paint={{
              "circle-radius": [
                "match",
                ["get", "level"],
                "low", 7,
                "moderate", 10,
                "high", 16,
                6
              ],
              "circle-color": circleColor,
              "circle-opacity": 0.8,
              "circle-stroke-width": [
                "case",
                ["==", ["get", "level"], "high"], 3,
                1
              ],
              "circle-stroke-color": [
                "case",
                ["==", ["get", "level"], "high"], "#fff",
                "#222"
              ],
              "circle-blur": [
                "case",
                ["==", ["get", "level"], "high"], 0.5,
                0
              ]
            }}
          />
        </Source>

        {/* User location marker */}
        {userLocation && (
          <Marker
            latitude={userLocation.latitude}
            longitude={userLocation.longitude}
            color="blue"
          />
        )}

        {/* Popup on click */}
        {selectedPoint && (
          <InfoPopup
            timestamp={selectedPoint.properties.timestamp}
            latitude={selectedPoint.geometry.coordinates[1]}
            longitude={selectedPoint.geometry.coordinates[0]}
            cpm={selectedPoint.properties.value}
            setSelectedPoint={setSelectedPoint}
          />
        )}
      </Map>
    </div>
  );
}
