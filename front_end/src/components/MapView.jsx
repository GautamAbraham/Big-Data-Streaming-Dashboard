import React, { useEffect, useRef, useState, useCallback } from "react";
import ReactMapGL, { Source, Layer, Marker, Map } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";

export default function MapView({
  userLocation,
  setUserLocation,
  threshold,
  playbackSpeed,
  setAlertMessages,
}) {
  const mapRef = useRef();
  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [dataStats, setDataStats] = useState({ total: 0, lastUpdate: null });
  const [geojson, setGeojson] = useState({
    type: "FeatureCollection",
    features: [],
  });

  const thresholdRef = useRef(threshold);
  useEffect(() => {
    thresholdRef.current = threshold;
  }, [threshold]);

  // Enhanced color mapping for radiation levels
  const circleColor = [
    "case",
    [">=", ["get", "value"], 100], "#dc2626", // Very high (red)
    [">=", ["get", "value"], 50], "#ea580c",  // High (orange-red)
    [">=", ["get", "value"], 20], "#eab308",  // Moderate (yellow)
    [">=", ["get", "value"], 10], "#22c55e",  // Low (green)
    "#6b7280" // Very low/unknown (gray)
  ];

  const circleRadius = [
    "case",
    [">=", ["get", "value"], 100], 8,
    [">=", ["get", "value"], 50], 6,
    [">=", ["get", "value"], 20], 5,
    4
  ];

  const connectWebSocket = useCallback(() => {
    const wsUrl = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws";
    console.log("WebSocket connecting to:", wsUrl);
    
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return; // Already connected
    }

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;
    let buffer = [];
    let bufferTimer;

    ws.onopen = () => {
      console.log("WebSocket connected");
      setConnectionStatus('connected');
      
      // Start buffering timer with playback speed
      const updateInterval = Math.max(50, 1000 / Math.max(playbackSpeed, 0.1));
      bufferTimer = setInterval(() => {
        if (buffer.length > 0) {
          const newFeatures = buffer.map(data => ({
            type: "Feature",
            geometry: {
              type: "Point",
              coordinates: [Number(data.lon), Number(data.lat)],
            },
            properties: {
              ...data,
              level: getLevelFromValue(data.value),
              timestamp: new Date().toISOString()
            },
          }));

          setGeojson(prev => ({
            ...prev,
            features: [...prev.features, ...newFeatures].slice(-2000), // Keep last 2000 points
          }));

          setDataStats(prev => ({
            total: prev.total + buffer.length,
            lastUpdate: new Date().toISOString()
          }));

          console.log(`Added ${buffer.length} new points. Total features: ${newFeatures.length}`);
          buffer = [];
        }
      }, updateInterval);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        // Handle heartbeat messages
        if (data.type === 'heartbeat') {
          console.log("Received heartbeat");
          return;
        }

        // Validate required fields
        const lat = Number(data.lat);
        const lon = Number(data.lon);
        const value = Number(data.value);

        if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && 
            lon >= -180 && lon <= 180 && !isNaN(value)) {
              if (value >= thresholdRef.current) {
                setAlertMessages((prev) => [
                  ...prev,
                  `High radiation detected: ${value} at (${lat}, ${lon})`
                ]);
              }
          buffer.push({ ...data, lat, lon, value });
        } else {
          console.warn("Invalid data point:", data);
        }
      } catch (e) {
        console.warn("Failed to parse WebSocket message:", e);
      }
    };

    ws.onclose = () => {
      console.log("WebSocket disconnected");
      setConnectionStatus('disconnected');
      clearInterval(bufferTimer);
      
      // Auto-reconnect after 3 seconds
      reconnectTimeoutRef.current = setTimeout(() => {
        connectWebSocket();
      }, 3000);
    };

    ws.onerror = (error) => {
      console.error("WebSocket error:", error);
      setConnectionStatus('error');
    };

    return () => {
      clearInterval(bufferTimer);
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [playbackSpeed, setAlertMessages]);

  // Helper function to determine radiation level
  const getLevelFromValue = (value) => {
    if (value >= 100) return "very-high";
    if (value >= 50) return "high";
    if (value >= 20) return "moderate";
    if (value >= 10) return "low";
    return "very-low";
  };

  useEffect(() => {
    connectWebSocket();
    
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [connectWebSocket]);

  // Getting the user's current location
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

  // Debug logging (can be removed in production)
  console.log(`Features count: ${geojson.features.length}, Status: ${connectionStatus}`);

  return (
    <div style={{ position: 'relative', width: '100vw', height: '100vh' }}>
      {/* Connection Status Indicator */}
      <div style={{
        position: 'absolute',
        top: 10,
        right: 10,
        zIndex: 1000,
        background: connectionStatus === 'connected' ? '#22c55e' : '#ef4444',
        color: 'white',
        padding: '8px 12px',
        borderRadius: '4px',
        fontSize: '12px'
      }}>
        {connectionStatus === 'connected' ? '● Connected' : '● Disconnected'}
        <div style={{ fontSize: '10px', marginTop: '2px' }}>
          Points: {geojson.features.length} | Total: {dataStats.total}
        </div>
      </div>

      <Map
        ref={mapRef}
        initialViewState={{ latitude: 35.6762, longitude: 139.6503, zoom: 6 }} // Tokyo area for better default view
        style={{ width: "100vw", height: "100vh" }}
        mapStyle="mapbox://styles/mapbox/light-v10"
        mapboxAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
        interactiveLayerIds={["radiation-points"]}
      >
        <Source id="radiation" type="geojson" data={geojson}>
          <Layer
            id="radiation-points"
            type="circle"
            paint={{
              "circle-radius": circleRadius,
              "circle-color": circleColor,
              "circle-opacity": 0.8,
              "circle-stroke-width": 1,
              "circle-stroke-color": "#ffffff"
            }}
          />
        </Source>

        {userLocation && (
          <Marker
            latitude={userLocation.latitude}
            longitude={userLocation.longitude}
            color="blue"
          />
        )}
      </Map>
    </div>
  );
}
