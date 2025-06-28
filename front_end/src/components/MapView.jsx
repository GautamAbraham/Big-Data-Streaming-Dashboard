import React, { useEffect, useRef, useState } from "react";
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
  const [geojson, setGeojson] = useState({
    type: "FeatureCollection",
    features: [],
  });

  const thresholdRef = useRef(threshold);
  useEffect(() => {
    thresholdRef.current = threshold;
  }, [threshold]);

  const circleColor = [
    "match",
    ["get", "level"],
    "low", "#22c55e",
    "moderate", "#eab308",
    "high", "#ef4444",
    "#6b7280"
  ];

  useEffect(() => {
    const wsUrl = "ws://localhost:8000/ws";
    let ws;
    let buffer = [];

    function connect() {
      ws = new window.WebSocket(wsUrl);

      ws.onopen = () => console.log("WebSocket connected");
      ws.onclose = () => {
        console.log("WebSocket closed. Reconnecting in 2s...");
        setTimeout(connect, 2000);
      };
      ws.onerror = (e) => {
        console.error("WebSocket error:", e);
        ws.close();
      };
      ws.onmessage = (event) => {
        //console.log("Current threshold:", threshold, "Current value:", value);
        try {
          const data = JSON.parse(event.data);
          const lat = Number(data.lat);
          const lon = Number(data.lon);
          const value = Number(data.value);
          
          if (!isNaN(lat) && !isNaN(lon) && value >= thresholdRef.current) {
           
              setAlertMessages((prev) => [
                ...prev,
                `CRITICAL: CPM reached ${value} at [${lat.toFixed(2)}, ${lon.toFixed(2)}]!`,
              ]);
           
            const feature = {
              type: "Feature",
              geometry: {
                type: "Point",
                coordinates: [lon, lat],
              },
              properties: data,
            };
            buffer.push(feature);
          }
        } catch (e) {
          console.warn("parse error", e);
        }
      };
    }

    connect();

    // Update geojson every interval, using playbackSpeed
    const intervalId = setInterval(() => {
      if (buffer.length > 0) {
        setGeojson((prev) => ({
          ...prev,
          features: [...prev.features, ...buffer].slice(-1000),
        }));
        buffer = [];
      }
    }, Math.max(100, 1000 / Math.max(playbackSpeed, 0.1))); 
    return () => {
      ws && ws.close();
      clearInterval(intervalId);
    };
  }, [threshold, playbackSpeed, setAlertMessages]);

  // Get user geolocation
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

  return (
    <Map
      ref={mapRef}
      initialViewState={{ latitude: 20, longitude: 0, zoom: 1.5 }}
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
            "circle-radius": 5,
            "circle-color": circleColor,
            "circle-opacity": 0.6,
          }}
        />
      </Source>
      {/* User location marker */}
      {userLocation && (
        <Marker
          latitude={userLocation.latitude}
          longitude={userLocation.longitude}
          offsetLeft={-12}
          offsetTop={-24}
        >
          <div className="h-5 w-5 bg-blue-500 rounded-full border-2 border-white shadow-lg" />
        </Marker>
      )}
    </Map>
  );
}
