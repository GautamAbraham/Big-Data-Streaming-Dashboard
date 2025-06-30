import { useEffect, useRef, useState, useCallback } from "react";
import { Source, Layer, Marker, Map } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { useWebSocket } from "../hooks/useWebsocket";
import { getCircleColor, getCircleRadius } from "../utils/mapStyles";
import { useDataStats } from "../hooks/useDatastats";


export default function MapView({userLocation, setUserLocation, threshold, playbackSpeed, setAlertMessages, setConnectionStatus, setDataStats}) {

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

  // color mapping for radiation levels
  const circleColor = getCircleColor();
  const circleRadius = getCircleRadius();


  // hook to set up data statistics
  useDataStats(geojson, setDataStats);

  
  // Helper to categorize radiation levels
  const getLevelFromValue = useCallback((value) => {
    if (value >= 100) return "very-high";
    if (value >= 50) return "high";
    if (value >= 20) return "moderate";
    if (value >= 10) return "low";
    return "very-low";
  }, []);


  // Buffer and add incoming data points
  const handleDataPoints = useCallback((points) => {
      const newFeatures = points.map((d) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [d.lon, d.lat] },
        properties: {
          ...d,
          level: getLevelFromValue(d.value),
          timestamp: new Date().toISOString(),
        },
      }));
      setGeojson((gj) => ({
        ...gj,
        features: [...gj.features, ...newFeatures].slice(-2000),
      }));
    }, [getLevelFromValue]
  );


  const handleConnectionStatus = useCallback((status) => {
      setConnectionStatus(status);
    }, [setConnectionStatus]
  );


  const handleAlert = useCallback((msg) => {
      setAlertMessages((list) => [...list, msg]);
    }, [setAlertMessages]
  );


  // Use custom WebSocket hook
  useWebSocket({
    wsUrl: import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws",
    playbackSpeed,
    threshold,
    onDataPoints: handleDataPoints,
    onConnectionStatus: handleConnectionStatus,
    onAlert: handleAlert,
  });


  // Obtain user's geolocation once
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
  

  // Pan map to user location when provided
  useEffect(() => {
    if (userLocation) {
      setViewState({
        latitude: userLocation.latitude,
        longitude: userLocation.longitude,
        zoom: 8,
      });
    }
  }, [userLocation]);


  return (
    <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
      <Map
        ref={mapRef}
        {...viewState}
        onMove={(evt) => setViewState(evt.viewState)}
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
              "circle-stroke-color": "#ffffff",
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
