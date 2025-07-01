import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { Source, Layer, Marker, Map } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { useWebSocket } from "../hooks/useWebsocket";
import { getCircleColor, getCircleRadius, getLevelFromValue } from "../utils/mapStyles";
import { useDataStats } from "../hooks/useDatastats";
import { useMapClick, usePointerCursor } from "../utils/mapInteractions";
import InfoPopup from "./InfoPopup";


export default function MapView({ userLocation, setUserLocation, threshold, playbackSpeed, filterLevel, setAlertMessages, setConnectionStatus, setDataStats }) {

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
  
  // state to track clicked point on the map
  const [selectedPoint, setSelectedPoint] = useState(null);

  // color mapping for radiation levels
  const circleColor = useMemo(() => getCircleColor(), []);
  const circleRadius = useMemo(() => getCircleRadius(), []);

  // mouse interaction hooks for the map
  const onMouseMove = usePointerCursor(mapRef);
  const onMapClick  = useMapClick(setSelectedPoint);

  // hook to set up data statistics
  useDataStats(geojson, setDataStats);

  
  // Filter geojson based on filterLevel
  const filteredGeojson = useMemo(() => {
    if (filterLevel === "all") return geojson;
    
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
        initialViewState={viewState}
        onMoveEnd={(evt) => setViewState(evt.viewState)}
        onMouseMove={onMouseMove}
        onClick={onMapClick}
        style={{ width: "100vw", height: "100vh" }}
        mapStyle="mapbox://styles/mapbox/light-v10"
        mapboxAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
        interactiveLayerIds={["radiation-points"]}
      >
        <Source id="radiation" type="geojson" data={filteredGeojson}>
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

        {selectedPoint && (
          <InfoPopup
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
