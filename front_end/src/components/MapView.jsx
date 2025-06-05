import { useEffect, useRef, useState } from "react";
import ReactMapGL, { Source, Layer, Marker, Map } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";

export default function MapView({ userLocation, setUserLocation }) {
  const mapRef = useRef();

  const [geojson, setGeojson] = useState({
    type: "FeatureCollection",
    features: [],
  });

  const circleColor = [
    "match",
    ["get", "level"],
    "low", "#22c55e",
    "moderate", "#eab308",
    "high", "#ef4444",
    "#6b7280"
  ];


  useEffect(() => {
    const wsUrl = "ws://localhost:8000/ws"; // hardcoded for testing
    console.log("WebSocket connecting to:", wsUrl); // Debug log
    const ws = new WebSocket(wsUrl);
    let buffer = [];
    let intervalId;

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const lat = Number(data.lat);
        const lon = Number(data.lon);
        if (!isNaN(lat) && !isNaN(lon)) {
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
        // Ignore parse errors
      }
    };

    intervalId = setInterval(() => {
      if (buffer.length > 0) {
        setGeojson((prev) => ({
          ...prev,
          features: [...prev.features, ...buffer].slice(-1000),
        }));
        buffer = [];
      }
    }, 100);

    return () => {
      ws.close();
      clearInterval(intervalId);
    };
  }, []);


  // getting the user's current location
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
  }, []);


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

      {/* Render user location marker if available */}
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
