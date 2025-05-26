import { useRef, useEffect, useState } from "react";
import ReactMapGL, { Source, Layer } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";

export default function MapView() {
  const mapRef = useRef();
  const [geojson, setGeojson] = useState({
    type: "FeatureCollection",
    features: [],
  });

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

  return (
    <ReactMapGL
      ref={mapRef}
      initialViewState={{ latitude: 20, longitude: 0, zoom: 1.5 }}
      style={{ width: "100vw", height: "100vh" }}
      mapStyle="mapbox://styles/mapbox/light-v10"
      mapboxAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
    >
      <Source id="radiation" type="geojson" data={geojson}>
        <Layer
          id="radiation-points"
          type="circle"
          paint={{
            "circle-radius": 4,
            "circle-color": "green",
            "circle-opacity": 0.6,
          }}
        />
      </Source>
    </ReactMapGL>
  );
}
