import { useEffect, useRef, useState } from "react";
import ReactMapGL, { Source, Layer, Marker } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";

export default function MapView() {
  const mapRef = useRef();
  const [userLocation, setUserLocation] = useState(null);

  const initialGeoJSON = {
    type: "FeatureCollection",
    features: [],
  };

  const circleColor = [
    "match",
    ["get", "level"],
    "low", "#22c55e",
    "medium", "#eab308",
    "high", "#ef4444",
    "#6b7280"
  ];


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
    <ReactMapGL
      ref={mapRef}
      initialViewState={{ latitude: 20, longitude: 0, zoom: 1.5 }}
      style={{ width: "100vw", height: "100vh" }}
      mapStyle="mapbox://styles/mapbox/light-v10"
      mapboxAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
    >
      <Source id="radiation" type="geojson" data={initialGeoJSON}>
        <Layer
          id="radiation-points"
          type="circle"
          paint={{
            "circle-radius": 4,
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
          <div className="h-6 w-6 bg-blue-500 rounded-full border-2 border-white shadow-lg" />
        </Marker>
      )}
      
    </ReactMapGL>
  );
}
