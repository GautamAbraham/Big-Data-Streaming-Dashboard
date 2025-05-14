import { useRef } from "react";
import ReactMapGL, { Source, Layer } from "react-map-gl";
import "mapbox-gl/dist/mapbox-gl.css";

export default function MapView() {
  const mapRef = useRef();

  const initialGeoJSON = {
    type: "FeatureCollection",
    features: [],
  };

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
            "circle-color": "green",
            "circle-opacity": 0.6,
          }}
        />
      </Source>
    </ReactMapGL>
  );
}
