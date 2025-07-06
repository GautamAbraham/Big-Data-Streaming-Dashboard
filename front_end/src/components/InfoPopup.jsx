import { Popup } from "react-map-gl";
import { memo } from "react";

function InfoPopup({ timestamp,latitude, longitude, cpm, setSelectedPoint }) {
  let lat, lon;

  if (Array.isArray(latitude)) {
    lon = latitude[0];
    lat = latitude[1];
  } 
  else if (Array.isArray(longitude)) {
    lon = longitude[0];
    lat = longitude[1];
  } 
  else {
    lat = latitude;
    lon = longitude;
  }

  // Format timestamp to a user-friendly format
  const formattedTimestamp = timestamp
    ? new Date(timestamp).toLocaleString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : "--";

  return (
    <Popup
      latitude={lat}
      longitude={lon}
      onClose={() => setSelectedPoint(null)}
      closeOnClick={false}
      closeButton={false}
    >
      <div className="bg-white rounded p-1 text-center min-w-[150px] text-sm font-sans">
        <div className="mb-2 text-gray-700 font-semibold">
          {"CPM: "}
          <span className="font-bold">{cpm}</span>
        </div>
        <div className="text-gray-500">
          {"Lat: "}
          <span className="font-mono text-gray-700">
            {typeof lat === "number" ? lat.toFixed(5) : "--"}
          </span>
        </div>
        <div className="text-gray-500">
          {"Lon: "}
          <span className="font-mono text-gray-700">
            {typeof lon === "number" ? lon.toFixed(5) : "--"}
          </span>
        </div>
         <div className="text-gray-500">
          {"Timestamp: "}
          <span className="font-mono text-gray-700">
            {formattedTimestamp}
          </span>
        </div>
      </div>
    </Popup>
  );
}

export default memo(InfoPopup);
