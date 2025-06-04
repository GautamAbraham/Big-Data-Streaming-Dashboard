const Legend = ({ userLocation }) => {
  return (
    <div className="absolute bottom-4 right-4 bg-white bg-opacity-90 p-3 rounded shadow">
      
      {userLocation && (
        <div className="flex items-center mb-1">
          <span className="inline-block w-3 h-3 bg-blue-500 rounded-full mr-2" />{" "}
          {"Current Location"}
        </div>
      )}

      <div className="flex items-center mb-1">
        <span className="inline-block w-3 h-3 bg-green-500 rounded-full mr-2" />{" "}
        {"Low (< 20 CPM)"}
      </div>

      <div className="flex items-center mb-1">
        <span className="inline-block w-3 h-3 bg-yellow-500 rounded-full mr-2" />{" "}
        {"Medium (> 20 CPM & < 60 CPM)"}
      </div>

      <div className="flex items-center">
        <span className="inline-block w-3 h-3 bg-red-500 rounded-full mr-2" />{" "}
        {"High (> 60 CPM)"}
      </div>
    </div>
  );
};

export default Legend;
