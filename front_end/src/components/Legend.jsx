const Legend = ({ userLocation }) => {
  return (
    <div className="absolute bottom-6 right-6 bg-white/95 p-5 rounded-2xl shadow-2xl border border-gray-200 min-w-[220px] backdrop-blur-md">

      <h3 className="text-lg font-bold text-gray-700 mb-4 flex items-center gap-2">
        {"Legend"}
      </h3>

      <div className="space-y-2">
        {userLocation && (
          <div className="flex items-center gap-2 text-blue-700 font-medium">
            <span className="inline-block w-4 h-4 bg-blue-500 rounded-full border-2 border-blue-300 shadow" />
            {"Current Location"}
          </div>
        )}

        <div className="flex items-center gap-2 text-green-700 font-medium">
          <span className="inline-block w-4 h-4 bg-green-500 rounded-full border-2 border-green-300 shadow" />
          {"Low (< 20 CPM)"}
        </div>

        <div className="flex items-center gap-2 text-yellow-700 font-medium">
          <span className="inline-block w-4 h-4 bg-yellow-400 rounded-full border-2 border-yellow-300 shadow" />
          {"Medium (20 - 60 CPM)"}
        </div>

        <div className="flex items-center gap-2 text-red-700 font-medium">
          <span className="inline-block w-4 h-4 bg-red-500 rounded-full border-2 border-red-300 shadow" />
          {"High (> 60 CPM)"}
        </div>
        
      </div>
    </div>
  );
};

export default Legend;
