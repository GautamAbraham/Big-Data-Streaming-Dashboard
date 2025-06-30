const Legend = ({ userLocation }) => {
  return (
    <div className="absolute bottom-4 right-4 bg-white/95 p-4 rounded-xl shadow-lg border border-gray-200 min-w-[180px] backdrop-blur-md">
      <h3 className="text-sm font-bold text-gray-700 mb-2 flex items-center gap-2">
        {"Legend"}
      </h3>

      <div className="space-y-1.5">
        {userLocation && (
          <div className="flex items-center gap-1.5 text-blue-700 text-sm font-medium">
            <span className="inline-block w-3 h-3 bg-blue-500 rounded-full border border-blue-300 shadow-sm" />
            {"Current Location"}
          </div>
        )}

        <div className="flex items-center gap-1.5 text-green-700 text-sm font-medium">
          <span className="inline-block w-3 h-3 bg-green-500 rounded-full border border-green-300 shadow-sm" />
          {"Low (< 20 CPM)"}
        </div>

        <div className="flex items-center gap-1.5 text-yellow-700 text-sm font-medium">
          <span className="inline-block w-3 h-3 bg-yellow-400 rounded-full border border-yellow-300 shadow-sm" />
          {"Medium (20 - 60 CPM)"}
        </div>

        <div className="flex items-center gap-1.5 text-red-700 text-sm font-medium">
          <span className="inline-block w-3 h-3 bg-red-500 rounded-full border border-red-300 shadow-sm" />
          {"High (> 60 CPM)"}
        </div>
      </div>
    </div>
  );
};

export default Legend;
