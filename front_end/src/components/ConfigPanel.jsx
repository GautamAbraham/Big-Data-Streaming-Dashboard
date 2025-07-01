function ConfigPanel({
  filterLevel,
  setFilterLevel,
  threshold,
  setThreshold,
  playbackSpeed,
  setPlaybackSpeed,
}) {
  return (
    <aside className="w-72 bg-gradient-to-b from-blue-50 via-white to-white px-9 py-6 overflow-auto">

      {/* CPM level filter */}
      <h2 className="font-extrabold mb-3 text-blue-700 tracking-wide flex items-center gap-2 uppercase">
        {"Filter by Level"}
      </h2>
      <div className="mb-6">
        <select
          value={filterLevel}
          onChange={(e) => setFilterLevel(e.target.value)}
          className="w-full border-2 border-blue-200 rounded-lg px-3 py-2 bg-blue-50 text-gray-700 font-medium shadow-sm outline-none"
        >
          <option value="all">All</option>
          <option value="low">Low</option>
          <option value="medium">Medium</option>
          <option value="high">High</option>
        </select>
      </div>
      <hr className="my-6 border-blue-100" />

      {/* configuration settings */}
      <h2 className="font-extrabold mb-4 text-green-700 tracking-wide flex items-center gap-2 uppercase">
        {"Configuration"}
      </h2>
      <div className="mb-6">
        <label className="block mb-2 font-semibold text-gray-700">
          {"Critical CPM Threshold"}
        </label>
        <input
          type="number"
          value={threshold}
          onChange={(e) => setThreshold(Number(e.target.value))}
          className="w-full border-2 border-green-200 focus:border-green-500 transition rounded-lg px-3 py-2 bg-green-50 text-gray-700 font-medium shadow-sm outline-none"
        />
      </div>

      <div className="mb-2">
        <label className="mb-2 font-semibold text-gray-700 flex items-center justify-between">
          <span>{"Playback Speed"}</span>
          <span className="text-blue-600 font-bold">{playbackSpeed}{"x"}</span>
        </label>
        <input
          type="range"
          min="0.1"
          max="10"
          step="0.1"
          value={playbackSpeed}
          onChange={(e) => setPlaybackSpeed(Number(e.target.value))}
          className="w-full accent-blue-500 h-2 rounded-lg appearance-none cursor-pointer bg-blue-100"
        />
      </div>
    </aside>
  );
}

export default ConfigPanel;
