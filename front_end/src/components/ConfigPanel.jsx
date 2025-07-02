function ConfigPanel({
  filterLevel,
  setFilterLevel,
  threshold,
  setThreshold,
  playbackSpeed,
  setPlaybackSpeed,
}) {
  return (
    <aside className="w-72 bg-white border-r border-gray-100 h-full flex flex-col">
      <div className="flex-1 p-6 space-y-8 overflow-y-auto">
        {/* Filter Section */}
        <div>
          <h2 className="text-sm font-medium text-gray-900 mb-3">
            {"Radiation Level Filter"}
          </h2>
          <select
            value={filterLevel}
            onChange={(e) => setFilterLevel(e.target.value)}
            className="w-full px-3 py-2 border border-gray-200 rounded-lg bg-white text-gray-900 text-sm font-medium focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
          >
            <option value="all">Show All Levels</option>
            <option value="low">Low Radiation</option>
            <option value="medium">Medium Radiation</option>
            <option value="high">High Radiation</option>
          </select>
        </div>

        {/* Alert Threshold Section */}
        <div>
          <h2 className="text-sm font-medium text-gray-900 mb-3">
            {"Alert Settings"}
          </h2>
          <div>
            <label className="block text-sm text-gray-600 mb-2">
              {"Critical Threshold (CPM)"}
            </label>
            <input
              type="number"
              value={threshold}
              onChange={(e) => setThreshold(Number(e.target.value))}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg bg-white text-gray-900 text-sm font-medium focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
              placeholder="Enter threshold value"
            />
            <p className="text-xs text-gray-500 mt-1">
              {"Alerts trigger when radiation exceeds this value"}
            </p>
          </div>
        </div>

        {/* Playback Speed Section */}
        <div>
          <h2 className="text-sm font-medium text-gray-900 mb-3">
            {"Data Playback"}
          </h2>
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm text-gray-600">
                {"Speed Multiplier"}
              </label>
              <span className="text-sm font-semibold text-blue-600 bg-blue-50 px-2 py-1 rounded">
                {playbackSpeed}{"x"}
              </span>
            </div>
            <input
              type="range"
              min="0.1"
              max="10"
              step="0.1"
              value={playbackSpeed}
              onChange={(e) => setPlaybackSpeed(Number(e.target.value))}
              className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-20"
              style={{
                background: `linear-gradient(to right, #3b82f6 0%, #3b82f6 ${
                  ((playbackSpeed - 0.1) / 9.9) * 100
                }%, #e5e7eb ${
                  ((playbackSpeed - 0.1) / 9.9) * 100
                }%, #e5e7eb 100%)`,
              }}
            />
            <div className="flex justify-between text-xs text-gray-400 mt-1">
              <span>{"0.1x"}</span>
              <span>{"10x"}</span>
            </div>
          </div>
        </div>
      </div>
    </aside>
  );
}

export default ConfigPanel;
