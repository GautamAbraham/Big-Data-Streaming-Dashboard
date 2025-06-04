function ConfigPanel({
  filterLevel,
  setFilterLevel,
  threshold,
  setThreshold,
  startTime,
  setStartTime,
  endTime,
  setEndTime,
  playbackSpeed,
  setPlaybackSpeed,
}) {
  return (
    <aside className="w-64 bg-gray-100 p-4 overflow-auto">

      {/* CPM level filter */}
      <h2 className="text-lg font-semibold mb-4">{"Filter by Level"}</h2>
      <div className="mb-6">
        <select
          value={filterLevel}
          onChange={(e) => setFilterLevel(e.target.value)}
          className="w-full border rounded px-2 py-1"
        >
          <option value="all">{"All"}</option>
          <option value="low">{"Low"}</option>
          <option value="medium">{"Medium"}</option>
          <option value="high">{"High"}</option>
        </select>
      </div>

      {/* configuration settings */}
      <h2 className="text-lg font-semibold mb-4">{"Configuration"}</h2>
      <div className="mb-6">
        <label className="block mb-1 font-medium">
          {"Critical CPM Threshold:"}
        </label>
        <input
          type="number"
          value={threshold}
          onChange={(e) => setThreshold(Number(e.target.value))}
          className="w-full border rounded px-2 py-1"
        />
      </div>

      <div className="mb-6">
        <label className="block mb-1 font-medium">{"Time Window:"}</label>
        <input
          type="datetime-local"
          value={startTime}
          onChange={(e) => setStartTime(e.target.value)}
          className="w-full border rounded px-2 py-1 mb-2"
        />
        <input
          type="datetime-local"
          value={endTime}
          onChange={(e) => setEndTime(e.target.value)}
          className="w-full border rounded px-2 py-1"
        />
      </div>

      <div className="mb-6">
        <label className="block mb-1 font-medium">
          {"Playback Speed (x): "}
          {playbackSpeed}
        </label>
        <input
          type="range"
          min="0.1"
          max="10"
          step="0.1"
          value={playbackSpeed}
          onChange={(e) => setPlaybackSpeed(Number(e.target.value))}
          className="w-full"
        />
      </div>
    </aside>
  );
}

export default ConfigPanel;
