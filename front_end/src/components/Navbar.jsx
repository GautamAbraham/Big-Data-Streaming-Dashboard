import { MenuIcon } from "../assets/icons";

function Navbar({ toggleSidebar, connectionStatus, dataStats }) {
  function getConnectionStatus() {
    return (
      <div className="relative group">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-50 hover:bg-gray-100 transition-colors duration-200">
          <span
            className={`inline-block w-2.5 h-2.5 rounded-full ${
              connectionStatus === "connected"
                ? "bg-green-500 animate-pulse"
                : "bg-red-500"
            }`}
            aria-label={
              connectionStatus === "connected" ? "Connected" : "Disconnected"
            }
          />
          <span className="text-xs font-medium text-gray-700">
            {connectionStatus === "connected" ? "Live" : "Offline"}
          </span>
        </div>
        <div className="absolute right-0 mt-2 px-3 py-2 rounded-lg bg-gray-800 text-white text-xs opacity-0 group-hover:opacity-100 pointer-events-none whitespace-nowrap z-10 transition-opacity duration-200 shadow-lg">
          {connectionStatus === "connected"
            ? "Real-time data connection active"
            : "Connection lost - attempting to reconnect"}
        </div>
      </div>
    );
  }

  function getDataStats() {
    const high = dataStats?.high ?? 0;
    const medium = dataStats?.medium ?? 0;
    const low = dataStats?.low ?? 0;

    return (
      <div className="hidden sm:flex items-center gap-4 px-4 py-1.5 rounded-lg bg-gray-50">
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-red-500"></div>
          <span className="text-xs font-semibold text-gray-700">High: {high}</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-yellow-500"></div>
          <span className="text-xs font-semibold text-gray-700">Med: {medium}</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-green-500"></div>
          <span className="text-xs font-semibold text-gray-700">Low: {low}</span>
        </div>
      </div>
    );
  }

  // Format the date in a human-friendly way
  function getFormattedTime() {
    if (!dataStats?.lastUpdate) return "--";
    const date = new Date(dataStats.lastUpdate);
    return date.toLocaleString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour12: false,
    });
  }

  return (
    <nav className="bg-white shadow-sm border-b border-gray-200 flex items-center h-16 px-4">
      <div className="flex items-center">
        <button
          onClick={toggleSidebar}
          className="p-2 rounded-lg hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors duration-200"
          aria-label="Toggle sidebar"
        >
          <MenuIcon />
        </button>
      </div>

      {/* Center - Data stats (hidden on mobile) */}
      <div className="flex-1 flex justify-center">
        {getDataStats()}
      </div>

      {/* Right side - Connection status and last update */}
      <div className="flex items-center gap-4">
        {getConnectionStatus()}
        <div className="hidden md:block text-xs text-gray-600">
          <span className="font-medium">Last Update:</span>
          <span className="ml-1 font-mono">{getFormattedTime()}</span>
        </div>
      </div>
    </nav>
  );
}

export default Navbar;
