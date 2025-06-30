import { MenuIcon } from "../assets/icons";

function Navbar({ toggleSidebar, connectionStatus, dataStats }) {
  function getConnectionStatus() {
    const high = dataStats?.high ?? 0;
    const medium = dataStats?.medium ?? 0;
    const low = dataStats?.low ?? 0;

    return (
      <div className="inline-flex items-center justify-center gap-3">
        <div className="relative group">
          <span
            className={`inline-block w-3.5 h-3.5 rounded-full border-2 ${
              connectionStatus === "connected"
                ? "bg-green-500 border-green-400 shadow-sm"
                : "bg-red-500 border-red-400 shadow-sm"
            }`}
            aria-label={
              connectionStatus === "connected" ? "Connected" : "Disconnected"
            }
          />
          <div className="absolute left-1/2 -translate-x-1/2 mt-2 px-2 py-1 rounded bg-gray-800 text-white text-xs opacity-0 group-hover:opacity-100 pointer-events-none whitespace-nowrap z-10 transition-opacity duration-200">
            {connectionStatus === "connected"
              ? "Connected to server"
              : "Disconnected from server"}
          </div>
        </div>
        <span className="text-sm font-semibold text-gray-800 bg-gray-100 px-2 py-0.5 rounded">
          <span className="text-red-600 font-bold">High:</span> {high}
          <span className="mx-2 text-gray-400">|</span>
          <span className="text-yellow-600 font-bold">Medium:</span> {medium}
          <span className="mx-2 text-gray-400">|</span>
          <span className="text-green-600 font-bold">Low:</span> {low}
        </span>
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
    <nav className="bg-white shadow flex items-center h-14 px-4">

      <div className="flex-1 flex items-center">
        <button
          onClick={toggleSidebar}
          className="p-2 rounded hover:bg-gray-200 focus:outline-none"
          aria-label="Toggle sidebar"
        >
          <MenuIcon />
        </button>
      </div>

      <div className="flex-1 flex justify-center">
        {getConnectionStatus()}
      </div>

      <div className="flex-1 flex justify-end items-center">
        <span className="text-xs sm:text-sm px-3 py-1 text-gray-700">
          <span className="font-bold text-blue-700 mr-1">{"Last Update:"}</span>
          {getFormattedTime()}
        </span>
      </div>

    </nav>
  );
}

export default Navbar;
