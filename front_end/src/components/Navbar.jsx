import { MenuIcon } from "../assets/icons";

function Navbar({ toggleSidebar, connectionStatus, dataStats }) {
  
  function getConnectionStatus() {
    return (
      <div className="flex-grow text-center">
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
          <span className="text-base font-semibold text-gray-800 bg-gray-100 px-2 py-0.5">
            {dataStats?.total ?? 0}{" "}
            <span className="font-normal text-gray-500">{"points"}</span>
          </span>
        </div>
      </div>
    );
  }


  return (
    <nav className="bg-white shadow flex items-center h-14">
      <button
        onClick={toggleSidebar}
        className="p-2 ml-4 rounded hover:bg-gray-200 focus:outline-none"
        aria-label="Toggle sidebar"
      >
        <MenuIcon />
      </button>
      {getConnectionStatus()}
    </nav>
  );
}

export default Navbar;
