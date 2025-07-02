import { useState, useEffect } from "react";
import MapView from "./components/MapView";
import Navbar from "./components/Navbar";
import ConfigPanel from "./components/ConfigPanel";
import Legend from "./components/Legend";
import Alerts from "./components/Alerts";
import { useAlertManager } from "./hooks/useAlertManager";

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [userLocation, setUserLocation] = useState(null);

  // configuration states
  const [threshold, setThreshold] = useState(500);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [filterLevel, setFilterLevel] = useState("all");

  // Use the new alert manager
  const { alerts, addAlert, dismissAlert, clearAllAlerts } = useAlertManager();

  // connection status and data stats
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [dataStats, setDataStats] = useState({
    lastUpdate: null,
    high: 0,
    medium: 0,
    low: 0
  });

  // Clear alerts when threshold changes
  useEffect(() => {
    clearAllAlerts();
  }, [threshold, clearAllAlerts]);


  return (
    <div className="flex flex-col h-screen">
      <Navbar 
        toggleSidebar={() => setSidebarOpen((prev) => !prev)}
        connectionStatus={connectionStatus}
        dataStats={dataStats}
      />

      {/* sidebar section */}
      <div className="flex flex-1 overflow-hidden">
        <div
          className={`transform transition-all duration-300 ease-in-out ${
            sidebarOpen ? "w-72 opacity-100" : "w-0 opacity-0"
          }`}
        >
          <ConfigPanel
            filterLevel={filterLevel}
            setFilterLevel={setFilterLevel}
            threshold={threshold}
            setThreshold={setThreshold}
            playbackSpeed={playbackSpeed}
            setPlaybackSpeed={setPlaybackSpeed}
          />
        </div>
        
        {/* map section */}
        <div className="flex-1">
          <MapView
            userLocation={userLocation}
            setUserLocation={setUserLocation}
            threshold={threshold}
            playbackSpeed={playbackSpeed}
            filterLevel={filterLevel}
            onAlert={addAlert}
            setConnectionStatus={setConnectionStatus}
            setDataStats={setDataStats}
          />
          <Legend userLocation={userLocation} />
          <Alerts alerts={alerts} onDismiss={dismissAlert} />
        </div>
      </div>
    </div>
  );
}

export default App;
