import { useState, useEffect } from "react";
import MapView from "./components/MapView";
import Navbar from "./components/Navbar";
import ConfigPanel from "./components/ConfigPanel";
import Legend from "./components/Legend";
import Alerts from "./components/Alerts";

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [userLocation, setUserLocation] = useState(null);

  // configuration states
  const [threshold, setThreshold] = useState(500);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);

  // alert state
  const [alertMessages, setAlertMessages] = useState([]);

  // connection status and data stats
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [dataStats, setDataStats] = useState({
    lastUpdate: null,
    high: 0,
    medium: 0,
    low: 0
  });


  // --- AUTO DISMISS ALERT: PLACE THIS RIGHT AFTER THE alertMessages state ---
  useEffect(() => {
    if (alertMessages.length > 0) {
      const timer = setTimeout(() => {
        setAlertMessages((msgs) => msgs.slice(1));
      }, 3000);
      return () => clearTimeout(timer);
    }
  }, [alertMessages]);
  // --------------------------------------------------------------------------
  // --- CLEAR ALERTS when CPM threshold changes ---
  useEffect(() => {
    setAlertMessages([]);
  }, [threshold]);


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
            setAlertMessages={setAlertMessages}
            setConnectionStatus={setConnectionStatus}
            setDataStats={setDataStats}
          />
          <Legend userLocation={userLocation} />
          <Alerts messages={alertMessages} />
        </div>
      </div>
    </div>
  );
}

export default App;
