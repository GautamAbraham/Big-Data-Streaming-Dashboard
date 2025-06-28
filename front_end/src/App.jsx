import React, { useState, useEffect } from "react";
import MapView from "./components/MapView";
import Navbar from "./components/Navbar";
import ConfigPanel from "./components/ConfigPanel";
import Legend from "./components/Legend";
import Alerts from "./components/Alerts";

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [userLocation, setUserLocation] = useState(null);

  // configuration states
  const [threshold, setThreshold] = useState(60);
  const [startTime, setStartTime] = useState("");
  const [endTime, setEndTime] = useState("");
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [dataStream, setDataStream] = useState(null);

  // alert state
  const [alertQueue, setAlertQueue] = useState([]);
  const [alertMessages, setAlertMessages] = useState([]);

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
      <Navbar toggleSidebar={() => setSidebarOpen((prev) => !prev)} />
      {/* sidebar section */}
      <div className="flex flex-1 overflow-hidden">
        <div className={`${sidebarOpen ? "p-4" : "p-0"}`}>
          {sidebarOpen && (
            <ConfigPanel
              threshold={threshold}
              setThreshold={setThreshold}
              startTime={startTime}
              setStartTime={setStartTime}
              endTime={endTime}
              setEndTime={setEndTime}
              playbackSpeed={playbackSpeed}
              setPlaybackSpeed={setPlaybackSpeed}
            />
          )}
        </div>
        {/* map section */}
        <div className="flex-1">
          <MapView
            userLocation={userLocation}
            setUserLocation={setUserLocation}
            threshold={threshold}
            playbackSpeed={playbackSpeed}
            setAlertMessages={setAlertMessages}
          />
          <Legend userLocation={userLocation} />
          <Alerts messages={alertMessages} />
        </div>
      </div>
    </div>
  );
}

export default App;
