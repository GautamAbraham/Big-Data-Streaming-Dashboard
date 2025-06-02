import { useState } from "react";
import MapView from "./components/MapView";
import Navbar from "./components/Navbar";
import ConfigPanel from "./components/ConfigPanel";
import Legend from "./components/Legend";
import Alerts from "./components/Alerts";

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // configuration states
  const [threshold, setThreshold] = useState(100);
  const [north, setNorth] = useState(90);
  const [south, setSouth] = useState(-90);
  const [east, setEast] = useState(180);
  const [west, setWest] = useState(-180);
  const [startTime, setStartTime] = useState("");
  const [endTime, setEndTime] = useState("");
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [dataStream, setDataStream] = useState(null);

  // Alerts state
  const [alertQueue, setAlertQueue] = useState([]);
  const [alertMessages, setAlertMessages] = useState([]);


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
              north={north}
              setNorth={setNorth}
              south={south}
              setSouth={setSouth}
              east={east}
              setEast={setEast}
              west={west}
              setWest={setWest}
              startTime={startTime}
              setStartTime={setStartTime}
              endTime={endTime}
              setEndTime={setEndTime}
              playbackSpeed={playbackSpeed}
              setPlaybackSpeed={setPlaybackSpeed}
            />
          )}
        </div>

        <div className="flex-1">
          <MapView
            threshold={threshold}
            bbox={{ north, south, east, west }}
            timeWindow={{ start: startTime, end: endTime }}
            playbackSpeed={playbackSpeed}
            dataStream={dataStream}
          />
          <Legend />
          <Alerts messages={alertMessages} />
        </div>
      </div>
    </div>
  );
}

export default App;
