import { useState, useEffect } from 'react';
import SystemMonitor from './SystemMonitor';

export default function Dashboard({ children }) {
  const [showMonitor, setShowMonitor] = useState(false);
  const [stats, setStats] = useState({
    totalPoints: 0,
    dangerousPoints: 0,
    averageRadiation: 0,
    lastUpdate: null
  });

  const toggleMonitor = () => setShowMonitor(!showMonitor);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {children}
      
      {/* Toggle Button */}
      <button
        onClick={toggleMonitor}
        style={{
          position: 'absolute',
          top: 10,
          left: 10,
          zIndex: 1000,
          background: '#3b82f6',
          color: 'white',
          border: 'none',
          padding: '8px 12px',
          borderRadius: '4px',
          cursor: 'pointer',
          fontSize: '12px'
        }}
      >
        {showMonitor ? 'Hide Monitor' : 'Show Monitor'}
      </button>

      {/* System Monitor */}
      {showMonitor && <SystemMonitor />}

      {/* Data Statistics Panel */}
      <div style={{
        position: 'absolute',
        bottom: 10,
        right: 10,
        zIndex: 1000,
        background: 'rgba(0, 0, 0, 0.8)',
        color: 'white',
        padding: '12px',
        borderRadius: '8px',
        fontSize: '12px',
        minWidth: '200px'
      }}>
        <h4 style={{ margin: '0 0 8px 0', fontSize: '14px' }}>Data Statistics</h4>
        <div>Total Points: {stats.totalPoints}</div>
        <div>Dangerous Points: {stats.dangerousPoints}</div>
        <div>Average Radiation: {stats.averageRadiation.toFixed(2)} CPM</div>
        {stats.lastUpdate && (
          <div style={{ fontSize: '10px', marginTop: '8px', color: '#ccc' }}>
            Last Update: {new Date(stats.lastUpdate).toLocaleTimeString()}
          </div>
        )}
      </div>

      {/* Legend */}
      <div style={{
        position: 'absolute',
        bottom: 10,
        left: 10,
        zIndex: 1000,
        background: 'rgba(255, 255, 255, 0.95)',
        padding: '12px',
        borderRadius: '8px',
        fontSize: '12px'
      }}>
        <h4 style={{ margin: '0 0 8px 0', fontSize: '14px' }}>Radiation Levels</h4>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#22c55e', marginRight: 8 }}></div>
            Very Low (&lt;10 CPM)
          </div>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#22c55e', marginRight: 8 }}></div>
            Low (10-50 CPM)
          </div>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#eab308', marginRight: 8 }}></div>
            Moderate (50-200 CPM)
          </div>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#ea580c', marginRight: 8 }}></div>
            High (200-1000 CPM)
          </div>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div style={{ width: 12, height: 12, borderRadius: '50%', background: '#dc2626', marginRight: 8 }}></div>
            Very High (&gt;1000 CPM)
          </div>
        </div>
      </div>
    </div>
  );
}
