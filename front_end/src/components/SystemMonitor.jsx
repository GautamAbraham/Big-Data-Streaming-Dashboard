import { useState, useEffect } from 'react';

export default function SystemMonitor() {
  const [metrics, setMetrics] = useState({
    backendHealth: 'unknown',
    lastHealthCheck: null,
    wsConnections: 0,
    kafkaTopic: '',
    responseTime: null
  });

  useEffect(() => {
    const checkHealth = async () => {
      const startTime = Date.now();
      try {
        const response = await fetch(`${import.meta.env.VITE_API_URL || 'http://localhost:8000'}/health`);
        const data = await response.json();
        const responseTime = Date.now() - startTime;
        
        setMetrics({
          backendHealth: 'healthy',
          lastHealthCheck: new Date().toISOString(),
          wsConnections: data.connected_clients,
          kafkaTopic: data.kafka_topic,
          responseTime
        });
      } catch (error) {
        setMetrics(prev => ({
          ...prev,
          backendHealth: 'unhealthy',
          lastHealthCheck: new Date().toISOString(),
          responseTime: null
        }));
      }
    };

    // Check health immediately and then every 30 seconds
    checkHealth();
    const interval = setInterval(checkHealth, 30000);

    return () => clearInterval(interval);
  }, []);

  const getHealthColor = (status) => {
    switch (status) {
      case 'healthy': return '#22c55e';
      case 'unhealthy': return '#ef4444';
      default: return '#6b7280';
    }
  };

  return (
    <div style={{
      position: 'absolute',
      top: 60,
      right: 10,
      zIndex: 1000,
      background: 'rgba(255, 255, 255, 0.95)',
      padding: '12px',
      borderRadius: '8px',
      fontSize: '12px',
      minWidth: '200px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.1)'
    }}>
      <h4 style={{ margin: '0 0 8px 0', fontSize: '14px' }}>System Status</h4>
      
      <div style={{ marginBottom: '4px' }}>
        <span style={{ color: getHealthColor(metrics.backendHealth) }}>●</span>
        {' '}Backend: {metrics.backendHealth}
      </div>
      
      <div style={{ marginBottom: '4px' }}>
        WebSocket Clients: {metrics.wsConnections}
      </div>
      
      <div style={{ marginBottom: '4px' }}>
        Kafka Topic: {metrics.kafkaTopic}
      </div>
      
      {metrics.responseTime && (
        <div style={{ marginBottom: '4px' }}>
          Response Time: {metrics.responseTime}ms
        </div>
      )}
      
      {metrics.lastHealthCheck && (
        <div style={{ fontSize: '10px', color: '#6b7280' }}>
          Last check: {new Date(metrics.lastHealthCheck).toLocaleTimeString()}
        </div>
      )}
    </div>
  );
}
