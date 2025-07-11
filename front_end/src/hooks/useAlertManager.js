import { useState, useCallback, useRef } from "react";

export function useAlertManager() {
    const [alerts, setAlerts] = useState([]);
    const alertCooldowns = useRef(new Map()); // Prevent spam
    const alertCount = useRef(0);

    const addAlert = useCallback(
        (message, severity = "warning", location = null) => {
            const now = Date.now();

            // Create location key for duplication prevention
            const locationKey =
                location &&
                typeof location.lat === "number" &&
                typeof location.lon === "number" &&
                !isNaN(location.lat) &&
                !isNaN(location.lon)
                    ? `${location.lat.toFixed(3)},${location.lon.toFixed(3)}`
                    : "general";
            const alertKey = `${severity}-${locationKey}`;

            // prevent same location alerts within 30 seconds
            const lastAlert = alertCooldowns.current.get(alertKey);
            if (lastAlert && now - lastAlert < 30000) {
                return;
            }

            alertCooldowns.current.set(alertKey, now);

            // Sanitize location data to prevent undefined access
            const validLocation =
                location &&
                typeof location.lat === "number" &&
                typeof location.lon === "number" &&
                !isNaN(location.lat) &&
                !isNaN(location.lon)
                    ? { lat: location.lat, lon: location.lon }
                    : null;

            const alert = {
                id: `alert-${alertCount.current++}`,
                message,
                severity, // 'critical', 'warning', 'info'
                timestamp: now,
                location: validLocation,
                dismissed: false,
            };

            setAlerts((prev) => {
                // Keep only last 5 alerts and remove old ones
                const filtered = prev.filter((a) => !a.dismissed).slice(-4);
                return [...filtered, alert];
            });

            // Auto-dismiss based on severity - increased timeouts for better readability
            const dismissTime =
                severity === "critical"
                    ? 15000
                    : severity === "warning"
                    ? 10000
                    : 7000;
            setTimeout(() => {
                dismissAlert(alert.id);
            }, dismissTime);
        },
        []
    );

    const dismissAlert = useCallback((alertId) => {
        setAlerts((prev) =>
            prev.map((alert) =>
                alert.id === alertId ? { ...alert, dismissed: true } : alert
            )
        );

        // Remove dismissed alerts after animation
        setTimeout(() => {
            setAlerts((prev) => prev.filter((alert) => alert.id !== alertId));
        }, 300);
    }, []);

    const clearAllAlerts = useCallback(() => {
        setAlerts([]);
        alertCooldowns.current.clear();
    }, []);

    return {
        alerts: alerts.filter((a) => !a.dismissed),
        addAlert,
        dismissAlert,
        clearAllAlerts,
    };
}
