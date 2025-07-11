import { useEffect, useRef, useCallback } from "react";

export function useWebSocket({
    wsUrl,
    playbackSpeed,
    threshold,
    onDataPoints,
    onConnectionStatus,
    onAlert,
}) {
    const wsRef = useRef(null);
    const reconnectTimeoutRef = useRef(null);
    const thresholdRef = useRef(threshold);

    // keep thresholdRef up to date
    useEffect(() => {
        thresholdRef.current = threshold;
    }, [threshold]);

    const connect = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) return;

        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;
        let buffer = [];
        let bufferTimer;

        ws.onopen = () => {
            onConnectionStatus("connected");

            const interval = Math.max(50, 1000 / Math.max(playbackSpeed, 0.1));
            bufferTimer = setInterval(() => {
                if (buffer.length) {
                    onDataPoints(buffer.splice(0));
                }
            }, interval);
        };

        ws.onmessage = ({ data }) => {
            try {
                const msg = JSON.parse(data);
                if (msg.type === "heartbeat") {
                    return;
                }

                const lat = +msg.lat,
                    lon = +msg.lon,
                    value = +msg.value;

                if (
                    !isNaN(lat) &&
                    lat >= -90 &&
                    lat <= 90 &&
                    !isNaN(lon) &&
                    lon >= -180 &&
                    lon <= 180 &&
                    !isNaN(value)
                ) {
                    if (value >= thresholdRef.current) {
                        onAlert(
                            `High radiation: ${value} CPM`,
                            value,
                            lat,
                            lon
                        );
                    }
                    const dataPoint = { ...msg, lat, lon, value };
                    buffer.push(dataPoint);
                } else {
                    console.warn("Invalid data received:", { lat, lon, value });
                }
            } catch (e) {
                console.warn("WebSocket parse error:", e);
            }
        };

        ws.onclose = () => {
            onConnectionStatus("disconnected");
            clearInterval(bufferTimer);
            reconnectTimeoutRef.current = setTimeout(connect, 3000);
        };

        ws.onerror = (err) => {
            console.error("WS error", err);
            onConnectionStatus("error");
        };

        return () => {
            clearInterval(bufferTimer);
            clearTimeout(reconnectTimeoutRef.current);
            ws.close();
        };
    }, [wsUrl, playbackSpeed, onDataPoints, onConnectionStatus, onAlert]);

    useEffect(() => {
        const cleanup = connect();
        return () => cleanup && cleanup();
    }, [connect]);
}
