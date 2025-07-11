import { X, AlertTriangle, AlertCircle, Info } from "lucide-react";

function AlertToast({ alert, onDismiss }) {
  const getSeverityConfig = (severity) => {
    switch (severity) {
      case "critical":
        return {
          icon: AlertTriangle,
          bgColor: "bg-red-50 border-red-200",
          iconColor: "text-red-600",
          textColor: "text-red-800",
          pulse: "animate-pulse",
        };
      case "warning":
        return {
          icon: AlertCircle,
          bgColor: "bg-yellow-50 border-yellow-200",
          iconColor: "text-yellow-600",
          textColor: "text-yellow-800",
          pulse: "",
        };
      default:
        return {
          icon: Info,
          bgColor: "bg-blue-50 border-blue-200",
          iconColor: "text-blue-600",
          textColor: "text-blue-800",
          pulse: "",
        };
    }
  };

  const config = getSeverityConfig(alert.severity);
  const Icon = config.icon;

  return (
    <div
      className={`
      flex items-start gap-3 p-4 rounded-lg border shadow-lg backdrop-blur-sm
      ${config.bgColor} ${config.pulse}
      transform transition-all duration-300 ease-in-out
      hover:scale-105 max-w-sm
    `}
    >
      <Icon className={`w-5 h-5 mt-0.5 flex-shrink-0 ${config.iconColor}`} />

      <div className="flex-1 min-w-0">
        <p className={`text-sm font-medium ${config.textColor}`}>
          {alert.message}
        </p>
        <p className="text-xs text-gray-500 mt-1">
          {alert.location
            ? `${alert.location.lat.toFixed(4)}, ${alert.location.lon.toFixed(4)}`
            : "General alert"}
        </p>
        <p className="text-xs text-gray-400 mt-1">
          {new Date(alert.timestamp).toLocaleTimeString()}
        </p>
      </div>

      <button
        onClick={() => onDismiss(alert.id)}
        className="flex-shrink-0 p-1 rounded hover:bg-gray-200 transition-colors"
      >
        <X className="w-4 h-4 text-gray-400" />
      </button>
    </div>
  );
}

function Alerts({ alerts, onDismiss }) {
  if (!alerts || alerts.length === 0) return null;

  return (
    <div className="fixed top-20 right-4 z-50 space-y-2 max-h-[60vh] overflow-y-auto">
      {alerts.map((alert) => (
        <AlertToast key={alert.id} alert={alert} onDismiss={onDismiss} />
      ))}
    </div>
  );
}

export default Alerts;
