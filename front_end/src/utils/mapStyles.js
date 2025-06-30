export function getCircleColor(thresholds = [100, 50, 10], colors = ["#b91c1c", "#eab308", "#22c55e", "#6b7280"]) {
  const expr = ["case"];
  thresholds.forEach((thr, i) => {
    expr.push([">=", ["get", "value"], thr], colors[i]);
  });
  expr.push(colors[colors.length - 1]);
  return expr;
}

export function getCircleRadius(thresholds = [100, 50, 20], radii = [8, 6, 5, 4]) {
  const expr = ["case"];
  thresholds.forEach((thr, i) => {
    expr.push([">=", ["get", "value"], thr], radii[i]);
  });
  expr.push(radii[radii.length - 1]);
  return expr;
}

export function getLevelFromValue(value) {
  if (value == null) return "very-low";
  if (value >= 100) return "very-high";
  if (value >= 50) return "high";
  if (value >= 20) return "moderate";
  if (value >= 10) return "low";
  return "very-low";
}