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
