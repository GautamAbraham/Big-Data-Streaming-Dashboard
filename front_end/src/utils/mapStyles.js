export function getCircleColor(
    thresholds = [1000, 200, 50],
    colors = ["#8b0000", "#dc2626", "#eab308", "#22c55e"]
) {
    const expr = ["case"];
    thresholds.forEach((thr, i) => {
        expr.push([">=", ["get", "value"], thr], colors[i]);
    });
    expr.push(colors[colors.length - 1]);
    return expr;
}

export function getCircleRadius(
    thresholds = [1000, 200, 50],
    radii = [8, 6, 5, 4]
) {
    const expr = ["case"];
    thresholds.forEach((thr, i) => {
        expr.push([">=", ["get", "value"], thr], radii[i]);
    });
    expr.push(radii[radii.length - 1]);
    return expr;
}

export function getLevelFromValue(value) {
    if (value == null) return "very-low";
    if (value >= 1000) return "very-high";
    if (value >= 200) return "high";
    if (value >= 50) return "moderate";
    if (value >= 10) return "low";
    return "very-low";
}
