import { useCallback } from 'react';

export function usePointerCursor(mapRef, layerId = 'radiation-points') {
  return useCallback(evt => {
    const canvas = mapRef.current.getMap().getCanvas();
    const over = evt.features?.some(f => f.layer.id === layerId);
    canvas.style.cursor = over ? 'pointer' : '';
  }, [mapRef, layerId]);
}

export function useMapClick(setSelectedPoint, layerId = 'radiation-points') {
  return useCallback(evt => {
    const feature = evt.features?.find(f => f.layer.id === layerId);

    setSelectedPoint(prev => {
      if (!feature) return prev ? null : prev;

      const [lon, lat] = feature.geometry.coordinates;
      if (prev && prev.geometry.coordinates[0] === lon && prev.geometry.coordinates[1] === lat) {
        return prev;
      }

      return feature;
    });
  }, [setSelectedPoint, layerId]);
}
