import { useCallback, useEffect, useMemo, useRef } from 'react'
import L from 'leaflet'

export type MapMarkerState = 'active' | 'warning' | 'alarm' | 'dismissed'
const DEFAULT_ZOOM = 13.8
const BOTTOM_TOOLTIP_ROOMS = new Set(['LAMINATO', 'LAMINATI', 'BRAVO', 'CENTAC', 'SS2'])

interface PlantGeoMapProps {
  rooms: string[]
  selectedRoom: string
  markerStates: Record<string, MapMarkerState>
  bookmarks: Record<string, [number, number]>
  center: [number, number]
  onSelectRoom: (room: string) => void
}

function markerStyle(state: MapMarkerState, selected: boolean) {
  if (state === 'alarm') {
    return { fill: '#ef4444', stroke: '#b91c1c', radius: selected ? 10 : 8 }
  }
  if (state === 'warning') {
    return { fill: '#f59e0b', stroke: '#b45309', radius: selected ? 10 : 8 }
  }
  if (state === 'dismissed') {
    return { fill: '#9ca3af', stroke: '#4b5563', radius: selected ? 9 : 7 }
  }
  return { fill: '#22c55e', stroke: '#15803d', radius: selected ? 10 : 8 }
}

export default function PlantGeoMap({
  rooms,
  selectedRoom,
  markerStates,
  bookmarks,
  center,
  onSelectRoom,
}: PlantGeoMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<L.Map | null>(null)
  const markerLayerRef = useRef<L.LayerGroup | null>(null)

  const points = useMemo(
    () =>
      rooms
        .filter((room) => Boolean(bookmarks[room]))
        .map((room) => ({
          room,
          coord: bookmarks[room],
          state: markerStates[room] || 'warning',
          selected: room === selectedRoom,
        })),
    [rooms, bookmarks, markerStates, selectedRoom]
  )

  const recenterMap = useCallback(() => {
    const map = mapRef.current
    if (!map) return
    map.setView(center, DEFAULT_ZOOM)
  }, [center])

  useEffect(() => {
    const el = containerRef.current
    if (!el || mapRef.current) return

    const map = L.map(el, {
      zoomControl: true,
      scrollWheelZoom: true,
      attributionControl: false,
    }).setView(center, DEFAULT_ZOOM)

    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community',
      maxZoom: 19,
    }).addTo(map)

    markerLayerRef.current = L.layerGroup().addTo(map)
    mapRef.current = map

    setTimeout(() => {
      map.invalidateSize()
    }, 0)

    return () => {
      markerLayerRef.current?.clearLayers()
      markerLayerRef.current = null
      map.remove()
      mapRef.current = null
    }
  }, [center])

  useEffect(() => {
    const markerLayer = markerLayerRef.current
    if (!markerLayer) return

    markerLayer.clearLayers()

    for (const point of points) {
      const style = markerStyle(point.state, point.selected)
      const marker = L.circleMarker(point.coord, {
        color: style.stroke,
        fillColor: style.fill,
        fillOpacity: 0.9,
        weight: point.selected ? 3 : 2,
        radius: style.radius,
      })
      const tooltipOnBottom = BOTTOM_TOOLTIP_ROOMS.has(point.room)
      marker.bindTooltip(point.room, {
        direction: tooltipOnBottom ? 'bottom' : 'top',
        offset: tooltipOnBottom ? [0, 8] : [0, -6],
        opacity: 0.95,
      })
      marker.on('click', () => onSelectRoom(point.room))
      marker.addTo(markerLayer)
    }
  }, [points, onSelectRoom])

  return (
    <div className="relative h-full min-h-[20rem] w-full">
      <div ref={containerRef} className="h-full min-h-[20rem] w-full rounded-md" />
      <button
        type="button"
        onClick={recenterMap}
        className="absolute right-3 top-3 z-[500] rounded-md border border-slate-300 bg-white/95 px-3 py-1.5 text-xs font-medium text-slate-700 shadow-sm hover:bg-white"
      >
        Ricentra
      </button>
    </div>
  )
}
