import type { PlantSummary } from '../types/api'

export const ROOM_ALIASES: Record<string, string[]> = {
  LAMINATO: ['LAMINATI', 'LaminatiAlta', 'LaminatiBassa'],
  LAMINATI: ['LAMINATO', 'LaminatiAlta', 'LaminatiBassa'],
  'PRIMO ALTA': ['PRIMOAlta'],
  'PRIMO BASSA': ['PRIMOBassa'],
  'SS1 COMPOSIZIONE': ['COMPOSIZIONE', 'SS1_COMP'],
  'SS2 COMPOSIZIONE': ['SS2 Bassa Pressione', 'SS2_COMP'],
}

export const SUMMARY_CACHE_KEY = 'gfr_dashboard_summary_cache_v1'

const FORCE_UNMAPPED_LABELS = new Set<string>()

function resolveApiRoomsForLabel(
  label: string,
  normalizedPlants: Map<string, string>,
  canonicalPlants: Map<string, string>
) {
  const names = [label, ...(ROOM_ALIASES[label] || [])]
  const found: string[] = []
  for (const name of names) {
    const direct = normalizedPlants.get(name.trim().toUpperCase().replace(/\s+/g, ' '))
    if (direct) {
      found.push(direct)
      continue
    }
    const canonical = canonicalPlants.get(name.toUpperCase().replace(/[^A-Z0-9]/g, ''))
    if (canonical) found.push(canonical)
  }
  return Array.from(new Set(found))
}

export function buildRoomApiMapping(
  labels: string[],
  normalizedPlants: Map<string, string>,
  canonicalPlants: Map<string, string>
) {
  const mapping = new Map<string, string[]>()
  const assignedPlants = new Set<string>()
  const unresolved: string[] = []

  for (const label of labels) {
    if (FORCE_UNMAPPED_LABELS.has(label)) {
      mapping.set(label, [])
      continue
    }
    const direct =
      normalizedPlants.get(label.trim().toUpperCase().replace(/\s+/g, ' ')) ||
      canonicalPlants.get(label.toUpperCase().replace(/[^A-Z0-9]/g, ''))
    if (direct) {
      mapping.set(label, [direct])
      assignedPlants.add(direct)
    } else {
      unresolved.push(label)
      mapping.set(label, [])
    }
  }

  for (const label of unresolved) {
    const aliases = resolveApiRoomsForLabel(label, normalizedPlants, canonicalPlants).filter(
      (plant) => !assignedPlants.has(plant)
    )
    if (aliases.length > 0) {
      mapping.set(label, aliases)
      aliases.forEach((plant) => assignedPlants.add(plant))
    }
  }

  return mapping
}

export function loadSummaryCache(): Record<string, PlantSummary> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.sessionStorage.getItem(SUMMARY_CACHE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return {}
    return parsed as Record<string, PlantSummary>
  } catch {
    return {}
  }
}
