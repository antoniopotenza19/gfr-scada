export type SiteId = 'san-salvo' | 'marghera'

export interface SiteDefinition {
  id: SiteId
  name: string
  legacyKey: 'SAN SALVO' | 'MARGHERA'
}

export const SITES: SiteDefinition[] = [
  { id: 'san-salvo', name: 'San Salvo', legacyKey: 'SAN SALVO' },
  { id: 'marghera', name: 'Marghera', legacyKey: 'MARGHERA' },
]

const SITE_ID_TO_LEGACY: Record<SiteId, SiteDefinition['legacyKey']> = {
  'san-salvo': 'SAN SALVO',
  marghera: 'MARGHERA',
}

const LEGACY_TO_SITE_ID: Record<SiteDefinition['legacyKey'], SiteId> = {
  'SAN SALVO': 'san-salvo',
  MARGHERA: 'marghera',
}

export function isSiteId(value: string | null | undefined): value is SiteId {
  return value === 'san-salvo' || value === 'marghera'
}

export function siteIdToLegacyKey(siteId: SiteId | string | null | undefined): SiteDefinition['legacyKey'] | null {
  if (!siteId || !isSiteId(siteId)) return null
  return SITE_ID_TO_LEGACY[siteId]
}

export function legacyKeyToSiteId(legacyKey: string | null | undefined): SiteId | null {
  if (!legacyKey) return null
  const normalized = legacyKey.trim().toUpperCase() as SiteDefinition['legacyKey']
  return LEGACY_TO_SITE_ID[normalized] || null
}

export function siteNameFromId(siteId: SiteId): string {
  return SITES.find((site) => site.id === siteId)?.name || siteId
}
