import type { SiteId } from '../constants/sites'

export type EffectiveUserRole =
  | 'san_salvo_viewer'
  | 'marghera_viewer'
  | 'gfr'
  | 'dev'
  | 'unknown'

export interface AuthUser {
  role: EffectiveUserRole
  rawRole: string
  allowedSiteIds: SiteId[]
}

function decodeBase64Url(input: string): string {
  const normalized = input.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4)
  return atob(padded)
}

function normalizeRole(role: string): EffectiveUserRole {
  const value = role.trim().toLowerCase()
  if (value === 'san_salvo_viewer' || value === 'san_salso_viewer' || value === 'san-salvo-viewer') return 'san_salvo_viewer'
  if (value === 'marghera_viewer') return 'marghera_viewer'
  if (value === 'gfr') return 'gfr'
  if (value === 'dev') return 'dev'

  // Legacy role compatibility
  if (value === 'viewer') return 'san_salvo_viewer'
  if (value === 'operator') return 'gfr'
  if (value === 'admin') return 'dev'

  return 'unknown'
}

function allowedSitesFromRole(role: EffectiveUserRole): SiteId[] {
  if (role === 'san_salvo_viewer') return ['san-salvo']
  if (role === 'marghera_viewer') return ['marghera']
  if (role === 'gfr' || role === 'dev') return ['san-salvo', 'marghera']
  return []
}

export function getAuthUserFromSessionToken(): AuthUser {
  try {
    const token = sessionStorage.getItem('gfr_token')
    if (!token) return { role: 'unknown', rawRole: '', allowedSiteIds: [] }
    const parts = token.split('.')
    if (parts.length < 2) return { role: 'unknown', rawRole: '', allowedSiteIds: [] }
    const payloadJson = decodeBase64Url(parts[1])
    const payload = JSON.parse(payloadJson)
    const rawRole = String(payload?.role || '')
    const role = normalizeRole(rawRole)
    return { role, rawRole, allowedSiteIds: allowedSitesFromRole(role) }
  } catch {
    return { role: 'unknown', rawRole: '', allowedSiteIds: [] }
  }
}

export function canViewSite(user: AuthUser | null | undefined, siteId: SiteId): boolean {
  if (!user) return false
  return user.allowedSiteIds.includes(siteId)
}

export function canViewDevFeatures(user: AuthUser | null | undefined): boolean {
  if (!user) return false
  return user.role === 'dev'
}

export function canRemoteControl(user: AuthUser | null | undefined): boolean {
  if (!user) return false
  return user.role === 'gfr' || user.role === 'dev'
}

// Backward-compatible exports used by existing code.
export function getUserRoleFromSessionToken(): EffectiveUserRole {
  return getAuthUserFromSessionToken().role
}

export function canSendCommands(role: EffectiveUserRole): boolean {
  return role === 'gfr' || role === 'dev'
}

export function defaultPathForUser(user: AuthUser | null | undefined): string {
  if (!user || user.allowedSiteIds.length === 0) return '/sites'
  if (user.allowedSiteIds.length === 1) return `/dashboard?site=${user.allowedSiteIds[0]}`
  return '/sites'
}
