import type { SiteId } from '../constants/sites'
import { isSiteId } from '../constants/sites'

const SELECTED_SITE_SESSION_KEY = 'gfr_selected_site_id'

export function getSelectedSiteId(): SiteId | null {
  const value = sessionStorage.getItem(SELECTED_SITE_SESSION_KEY)
  return isSiteId(value) ? value : null
}

export function setSelectedSiteId(siteId: SiteId): void {
  sessionStorage.setItem(SELECTED_SITE_SESSION_KEY, siteId)
}

export function clearSelectedSiteId(): void {
  sessionStorage.removeItem(SELECTED_SITE_SESSION_KEY)
}
