const LAST_SELECTED_SALA_KEY = 'gfr_last_selected_sala'

export function getLastSelectedSala(): string {
  if (typeof window === 'undefined') return ''
  try {
    return window.sessionStorage.getItem(LAST_SELECTED_SALA_KEY) || ''
  } catch {
    return ''
  }
}

export function setLastSelectedSala(value: string) {
  if (typeof window === 'undefined') return
  const normalized = value.trim()
  if (!normalized) return
  try {
    window.sessionStorage.setItem(LAST_SELECTED_SALA_KEY, normalized)
  } catch {
    // Ignore storage failures.
  }
}
