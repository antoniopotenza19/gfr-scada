function parsePositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number.parseInt(String(value || ''), 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

export const LIVE_SUMMARY_REFRESH_MS = parsePositiveInt(
  import.meta.env.VITE_SCADA_SUMMARY_REFRESH_MS,
  5_000
)
