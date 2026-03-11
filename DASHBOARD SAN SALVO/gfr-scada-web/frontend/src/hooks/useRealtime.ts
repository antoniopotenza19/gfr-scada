import { useEffect, useRef } from 'react'

import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'

type RealtimeOptions = {
  onEvent: (evt: any) => void
  onPollFallback?: () => void
  pollMs?: number
}

export function useRealtime({ onEvent, onPollFallback, pollMs = LIVE_SUMMARY_REFRESH_MS }: RealtimeOptions) {
  const pollRef = useRef<number | null>(null)

  useEffect(() => {
    const base = (import.meta.env.VITE_API_BASE_URL as string | undefined) || 'http://127.0.0.1:8000'
    const wsUrl = base.replace(/^http/, 'ws') + '/ws/realtime'
    const ws = new WebSocket(wsUrl)

    ws.onmessage = (msg) => {
      try {
        onEvent(JSON.parse(msg.data))
      } catch {
        onEvent({ type: 'raw', payload: msg.data })
      }
    }

    ws.onopen = () => {
      if (pollRef.current) {
        window.clearInterval(pollRef.current)
        pollRef.current = null
      }
    }

    ws.onclose = () => {
      if (!pollRef.current && onPollFallback) {
        pollRef.current = window.setInterval(() => onPollFallback(), pollMs)
      }
    }

    return () => {
      ws.close()
      if (pollRef.current) {
        window.clearInterval(pollRef.current)
      }
    }
  }, [onEvent, onPollFallback, pollMs])
}
