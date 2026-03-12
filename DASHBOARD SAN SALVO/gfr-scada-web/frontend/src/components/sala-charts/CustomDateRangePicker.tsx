import { useEffect, useMemo, useState } from 'react'
import { DayPicker, type DateRange } from 'react-day-picker'

import './customDateRangePicker.css'

interface CustomDateRangePickerProps {
  fromValue: string
  toValue: string
  onApplyRange: (fromValue: string, toValue: string) => void
  error?: string | null
}

function pad(value: number) {
  return String(value).padStart(2, '0')
}

function parseLocalDateTime(value: string) {
  if (!value) return null
  const parsed = new Date(value)
  return Number.isFinite(parsed.getTime()) ? parsed : null
}

function toLocalDateInputValue(value: Date) {
  return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`
}

function toLocalTimeInputValue(value: Date) {
  return `${pad(value.getHours())}:${pad(value.getMinutes())}`
}

function buildLocalDateTimeValue(dateValue: Date, timeValue: string, fallbackTime: string) {
  const [hours, minutes] = (timeValue || fallbackTime).split(':')
  const next = new Date(dateValue)
  next.setHours(Number(hours || 0), Number(minutes || 0), 0, 0)
  return `${toLocalDateInputValue(next)}T${toLocalTimeInputValue(next)}`
}

function updateTimePart(currentValue: string, nextTime: string, fallback: Date) {
  const base = parseLocalDateTime(currentValue) || fallback
  return buildLocalDateTimeValue(base, nextTime, toLocalTimeInputValue(base))
}

function formatSummaryDate(value: string) {
  const parsed = parseLocalDateTime(value)
  if (!parsed) {
    return {
      day: '--',
      monthYear: 'Intervallo',
      time: '--:--',
    }
  }
  return {
    day: parsed.toLocaleDateString('it-IT', { day: '2-digit', month: 'long', year: 'numeric' }),
    time: parsed.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' }),
  }
}

function CalendarIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-5 w-5">
      <path
        d="M7 2.75a.75.75 0 0 1 .75.75v1h8.5v-1a.75.75 0 0 1 1.5 0v1h.75A2.5 2.5 0 0 1 21 7v11.25a2.5 2.5 0 0 1-2.5 2.5h-13A2.5 2.5 0 0 1 3 18.25V7a2.5 2.5 0 0 1 2.5-2.5h.75v-1A.75.75 0 0 1 7 2.75Zm11.5 6H4.5v9.5c0 .552.448 1 1 1h13c.552 0 1-.448 1-1v-9.5Z"
        fill="currentColor"
      />
    </svg>
  )
}

const VISIBLE_MONTHS = 4
const CURRENT_MONTH_INDEX = 2
const HOUR_OPTIONS = Array.from({ length: 24 }, (_, index) => pad(index))
const MINUTE_OPTIONS = Array.from({ length: 60 }, (_, index) => pad(index))

function addMonths(value: Date, amount: number) {
  return new Date(value.getFullYear(), value.getMonth() + amount, 1)
}

function startOfMonth(value: Date) {
  return new Date(value.getFullYear(), value.getMonth(), 1)
}

function anchoredMonth() {
  return addMonths(startOfMonth(new Date()), -CURRENT_MONTH_INDEX)
}

function normalizeTimeSlot(value: string, fallbackTime: string) {
  const [rawHours = fallbackTime.slice(0, 2), rawMinutes = fallbackTime.slice(3, 5)] = value.split(':')
  const safeHours = Math.min(23, Math.max(0, Number(rawHours || 0)))
  const safeMinutes = Math.min(59, Math.max(0, Number(rawMinutes || 0)))
  return `${pad(safeHours)}:${pad(safeMinutes)}`
}

function normalizeDateTimeValue(value: string, fallbackTime: string) {
  const parsed = parseLocalDateTime(value)
  if (!parsed) return value
  return buildLocalDateTimeValue(parsed, normalizeTimeSlot(toLocalTimeInputValue(parsed), fallbackTime), fallbackTime)
}

interface QuickTimeSelectorProps {
  label: string
  value: string
  onChange: (value: string) => void
}

function QuickTimeSelector({ label, value, onChange }: QuickTimeSelectorProps) {
  const normalizedValue = normalizeTimeSlot(value, label === 'Orario 2' ? '23:59' : '00:00')
  const [hourValue, minuteValue] = normalizedValue.split(':')

  return (
    <div className="rounded-2xl border border-slate-200/90 bg-white/90 p-4 shadow-[0_14px_28px_-24px_rgba(15,23,42,0.45)]">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</div>
        <div className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-sm font-semibold text-slate-700">
          {normalizedValue}
        </div>
      </div>
      <div className="grid gap-3 md:grid-cols-[1fr_0.9fr_auto] md:items-end">
        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-700">Ora</span>
          <select
            value={hourValue}
            onChange={(event) => onChange(`${event.target.value}:${minuteValue}`)}
            className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2.5 text-sm font-medium text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
          >
            {HOUR_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>

        <label className="space-y-2">
          <span className="text-sm font-medium text-slate-700">Minuti</span>
          <select
            value={minuteValue}
            onChange={(event) => onChange(`${hourValue}:${event.target.value}`)}
            className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2.5 text-sm font-medium text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
          >
            {MINUTE_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>

        <div className="flex flex-wrap gap-2 md:justify-end">
          {label === 'Orario 1' ? (
            <button
              type="button"
              onClick={() => onChange('00:00')}
              className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-600 transition hover:border-slate-300 hover:bg-white"
            >
              Inizio giornata
            </button>
          ) : (
            <button
              type="button"
              onClick={() => onChange('23:59')}
              className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-600 transition hover:border-slate-300 hover:bg-white"
            >
              Fine giornata
            </button>
          )}
        </div>
      </div>
      <div className="mt-3 text-xs text-slate-600">Step selezione: 1 minuto</div>
    </div>
  )
}

function RangeSummaryCard({ fromValue, toValue }: { fromValue: string; toValue: string }) {
  const fromSummary = formatSummaryDate(fromValue)
  const toSummary = formatSummaryDate(toValue)

  return (
    <div className="min-w-0 flex-1 rounded-2xl border border-slate-200/90 bg-[linear-gradient(180deg,_#ffffff,_#f8fafc)] px-4 py-3.5 shadow-[0_14px_30px_-24px_rgba(15,23,42,0.35)]">
      <div className="mb-3 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Intervallo attuale</div>
      <div className="flex items-center gap-3">
        <div className="min-w-0 flex-1 rounded-xl border border-slate-200 bg-slate-50/90 px-3.5 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
          <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Da</div>
          <div className="mt-1">
            <span className="block text-2xl font-bold leading-none tracking-[-0.02em] text-slate-950">{fromSummary.day}</span>
          </div>
          <div className="mt-1.5 text-sm font-medium text-slate-700">{fromSummary.time}</div>
        </div>

        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-500 shadow-[0_10px_20px_-16px_rgba(15,23,42,0.3)]">
          <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
            <path d="M5 12h14m-5-5 5 5-5 5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>

        <div className="min-w-0 flex-1 rounded-xl border border-slate-200 bg-slate-50/90 px-3.5 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
          <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">A</div>
          <div className="mt-1">
            <span className="block text-2xl font-bold leading-none tracking-[-0.02em] text-slate-950">{toSummary.day}</span>
          </div>
          <div className="mt-1.5 text-sm font-medium text-slate-700">{toSummary.time}</div>
        </div>
      </div>
    </div>
  )
}

export default function CustomDateRangePicker({
  fromValue,
  toValue,
  onApplyRange,
  error,
}: CustomDateRangePickerProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [draftFrom, setDraftFrom] = useState(fromValue)
  const [draftTo, setDraftTo] = useState(toValue)
  const [displayMonth, setDisplayMonth] = useState(() => anchoredMonth())

  useEffect(() => {
    setDraftFrom(fromValue)
    setDraftTo(toValue)
  }, [fromValue, toValue])

  useEffect(() => {
    if (!isOpen) {
      setDisplayMonth(anchoredMonth())
    }
  }, [isOpen])

  const fromDate = parseLocalDateTime(draftFrom)
  const toDate = parseLocalDateTime(draftTo)
  const selectedRange = useMemo<DateRange | undefined>(
    () => ({
      from: fromDate || undefined,
      to: toDate || undefined,
    }),
    [fromDate, toDate]
  )

  const handleRangeSelect = (range: DateRange | undefined) => {
    if (range?.from) {
      setDraftFrom(buildLocalDateTimeValue(range.from, fromDate ? toLocalTimeInputValue(fromDate) : '00:00', '00:00'))
    }
    if (range?.to) {
      setDraftTo(buildLocalDateTimeValue(range.to, toDate ? toLocalTimeInputValue(toDate) : '23:59', '23:59'))
      return
    }
    if (range?.from && !range.to) {
      setDraftTo('')
    }
  }

  const handleCancel = () => {
    setDraftFrom(fromValue)
    setDraftTo(toValue)
    setIsOpen(false)
  }

  const handleApply = () => {
    onApplyRange(
      normalizeDateTimeValue(draftFrom, '00:00'),
      normalizeDateTimeValue(draftTo, '23:59')
    )
    setIsOpen(false)
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        <div className="flex min-w-[240px] flex-col gap-2">
          <button
            type="button"
            onClick={() =>
              setIsOpen((current) => {
                const next = !current
                if (next) {
                  setDisplayMonth(anchoredMonth())
                }
                return next
              })
            }
            className="inline-flex min-h-[90px] items-center gap-3 rounded-2xl border border-slate-200/90 bg-[linear-gradient(180deg,_#ffffff,_#f8fafc)] px-5 py-4 text-left text-sm font-semibold text-slate-800 shadow-[0_14px_30px_-24px_rgba(15,23,42,0.35)] transition hover:border-slate-300 hover:bg-slate-50"
          >
            <span className="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-slate-200 bg-slate-50 text-slate-500">
              <CalendarIcon />
            </span>
            <span className="flex min-w-0 flex-col">
              <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Range custom</span>
              <span className="mt-1 text-base font-semibold text-slate-950">Intervallo personalizzato</span>
            </span>
          </button>
        </div>
        <RangeSummaryCard fromValue={fromValue} toValue={toValue} />
      </div>

      {isOpen ? (
        <div className="space-y-4 rounded-[28px] border border-slate-200/80 bg-[linear-gradient(180deg,_rgba(248,250,252,0.96),_rgba(255,255,255,1)),radial-gradient(circle_at_top_left,_rgba(13,148,136,0.08),_transparent_30%)] p-4 shadow-[0_18px_45px_-30px_rgba(15,23,42,0.35)] sm:p-5">
          <div className="sala-range-picker">
            <DayPicker
              mode="range"
              selected={selectedRange}
              onSelect={handleRangeSelect}
              numberOfMonths={VISIBLE_MONTHS}
              month={displayMonth}
              onMonthChange={setDisplayMonth}
              showOutsideDays
              defaultMonth={anchoredMonth()}
              pagedNavigation
            />
          </div>

          <div className="grid gap-3 border-t border-slate-200/80 pt-4 md:grid-cols-2">
            <QuickTimeSelector
              label="Orario 1"
              value={fromDate ? toLocalTimeInputValue(fromDate) : '00:00'}
              onChange={(value) => setDraftFrom(updateTimePart(draftFrom, value, fromDate || new Date()))}
            />

            <QuickTimeSelector
              label="Orario 2"
              value={toDate ? toLocalTimeInputValue(toDate) : '23:59'}
              onChange={(value) => setDraftTo(updateTimePart(draftTo, value, toDate || new Date()))}
            />
          </div>

          <div className="flex flex-wrap justify-end gap-2 border-t border-slate-200/80 pt-4">
            <button
              type="button"
              onClick={handleCancel}
              className="rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-slate-50"
            >
              Annulla
            </button>
            <button
              type="button"
              onClick={handleApply}
              className="rounded-xl border border-teal-600 bg-gradient-to-r from-sky-500 to-teal-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:from-sky-600 hover:to-teal-700"
            >
              Applica intervallo
            </button>
          </div>
        </div>
      ) : null}

      {error ? <div className="text-sm font-medium text-rose-600">{error}</div> : null}
    </div>
  )
}
