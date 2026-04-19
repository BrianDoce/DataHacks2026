const BASE = ""

export interface CitySummary {
  city: string
  state: string
  total_permits: number
  total_active_permits: number
  total_annual_kwh: number
  total_annual_savings_usd: number
  total_co2_offset_metric_tons: number
  avg_system_size_kw: number
  high_yield_index_score: number
  last_updated: string
}

export interface Permit {
  permit_id: string
  zip_code: string | null
  city: string
  state: string
  system_size_kw: number | null
  install_date: string | null
  status: string | null
  latitude: number | null
  longitude: number | null
  ac_annual_kwh: number | null
  electricity_rate_per_kwh: number | null
  annual_savings_usd: number | null
  co2_offset_metric_tons: number | null
  enriched_at: string | null
}

export interface PermitPage {
  city: string
  total: number
  limit: number
  offset: number
  permits: Permit[]
}

export interface CityForecast {
  city: string
  state: string
  velocity_permits_per_month: number
  avg_system_size_kw: number
  months: string[]
  monthly_projected_kwh: number[]
  monthly_projected_savings_usd: number[]
  monthly_projected_co2_metric_tons: number[]
}

export interface LeaderboardEntry {
  rank: number
  city: string
  state: string
  total_permits: number
  total_active_permits: number
  total_annual_kwh: number
  total_annual_savings_usd: number
  total_co2_offset_metric_tons: number
  avg_system_size_kw: number
  high_yield_index_score: number
  last_updated: string
}

export interface LeaderboardResponse {
  total: number
  state_filter: string | null
  entries: LeaderboardEntry[]
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) {
    const body = await res.text().catch(() => "")
    throw new Error(`${res.status} ${res.statusText}${body ? `: ${body}` : ""}`)
  }
  return res.json() as Promise<T>
}

export const fetchLeaderboard = (state?: string, limit = 100) =>
  get<LeaderboardResponse>(
    `/api/leaderboard?limit=${limit}${state ? `&state=${encodeURIComponent(state)}` : ""}`
  )

export const fetchCitySummary = (city: string) =>
  get<CitySummary>(`/api/city/${encodeURIComponent(city)}/summary`)

export const fetchCityPermits = (
  city: string,
  opts: { limit?: number; offset?: number } = {}
) => {
  const params = new URLSearchParams()
  if (opts.limit != null) params.set("limit", String(opts.limit))
  if (opts.offset != null) params.set("offset", String(opts.offset))
  return get<PermitPage>(`/api/city/${encodeURIComponent(city)}/permits?${params}`)
}

export const fetchCityForecast = (city: string) =>
  get<CityForecast>(`/api/city/${encodeURIComponent(city)}/forecast`)
