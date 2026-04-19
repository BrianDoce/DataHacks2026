import { NextResponse } from "next/server"

const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8000"

// Per-city fixture data derived from leaderboard
const CITY_DATA: Record<string, { state: string; total_permits: number; total_active_permits: number; total_annual_kwh: number; total_annual_savings_usd: number; total_co2_offset_metric_tons: number; avg_system_size_kw: number; high_yield_index_score: number }> = {
  "san diego":    { state: "CA", total_permits: 1247, total_active_permits: 1089, total_annual_kwh: 18245320, total_annual_savings_usd: 4013970.4, total_co2_offset_metric_tons: 3831.52, avg_system_size_kw: 9.2, high_yield_index_score: 87.34 },
  "los angeles":  { state: "CA", total_permits: 2891, total_active_permits: 2604, total_annual_kwh: 39870500, total_annual_savings_usd: 8771510,   total_co2_offset_metric_tons: 8372.8,  avg_system_size_kw: 8.7, high_yield_index_score: 84.91 },
  "phoenix":      { state: "AZ", total_permits: 3102, total_active_permits: 2789, total_annual_kwh: 52341800, total_annual_savings_usd: 6280916,   total_co2_offset_metric_tons: 11013.78, avg_system_size_kw: 10.6, high_yield_index_score: 82.17 },
  "las vegas":    { state: "NV", total_permits: 1876, total_active_permits: 1712, total_annual_kwh: 33841600, total_annual_savings_usd: 3722576,   total_co2_offset_metric_tons: 7111.54, avg_system_size_kw: 11.1, high_yield_index_score: 79.56 },
  "austin":       { state: "TX", total_permits: 1543, total_active_permits: 1388, total_annual_kwh: 22194000, total_annual_savings_usd: 2441340,   total_co2_offset_metric_tons: 4661.14, avg_system_size_kw: 9.0, high_yield_index_score: 76.23 },
  "denver":       { state: "CO", total_permits: 1198, total_active_permits: 1067, total_annual_kwh: 17816900, total_annual_savings_usd: 2494366,   total_co2_offset_metric_tons: 3742.55, avg_system_size_kw: 9.4, high_yield_index_score: 73.88 },
  "sacramento":   { state: "CA", total_permits: 987,  total_active_permits: 891,  total_annual_kwh: 14900400, total_annual_savings_usd: 3278088,   total_co2_offset_metric_tons: 3129.08, avg_system_size_kw: 9.4, high_yield_index_score: 71.45 },
  "miami":        { state: "FL", total_permits: 1124, total_active_permits: 1002, total_annual_kwh: 15231600, total_annual_savings_usd: 1981108,   total_co2_offset_metric_tons: 3198.64, avg_system_size_kw: 8.6, high_yield_index_score: 68.92 },
  "albuquerque":  { state: "NM", total_permits: 743,  total_active_permits: 671,  total_annual_kwh: 12543700, total_annual_savings_usd: 1630681,   total_co2_offset_metric_tons: 2634.18, avg_system_size_kw: 10.5, high_yield_index_score: 66.14 },
  "tucson":       { state: "AZ", total_permits: 891,  total_active_permits: 778,  total_annual_kwh: 9876300,  total_annual_savings_usd: 1580208,   total_co2_offset_metric_tons: 2074.02, avg_system_size_kw: 7.1, high_yield_index_score: 58.37 },
}

export async function GET(_req: Request, { params }: { params: Promise<{ city: string }> }) {
  const { city: rawCity } = await params
  const city = decodeURIComponent(rawCity)
  try {
    const res = await fetch(`${BACKEND}/api/city/${encodeURIComponent(city)}/summary`, { cache: "no-store" })
    if (res.ok) return NextResponse.json(await res.json())
  } catch {}

  const data = CITY_DATA[city.toLowerCase()]
  if (!data) return NextResponse.json({ detail: `City '${city}' not found.` }, { status: 404 })

  return NextResponse.json({
    city,
    ...data,
    last_updated: "2026-04-19T02:00:00Z",
  })
}
