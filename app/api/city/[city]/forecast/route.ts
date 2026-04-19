import { NextResponse } from "next/server"

const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8000"

const CITY_PARAMS: Record<string, { state: string; velocity: number; avg_kw: number; rate: number }> = {
  "san diego":   { state: "CA", velocity: 18.2,  avg_kw: 9.2,  rate: 0.22 },
  "los angeles": { state: "CA", velocity: 43.4,  avg_kw: 8.7,  rate: 0.23 },
  "phoenix":     { state: "AZ", velocity: 46.5,  avg_kw: 10.6, rate: 0.12 },
  "las vegas":   { state: "NV", velocity: 28.5,  avg_kw: 11.1, rate: 0.11 },
  "austin":      { state: "TX", velocity: 23.1,  avg_kw: 9.0,  rate: 0.11 },
  "denver":      { state: "CO", velocity: 17.8,  avg_kw: 9.4,  rate: 0.14 },
  "sacramento":  { state: "CA", velocity: 14.9,  avg_kw: 9.4,  rate: 0.22 },
  "miami":       { state: "FL", velocity: 16.7,  avg_kw: 8.6,  rate: 0.13 },
  "albuquerque": { state: "NM", velocity: 11.2,  avg_kw: 10.5, rate: 0.13 },
  "tucson":      { state: "AZ", velocity: 13.0,  avg_kw: 7.1,  rate: 0.12 },
}

const SUN_HOURS = 5.5
const DAYS_PER_MONTH = 30.4
const CO2_PER_KWH = 0.000211

function buildMonths(): string[] {
  const months = []
  const now = new Date()
  for (let i = 1; i <= 12; i++) {
    const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1))
    months.push(d.toLocaleDateString("en-US", { month: "short", year: "numeric", timeZone: "UTC" }))
  }
  return months
}

export async function GET(_req: Request, { params }: { params: Promise<{ city: string }> }) {
  const { city: rawCity } = await params
  const city = decodeURIComponent(rawCity)
  try {
    const res = await fetch(`${BACKEND}/api/city/${encodeURIComponent(city)}/forecast`, { cache: "no-store" })
    if (res.ok) return NextResponse.json(await res.json())
  } catch {}

  const p = CITY_PARAMS[city.toLowerCase()]
  if (!p) return NextResponse.json({ detail: `No forecast data for '${city}'.` }, { status: 404 })

  const months = buildMonths()
  const monthlyKwh = p.velocity * p.avg_kw * SUN_HOURS * DAYS_PER_MONTH * 0.85
  const monthlyArr = months.map(() => +monthlyKwh.toFixed(2))

  return NextResponse.json({
    city,
    state: p.state,
    velocity_permits_per_month: p.velocity,
    avg_system_size_kw: p.avg_kw,
    months,
    monthly_projected_kwh: monthlyArr,
    monthly_projected_savings_usd: monthlyArr.map((k) => +(k * p.rate).toFixed(2)),
    monthly_projected_co2_metric_tons: monthlyArr.map((k) => +(k * CO2_PER_KWH).toFixed(4)),
  })
}
