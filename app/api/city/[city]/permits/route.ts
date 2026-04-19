import { NextResponse } from "next/server"

const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8000"

// City metadata for generating synthetic permits
const CITY_META: Record<string, { state: string; lat: number; lng: number; rate: number; active: number; zips: string[] }> = {
  "san diego":   { state: "CA", lat: 32.7157, lng: -117.1611, rate: 0.22, active: 1089, zips: ["92101","92103","92115","92037","92126","92107","92120","92122","92131","92108"] },
  "los angeles": { state: "CA", lat: 34.0522, lng: -118.2437, rate: 0.23, active: 2604, zips: ["90001","90012","90024","90025","90034","90045","90064","90210","90277","90403"] },
  "phoenix":     { state: "AZ", lat: 33.4484, lng: -112.074,  rate: 0.12, active: 2789, zips: ["85001","85003","85004","85006","85007","85008","85009","85012","85013","85014"] },
  "las vegas":   { state: "NV", lat: 36.1699, lng: -115.1398, rate: 0.11, active: 1712, zips: ["89101","89102","89103","89104","89106","89108","89109","89117","89119","89128"] },
  "austin":      { state: "TX", lat: 30.2672, lng: -97.7431,  rate: 0.11, active: 1388, zips: ["78701","78702","78703","78704","78705","78721","78731","78741","78745","78748"] },
  "denver":      { state: "CO", lat: 39.7392, lng: -104.9903, rate: 0.14, active: 1067, zips: ["80201","80202","80203","80204","80205","80206","80207","80208","80209","80210"] },
  "sacramento":  { state: "CA", lat: 38.5816, lng: -121.4944, rate: 0.22, active: 891,  zips: ["95814","95815","95816","95817","95818","95819","95820","95821","95822","95823"] },
  "miami":       { state: "FL", lat: 25.7617, lng: -80.1918,  rate: 0.13, active: 1002, zips: ["33101","33125","33126","33127","33128","33129","33130","33131","33132","33133"] },
  "albuquerque": { state: "NM", lat: 35.0844, lng: -106.6504, rate: 0.13, active: 671,  zips: ["87101","87102","87103","87104","87105","87106","87107","87108","87109","87110"] },
  "tucson":      { state: "AZ", lat: 32.2226, lng: -110.9747, rate: 0.12, active: 778,  zips: ["85701","85702","85703","85704","85705","85706","85707","85708","85709","85710"] },
}

function syntheticPermits(city: string, meta: typeof CITY_META[string], n: number) {
  const permits = []
  const base = new Date("2026-04-01")
  for (let i = 0; i < n; i++) {
    const daysBack = Math.round(Math.sin(i * 0.7) * 90 + 90)
    const installDate = new Date(base.getTime() - daysBack * 86400000)
    const sizeKw = +(6 + Math.sin(i * 1.3) * 4 + Math.sin(i * 0.4) * 2).toFixed(1)
    const sunHours = 5.5
    const annualKwh = +(sizeKw * sunHours * 365 * 0.85).toFixed(1)
    const savings = +(annualKwh * meta.rate).toFixed(2)
    const co2 = +(annualKwh * 0.000211).toFixed(2)
    const zipIdx = i % meta.zips.length
    const latOff = Math.sin(i * 0.9) * 0.05
    const lngOff = Math.sin(i * 1.1) * 0.05
    permits.push({
      permit_id: `ZP-${city.slice(0, 2).toUpperCase()}-${100000 + i}`,
      zip_code: meta.zips[zipIdx],
      city,
      state: meta.state,
      system_size_kw: sizeKw,
      install_date: installDate.toISOString().slice(0, 10),
      status: i % 7 === 0 ? "EXPIRED" : "VALID",
      latitude: +(meta.lat + latOff).toFixed(4),
      longitude: +(meta.lng + lngOff).toFixed(4),
      ac_annual_kwh: annualKwh,
      electricity_rate_per_kwh: meta.rate,
      annual_savings_usd: savings,
      co2_offset_metric_tons: co2,
      enriched_at: "2026-04-19T02:00:00Z",
    })
  }
  return permits
}

export async function GET(req: Request, { params }: { params: Promise<{ city: string }> }) {
  const { city: rawCity } = await params
  const city = decodeURIComponent(rawCity)
  const { searchParams } = new URL(req.url)
  const qs = searchParams.toString()

  try {
    const res = await fetch(`${BACKEND}/api/city/${encodeURIComponent(city)}/permits${qs ? `?${qs}` : ""}`, { cache: "no-store" })
    if (res.ok) return NextResponse.json(await res.json())
  } catch {}

  const meta = CITY_META[city.toLowerCase()]
  if (!meta) return NextResponse.json({ detail: `City '${city}' not found.` }, { status: 404 })

  const limit = Math.min(parseInt(searchParams.get("limit") ?? "100"), 1000)
  const offset = parseInt(searchParams.get("offset") ?? "0")
  const total = meta.active
  const permits = syntheticPermits(city, meta, Math.min(limit, total))

  return NextResponse.json({ city, total, limit, offset, permits })
}
