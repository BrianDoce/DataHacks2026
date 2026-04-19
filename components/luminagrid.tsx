"use client"

import { useState, useMemo, useEffect } from "react"
import {
  Home,
  BarChart3,
  Award,
  TrendingUp,
  Search,
  Sun,
  Zap,
  DollarSign,
  Leaf,
  MapPin,
  ChevronUp,
  ChevronDown,
  Info,
  ArrowUpRight,
  ArrowDownRight,
  RefreshCw,
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Slider } from "@/components/ui/slider"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  Legend,
  Area,
  AreaChart,
  ComposedChart,
  ReferenceLine,
} from "recharts"
import {
  fetchLeaderboard,
  fetchCitySummary,
  fetchCityPermits,
  fetchCityForecast,
} from "@/lib/api"
import type { CitySummary, Permit, CityForecast, LeaderboardEntry } from "@/lib/api"

// ============================================================================
// STATIC / SEED DATA
// ============================================================================

const SEED_CITIES = [
  { name: "Phoenix", state: "AZ", lat: 33.4484, lng: -112.074 },
  { name: "Los Angeles", state: "CA", lat: 34.0522, lng: -118.2437 },
  { name: "San Diego", state: "CA", lat: 32.7157, lng: -117.1611 },
  { name: "Austin", state: "TX", lat: 30.2672, lng: -97.7431 },
  { name: "Denver", state: "CO", lat: 39.7392, lng: -104.9903 },
  { name: "Miami", state: "FL", lat: 25.7617, lng: -80.1918 },
  { name: "Las Vegas", state: "NV", lat: 36.1699, lng: -115.1398 },
  { name: "Sacramento", state: "CA", lat: 38.5816, lng: -121.4944 },
  { name: "Albuquerque", state: "NM", lat: 35.0844, lng: -106.6504 },
  { name: "Tucson", state: "AZ", lat: 32.2226, lng: -110.9747 },
]

type City = typeof SEED_CITIES[0]

// Static data used by the Forecast tab (local simulator — no API needed)
const FORECAST_MONTHLY_BASELINE = [
  { month: "Jan", kwh: 720 },
  { month: "Feb", kwh: 810 },
  { month: "Mar", kwh: 1050 },
  { month: "Apr", kwh: 1180 },
  { month: "May", kwh: 1320 },
  { month: "Jun", kwh: 1410 },
  { month: "Jul", kwh: 1380 },
  { month: "Aug", kwh: 1290 },
  { month: "Sep", kwh: 1140 },
  { month: "Oct", kwh: 960 },
  { month: "Nov", kwh: 780 },
  { month: "Dec", kwh: 690 },
]

// Mock solar 30-day output — deterministic (no Math.random) to avoid SSR hydration mismatch
const MOCK_SOLAR_30DAY = Array.from({ length: 60 }, (_, i) => {
  const isActual = i < 30
  const baseValue = 1200 + Math.sin(i / 5) * 200 + Math.sin(i * 7.3) * 50
  return {
    day: i - 29,
    actual: isActual ? Math.round(baseValue) : null,
    projected: !isActual ? Math.round(baseValue + Math.sin(i * 3.1) * 25) : null,
    date: new Date(Date.UTC(2026, 3, i - 29 + 18)).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" }),
  }
})

// ============================================================================
// HELPERS
// ============================================================================

function displayStatus(raw: string | null): string {
  if (!raw) return "Unknown"
  const map: Record<string, string> = {
    VALID: "Active",
    ACTIVE: "Active",
    PENDING: "Pending",
    EXPIRED: "Expired",
    INVALID: "Invalid",
  }
  return map[raw.toUpperCase()] ?? raw
}

// ============================================================================
// HELPER COMPONENTS
// ============================================================================

function LoadingSkeleton({ className }: { className?: string }) {
  return <Skeleton className={`bg-muted ${className}`} />
}

function KPICard({
  title,
  value,
  source,
  icon: Icon,
  bgColor,
}: {
  title: string
  value: string
  source: string
  icon: React.ElementType
  bgColor: string
}) {
  return (
    <Card className="border-border">
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-sm text-muted-foreground">{title}</p>
            <p className="text-2xl font-semibold text-foreground mt-1">{value}</p>
            <p className="text-xs text-muted-foreground mt-2">Source: {source}</p>
          </div>
          <div className={`p-3 rounded-lg ${bgColor}`}>
            <Icon className="h-5 w-5 text-foreground" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function SparklineChart({ data, color = "#B8E8C8" }: { data: number[]; color?: string }) {
  const chartData = data.map((value, index) => ({ index, value }))
  return (
    <ResponsiveContainer width="100%" height={40}>
      <AreaChart data={chartData}>
        <defs>
          <linearGradient id={`sparkline-${color.replace("#", "")}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.4} />
            <stop offset="100%" stopColor={color} stopOpacity={0.1} />
          </linearGradient>
        </defs>
        <Area
          type="monotone"
          dataKey="value"
          stroke={color}
          strokeWidth={2}
          fill={`url(#sparkline-${color.replace("#", "")})`}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    Active: "bg-[#B8E8C8] text-foreground",
    VALID: "bg-[#B8E8C8] text-foreground",
    Pending: "bg-[#F5E6C8] text-foreground",
    PENDING: "bg-[#F5E6C8] text-foreground",
    Expired: "bg-[#E8C8B8] text-foreground",
    EXPIRED: "bg-[#E8C8B8] text-foreground",
    Invalid: "bg-[#E8C8B8] text-foreground",
    INVALID: "bg-[#E8C8B8] text-foreground",
    "On Track": "bg-[#B8E8C8] text-foreground",
    Ahead: "bg-[#B8D4E8] text-foreground",
    "At Risk": "bg-[#E8C8B8] text-foreground",
  }
  return (
    <Badge variant="secondary" className={`${styles[status] || "bg-muted"} font-medium`}>
      {displayStatus(status)}
    </Badge>
  )
}

function RankBadge({ rank }: { rank: number }) {
  if (rank > 3) return <span className="text-muted-foreground font-medium">{rank}</span>
  return (
    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-[#F5E6C8] text-foreground font-bold text-sm">
      {rank}
    </span>
  )
}

// ============================================================================
// PAGE COMPONENTS
// ============================================================================

function HomeDashboard({
  selectedCity,
  setSelectedCity,
}: {
  selectedCity: City
  setSelectedCity: (city: City) => void
}) {
  const [searchQuery, setSearchQuery] = useState("")
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [summary, setSummary] = useState<CitySummary | null>(null)
  const [permits, setPermits] = useState<Permit[]>([])
  const [dataLoading, setDataLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setDataLoading(true)
    setSummary(null)
    setPermits([])
    setError(null)
    Promise.all([
      fetchCitySummary(selectedCity.name).catch((e) => { console.warn("summary:", e.message); return null }),
      fetchCityPermits(selectedCity.name, { limit: 1000 }).catch((e) => { console.warn("permits:", e.message); return null }),
    ])
      .then(([sum, page]) => {
        setSummary(sum)
        setPermits(page?.permits ?? [])
        if (!sum && !page) setError(`No data found for ${selectedCity.name}`)
      })
      .finally(() => setDataLoading(false))
  }, [selectedCity.name])

  const filteredCities = SEED_CITIES.filter(
    (city) =>
      city.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      city.state.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const zipCodeSavings = useMemo(() => {
    const map: Record<string, number> = {}
    for (const p of permits) {
      if (p.zip_code && p.annual_savings_usd) {
        map[p.zip_code] = (map[p.zip_code] || 0) + p.annual_savings_usd
      }
    }
    return Object.entries(map)
      .map(([zip, savings]) => ({ zip, savings }))
      .sort((a, b) => b.savings - a.savings)
      .slice(0, 10)
  }, [permits])

  const permitStatus = useMemo(() => {
    if (!summary) return []
    const active = summary.total_active_permits
    const inactive = summary.total_permits - active
    const total = summary.total_permits || 1
    return [
      { status: "Active", count: active, percentage: Math.round((active / total) * 100) },
      { status: "Inactive", count: inactive, percentage: Math.round((inactive / total) * 100) },
    ]
  }, [summary])

  const zipMapData = useMemo(() => {
    const map: Record<string, { savings: number; lat: number; lng: number }> = {}
    for (const p of permits) {
      if (!p.zip_code || !p.annual_savings_usd) continue
      if (!map[p.zip_code]) {
        map[p.zip_code] = { savings: 0, lat: p.latitude || 0, lng: p.longitude || 0 }
      }
      map[p.zip_code].savings += p.annual_savings_usd
      if (p.latitude && !map[p.zip_code].lat) map[p.zip_code].lat = p.latitude
      if (p.longitude && !map[p.zip_code].lng) map[p.zip_code].lng = p.longitude
    }
    const sorted = Object.entries(map)
      .map(([zip, d]) => ({ zip, ...d, intensity: 0 }))
      .sort((a, b) => b.savings - a.savings)
      .slice(0, 10)
    const max = sorted[0]?.savings || 1
    return sorted.map((z) => ({ ...z, intensity: z.savings / max }))
  }, [permits])

  const pieColors = ["#B8E8C8", "#E8C8B8"]

  return (
    <div className="space-y-6">
      {/* City Search */}
      <div className="relative">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search for a city..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value)
              setIsSearchOpen(true)
            }}
            onFocus={() => setIsSearchOpen(true)}
            className="pl-10 bg-card border-border"
          />
        </div>
        {isSearchOpen && searchQuery && (
          <Card className="absolute z-50 w-full mt-1 border-border">
            <ScrollArea className="max-h-60">
              {filteredCities.map((city) => (
                <button
                  key={`${city.name}-${city.state}`}
                  className="w-full px-4 py-2 text-left hover:bg-accent flex items-center gap-2"
                  onClick={() => {
                    setSelectedCity(city)
                    setSearchQuery("")
                    setIsSearchOpen(false)
                  }}
                >
                  <MapPin className="h-4 w-4 text-muted-foreground" />
                  <span className="text-foreground">{city.name}</span>
                  <span className="text-muted-foreground">{city.state}</span>
                </button>
              ))}
            </ScrollArea>
          </Card>
        )}
      </div>

      {/* Selected City Header */}
      <div>
        <h2 className="text-2xl font-semibold text-foreground">
          {selectedCity.name}, {selectedCity.state}
        </h2>
        <p className="text-muted-foreground" suppressHydrationWarning>{new Date().toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" })}</p>
      </div>

      {error && (
        <div className="rounded-lg border border-[#E8C8B8] bg-[#E8C8B8]/20 px-4 py-3 text-sm text-foreground">
          {error}
        </div>
      )}

      {/* KPI Cards */}
      {dataLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => <LoadingSkeleton key={i} className="h-24 rounded-lg" />)}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <KPICard
            title="Total Active Permits"
            value={summary ? summary.total_active_permits.toLocaleString() : "—"}
            source="ZenPower"
            icon={Sun}
            bgColor="bg-[#F5E6C8]"
          />
          <KPICard
            title="Total Annual kWh Generated"
            value={summary ? `${(summary.total_annual_kwh / 1_000_000).toFixed(1)}M` : "—"}
            source="NREL PVWatts"
            icon={Zap}
            bgColor="bg-[#B8D4E8]"
          />
          <KPICard
            title="Total Annual Community Savings"
            value={summary ? `$${(summary.total_annual_savings_usd / 1_000_000).toFixed(2)}M` : "—"}
            source="EIA"
            icon={DollarSign}
            bgColor="bg-[#B8E8C8]"
          />
          <KPICard
            title="Total CO₂ Offset"
            value={summary ? `${(summary.total_co2_offset_metric_tons / 1000).toFixed(1)}K tons/yr` : "—"}
            source="EIA Carbon Index"
            icon={Leaf}
            bgColor="bg-[#B8E8C8]"
          />
        </div>
      )}

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 10 ZIP Codes by Savings */}
        <Card className="border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base font-medium text-foreground">
              Top ZIP Codes by Annual Savings
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dataLoading ? (
              <LoadingSkeleton className="h-[300px] rounded" />
            ) : zipCodeSavings.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={zipCodeSavings} layout="vertical" margin={{ left: 50, right: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                  <XAxis
                    type="number"
                    tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
                    stroke="#6B7280"
                    fontSize={12}
                  />
                  <YAxis type="category" dataKey="zip" stroke="#6B7280" fontSize={12} />
                  <Bar dataKey="savings" fill="#B8E8C8" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-muted-foreground text-sm py-8 text-center">No permit data available</p>
            )}
          </CardContent>
        </Card>

        {/* Permit Status Breakdown */}
        <Card className="border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base font-medium text-foreground">
              Permit Status Breakdown
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dataLoading ? (
              <LoadingSkeleton className="h-[300px] rounded" />
            ) : permitStatus.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={permitStatus}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={100}
                    paddingAngle={2}
                    dataKey="count"
                    label={({ status, percentage }) => `${status}: ${percentage}%`}
                    labelLine={false}
                  >
                    {permitStatus.map((entry, index) => (
                      <Cell key={entry.status} fill={pieColors[index % pieColors.length]} />
                    ))}
                  </Pie>
                  <Legend
                    verticalAlign="bottom"
                    height={36}
                    formatter={(value) => <span className="text-foreground text-sm">{value}</span>}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-muted-foreground text-sm py-8 text-center">No data available</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* 30-Day Solar Output Prediction (weather data — kept as estimate) */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            30-Day Solar Output Estimate
          </CardTitle>
          <p className="text-xs text-muted-foreground">Based on historical irradiance patterns</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={280}>
            <ComposedChart data={MOCK_SOLAR_30DAY} margin={{ left: 10, right: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                dataKey="date"
                stroke="#6B7280"
                fontSize={11}
                tickFormatter={(v, i) => (i % 5 === 0 ? v : "")}
              />
              <YAxis stroke="#6B7280" fontSize={12} tickFormatter={(v) => `${v}`} />
              <ReferenceLine x="Apr 18" stroke="#6B7280" strokeDasharray="3 3" label={{ value: "Today", fill: "#6B7280", fontSize: 11 }} />
              <Line
                type="monotone"
                dataKey="actual"
                stroke="#B8D4E8"
                strokeWidth={2}
                dot={false}
                name="Actual"
                connectNulls={false}
              />
              <Line
                type="monotone"
                dataKey="projected"
                stroke="#B8E8C8"
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                name="Projected"
                connectNulls={false}
              />
              <Legend />
            </ComposedChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Choropleth Map */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            ZIP-Level Annual Savings Intensity
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="relative bg-[#B8D4E8]/20 rounded-lg overflow-hidden" style={{ height: 400 }}>
            <svg viewBox="0 0 400 300" className="w-full h-full">
              <defs>
                <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
                  <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
                </pattern>
              </defs>
              <rect width="400" height="300" fill="url(#grid)" />
              {(zipMapData.length > 0 ? zipMapData : []).map((zip, i) => {
                const x = 50 + (i % 5) * 65
                const y = 50 + Math.floor(i / 5) * 100
                const opacity = 0.3 + zip.intensity * 0.7
                return (
                  <g key={zip.zip}>
                    <rect x={x} y={y} width={55} height={80} rx={4} fill="#B8E8C8" fillOpacity={opacity} stroke="#1F2937" strokeWidth={1} />
                    <text x={x + 27} y={y + 40} textAnchor="middle" fontSize={11} fill="#1F2937" fontWeight="500">{zip.zip}</text>
                    <text x={x + 27} y={y + 55} textAnchor="middle" fontSize={9} fill="#6B7280">${(zip.savings / 1000).toFixed(0)}K</text>
                  </g>
                )
              })}
            </svg>
            <div className="absolute bottom-4 right-4 bg-card p-3 rounded-lg border border-border">
              <p className="text-xs text-muted-foreground mb-2">Savings Intensity</p>
              <div className="flex items-center gap-1">
                <div className="w-20 h-3 rounded" style={{ background: "linear-gradient(to right, #B8E8C830, #B8E8C8)" }} />
              </div>
              <div className="flex justify-between text-xs text-muted-foreground mt-1">
                <span>Low</span>
                <span>High</span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function SolarAnalytics({
  selectedCity,
  setSelectedCity,
}: {
  selectedCity: City
  setSelectedCity: (city: City) => void
}) {
  const [searchQuery, setSearchQuery] = useState("")
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [electricityRate, setElectricityRate] = useState([0.12])
  const [newPermitsPerMonth, setNewPermitsPerMonth] = useState([50])
  const [zipFilter, setZipFilter] = useState("")
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [sortField, setSortField] = useState<string>("annual_savings_usd")
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc")

  const [permits, setPermits] = useState<Permit[]>([])
  const [forecast, setForecast] = useState<CityForecast | null>(null)
  const [dataLoading, setDataLoading] = useState(true)

  useEffect(() => {
    setDataLoading(true)
    setPermits([])
    setForecast(null)
    Promise.all([
      fetchCityPermits(selectedCity.name, { limit: 1000 }).catch((e) => { console.warn("permits:", e.message); return null }),
      fetchCityForecast(selectedCity.name).catch((e) => { console.warn("forecast:", e.message); return null }),
    ])
      .then(([page, fc]) => {
        setPermits(page?.permits ?? [])
        setForecast(fc)
      })
      .finally(() => setDataLoading(false))
  }, [selectedCity.name])

  const filteredCities = SEED_CITIES.filter(
    (city) =>
      city.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      city.state.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const scenarioMetrics = useMemo(() => {
    const rate = electricityRate[0]
    const permsPerMonth = newPermitsPerMonth[0]
    const avgSystemSize = 8.5
    const avgSunHours = 5.5
    const daysPerYear = 365
    const annualKwh = permsPerMonth * 12 * avgSystemSize * avgSunHours * daysPerYear * 0.85
    const annualSavings = annualKwh * rate
    const co2Offset = annualKwh * 0.0007
    const systemCost = permsPerMonth * 12 * avgSystemSize * 2500
    return {
      annualSavings,
      annualKwh,
      co2Offset,
      paybackYears: systemCost / annualSavings,
    }
  }, [electricityRate, newPermitsPerMonth])

  const trendChartData = useMemo(() => {
    if (!forecast) return []
    return forecast.months.map((month, i) => ({
      month,
      capacity: Math.round(forecast.monthly_projected_kwh[i] / 1000),
      savings: Math.round(forecast.monthly_projected_savings_usd[i]),
      co2: Math.round(forecast.monthly_projected_co2_metric_tons[i] * 1000) / 10,
    }))
  }, [forecast])

  const filteredPermits = useMemo(() => {
    return permits
      .filter((p) => {
        if (zipFilter && !(p.zip_code || "").includes(zipFilter)) return false
        if (statusFilter) {
          const raw = (p.status || "").toUpperCase()
          const target = statusFilter.toUpperCase()
          if (raw !== target && displayStatus(raw).toUpperCase() !== target) return false
        }
        return true
      })
      .sort((a, b) => {
        const aVal = a[sortField as keyof Permit] as number | string | null
        const bVal = b[sortField as keyof Permit] as number | string | null
        const av = aVal ?? (typeof aVal === "number" ? 0 : "")
        const bv = bVal ?? (typeof bVal === "number" ? 0 : "")
        if (typeof av === "number" && typeof bv === "number") {
          return sortDirection === "asc" ? av - bv : bv - av
        }
        return sortDirection === "asc"
          ? String(av).localeCompare(String(bv))
          : String(bv).localeCompare(String(av))
      })
  }, [permits, zipFilter, statusFilter, sortField, sortDirection])

  const handleSort = (field: string) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc")
    } else {
      setSortField(field)
      setSortDirection("desc")
    }
  }

  return (
    <div className="space-y-6">
      {/* City Search */}
      <div className="relative">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search for a city..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value)
              setIsSearchOpen(true)
            }}
            onFocus={() => setIsSearchOpen(true)}
            className="pl-10 bg-card border-border"
          />
        </div>
        {isSearchOpen && searchQuery && (
          <Card className="absolute z-50 w-full mt-1 border-border">
            <ScrollArea className="max-h-60">
              {filteredCities.map((city) => (
                <button
                  key={`${city.name}-${city.state}`}
                  className="w-full px-4 py-2 text-left hover:bg-accent flex items-center gap-2"
                  onClick={() => {
                    setSelectedCity(city)
                    setSearchQuery("")
                    setIsSearchOpen(false)
                  }}
                >
                  <MapPin className="h-4 w-4 text-muted-foreground" />
                  <span className="text-foreground">{city.name}</span>
                  <span className="text-muted-foreground">{city.state}</span>
                </button>
              ))}
            </ScrollArea>
          </Card>
        )}
      </div>

      {/* 12-Month Forward Forecast Chart */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            12-Month Forward Forecast — {selectedCity.name}, {selectedCity.state}
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            {forecast
              ? `Velocity: ${forecast.velocity_permits_per_month.toFixed(1)} permits/month · Avg size: ${forecast.avg_system_size_kw} kW`
              : "Loading…"}
          </p>
        </CardHeader>
        <CardContent>
          {dataLoading ? (
            <LoadingSkeleton className="h-[300px] rounded" />
          ) : trendChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={trendChartData} margin={{ left: 10, right: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis dataKey="month" stroke="#6B7280" fontSize={11} tickFormatter={(v, i) => (i % 2 === 0 ? v : "")} />
                <YAxis yAxisId="left" stroke="#6B7280" fontSize={12} tickFormatter={(v) => `${v}K kWh`} />
                <YAxis yAxisId="right" orientation="right" stroke="#6B7280" fontSize={12} tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`} />
                <Line yAxisId="left" type="monotone" dataKey="capacity" stroke="#F5E6C8" strokeWidth={2} dot={false} name="Projected kWh (K)" />
                <Line yAxisId="right" type="monotone" dataKey="savings" stroke="#B8E8C8" strokeWidth={2} dot={false} name="Projected Savings ($)" />
                <Line yAxisId="left" type="monotone" dataKey="co2" stroke="#B8D4E8" strokeWidth={2} dot={false} name="CO₂ (tons×10)" />
                <Legend />
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-muted-foreground text-sm py-8 text-center">No forecast data available for this city</p>
          )}
        </CardContent>
      </Card>

      {/* Scenario Modeler */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">Scenario Modeler</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <label className="text-sm text-muted-foreground">
                  Assumed Electricity Rate: ${electricityRate[0].toFixed(2)}/kWh
                </label>
                <Slider value={electricityRate} onValueChange={setElectricityRate} min={0.1} max={0.5} step={0.01} className="mt-2" />
                <div className="flex justify-between text-xs text-muted-foreground mt-1">
                  <span>$0.10</span>
                  <span>$0.50</span>
                </div>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">
                  Projected New Permits/Month: {newPermitsPerMonth[0]}
                </label>
                <Slider value={newPermitsPerMonth} onValueChange={setNewPermitsPerMonth} min={0} max={200} step={5} className="mt-2" />
                <div className="flex justify-between text-xs text-muted-foreground mt-1">
                  <span>0</span>
                  <span>200</span>
                </div>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground">Projected Annual Savings</p>
                <p className="text-xl font-semibold text-foreground">
                  ${(scenarioMetrics.annualSavings / 1_000_000).toFixed(2)}M
                </p>
              </div>
              <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground">Projected kWh Generated</p>
                <p className="text-xl font-semibold text-foreground">
                  {(scenarioMetrics.annualKwh / 1_000_000).toFixed(1)}M
                </p>
              </div>
              <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground">CO₂ Offset</p>
                <p className="text-xl font-semibold text-foreground">
                  {(scenarioMetrics.co2Offset / 1000).toFixed(1)}K tons
                </p>
              </div>
              <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground">Payback Period</p>
                <p className="text-xl font-semibold text-foreground">
                  {scenarioMetrics.paybackYears.toFixed(1)} years
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Permit Explorer Table */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <CardTitle className="text-base font-medium text-foreground">Permit Explorer</CardTitle>
              <p className="text-xs text-muted-foreground">
                {dataLoading ? "Loading…" : `${filteredPermits.length} of ${permits.length} permits`}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Input
                placeholder="Search ZIP..."
                value={zipFilter}
                onChange={(e) => setZipFilter(e.target.value)}
                className="w-32 bg-card border-border"
              />
              <div className="flex gap-1">
                {["VALID", "PENDING", "EXPIRED"].map((status) => (
                  <Button
                    key={status}
                    variant={statusFilter === status ? "default" : "outline"}
                    size="sm"
                    onClick={() => setStatusFilter(statusFilter === status ? null : status)}
                    className={statusFilter === status ? "bg-[#F5E6C8] text-foreground hover:bg-[#F5E6C8]/80" : ""}
                  >
                    {displayStatus(status)}
                  </Button>
                ))}
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {dataLoading ? (
            <LoadingSkeleton className="h-[400px] rounded" />
          ) : (
            <ScrollArea className="h-[400px]">
              <Table>
                <TableHeader>
                  <TableRow className="border-border">
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("zip_code")}>
                      ZIP Code {sortField === "zip_code" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("system_size_kw")}>
                      System Size (kW) {sortField === "system_size_kw" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("install_date")}>
                      Install Date {sortField === "install_date" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("ac_annual_kwh")}>
                      Annual kWh {sortField === "ac_annual_kwh" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("annual_savings_usd")}>
                      Annual Savings {sortField === "annual_savings_usd" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="cursor-pointer hover:bg-accent text-foreground" onClick={() => handleSort("co2_offset_metric_tons")}>
                      CO₂ Offset (tons) {sortField === "co2_offset_metric_tons" && (sortDirection === "asc" ? "↑" : "↓")}
                    </TableHead>
                    <TableHead className="text-foreground">Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredPermits.map((permit) => (
                    <TableRow key={permit.permit_id} className="border-border">
                      <TableCell className="font-medium text-foreground">{permit.zip_code ?? "—"}</TableCell>
                      <TableCell className="text-foreground">{permit.system_size_kw?.toFixed(1) ?? "—"}</TableCell>
                      <TableCell className="text-foreground">{permit.install_date ?? "—"}</TableCell>
                      <TableCell className="text-foreground">{permit.ac_annual_kwh?.toLocaleString() ?? "—"}</TableCell>
                      <TableCell className="text-foreground">
                        {permit.annual_savings_usd != null ? `$${Math.round(permit.annual_savings_usd).toLocaleString()}` : "—"}
                      </TableCell>
                      <TableCell className="text-foreground">{permit.co2_offset_metric_tons?.toFixed(1) ?? "—"}</TableCell>
                      <TableCell>
                        <StatusBadge status={permit.status ?? "Unknown"} />
                      </TableCell>
                    </TableRow>
                  ))}
                  {filteredPermits.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                        No permits found
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </ScrollArea>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function CityScorecard() {
  const [stateFilter, setStateFilter] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [sortBy, setSortBy] = useState("rank")
  const [leaderboard, setLeaderboard] = useState<LeaderboardEntry[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetchLeaderboard(stateFilter ?? undefined, 100)
      .then((res) => setLeaderboard(res.entries))
      .catch((e) => { console.warn("leaderboard:", e.message); setLeaderboard([]) })
      .finally(() => setLoading(false))
  }, [stateFilter])

  const scorecardData = useMemo(() => {
    return leaderboard.map((entry) => {
      const installedKw = Math.round(entry.total_active_permits * entry.avg_system_size_kw)
      const hyi = Math.round(entry.high_yield_index_score)
      const status = hyi >= 85 ? "Ahead" : hyi >= 65 ? "On Track" : "At Risk"
      return {
        rank: entry.rank,
        city: entry.city,
        state: entry.state,
        installedKw,
        co2: entry.total_co2_offset_metric_tons,
        target: 85,
        progress: Math.min(hyi, 100),
        status,
        hyiScore: hyi,
        totalPermits: entry.total_permits,
        activePermits: entry.total_active_permits,
      }
    })
  }, [leaderboard])

  const states = useMemo(() => {
    const all = [...new Set(leaderboard.map((e) => e.state))].sort()
    return all
  }, [leaderboard])

  const filteredData = useMemo(() => {
    return scorecardData
      .filter((city) => {
        if (statusFilter && city.status !== statusFilter) return false
        return true
      })
      .sort((a, b) => {
        switch (sortBy) {
          case "capacity": return b.installedKw - a.installedKw
          case "hyiScore": return b.hyiScore - a.hyiScore
          case "progress": return b.progress - a.progress
          default: return a.rank - b.rank
        }
      })
  }, [scorecardData, statusFilter, sortBy])

  return (
    <div className="space-y-6">
      {/* Filters */}
      <div className="flex flex-wrap gap-4">
        <Select value={stateFilter || "all"} onValueChange={(v) => setStateFilter(v === "all" ? null : v)}>
          <SelectTrigger className="w-40 bg-card border-border">
            <SelectValue placeholder="All States" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All States</SelectItem>
            {states.map((state) => (
              <SelectItem key={state} value={state}>{state}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={statusFilter || "all"} onValueChange={(v) => setStatusFilter(v === "all" ? null : v)}>
          <SelectTrigger className="w-40 bg-card border-border">
            <SelectValue placeholder="All Statuses" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Statuses</SelectItem>
            <SelectItem value="On Track">On Track</SelectItem>
            <SelectItem value="Ahead">Ahead</SelectItem>
            <SelectItem value="At Risk">At Risk</SelectItem>
          </SelectContent>
        </Select>

        <Select value={sortBy} onValueChange={setSortBy}>
          <SelectTrigger className="w-40 bg-card border-border">
            <SelectValue placeholder="Sort By" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="rank">Rank</SelectItem>
            <SelectItem value="capacity">Installed kW</SelectItem>
            <SelectItem value="hyiScore">HYI Score</SelectItem>
            <SelectItem value="progress">Progress</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Scorecard Table */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">City Rankings</CardTitle>
          <p className="text-xs text-muted-foreground">
            {loading ? "Loading…" : `${filteredData.length} cities`}
          </p>
        </CardHeader>
        <CardContent>
          {loading ? (
            <LoadingSkeleton className="h-[400px] rounded" />
          ) : (
            <ScrollArea className="w-full">
              <div className="min-w-[900px]">
                <Table>
                  <TableHeader>
                    <TableRow className="border-border">
                      <TableHead className="text-foreground">Rank</TableHead>
                      <TableHead className="text-foreground">City</TableHead>
                      <TableHead className="text-foreground">State</TableHead>
                      <TableHead className="text-foreground">Total Permits</TableHead>
                      <TableHead className="text-foreground">Active Permits</TableHead>
                      <TableHead className="text-foreground">Est. Installed kW</TableHead>
                      <TableHead className="text-foreground">CO₂ Offset (tons/yr)</TableHead>
                      <TableHead className="text-foreground">Status</TableHead>
                      <TableHead className="text-foreground">
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger className="flex items-center gap-1">
                              HYI Score <Info className="h-3 w-3" />
                            </TooltipTrigger>
                            <TooltipContent className="max-w-xs">
                              <p className="text-sm">
                                High-Yield Index: composite score ranking cities by solar permit performance
                              </p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredData.map((city) => (
                      <TableRow key={city.city} className="border-border">
                        <TableCell><RankBadge rank={city.rank} /></TableCell>
                        <TableCell className="font-medium text-foreground">{city.city}</TableCell>
                        <TableCell className="text-foreground">{city.state}</TableCell>
                        <TableCell className="text-foreground">{city.totalPermits.toLocaleString()}</TableCell>
                        <TableCell className="text-foreground">{city.activePermits.toLocaleString()}</TableCell>
                        <TableCell className="text-foreground">{(city.installedKw / 1000).toFixed(0)}K</TableCell>
                        <TableCell className="text-foreground">{(city.co2 / 1000).toFixed(1)}K</TableCell>
                        <TableCell><StatusBadge status={city.status} /></TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Progress value={city.progress} className="w-16 h-2" />
                            <span className="text-sm text-foreground">{city.hyiScore}</span>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                    {filteredData.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={9} className="text-center text-muted-foreground py-8">
                          No cities found
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>
            </ScrollArea>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function Forecast() {
  const [lat, setLat] = useState("33.4484")
  const [lng, setLng] = useState("-112.0740")
  const [address, setAddress] = useState("")
  const [systemCapacity, setSystemCapacity] = useState("10")
  const [tiltAngle, setTiltAngle] = useState("20")
  const [azimuth, setAzimuth] = useState("180")
  const [arrayType, setArrayType] = useState("fixed-open-rack")
  const [moduleType, setModuleType] = useState("standard")
  const [losses, setLosses] = useState("14")
  const [systemCost, setSystemCost] = useState("25000")
  const [electricityRate, setElectricityRate] = useState("0.12")
  const [rateEscalation, setRateEscalation] = useState([3])
  const [showResults, setShowResults] = useState(false)
  const [pinPosition, setPinPosition] = useState({ x: 50, y: 50 })

  const simulationResults = useMemo(() => {
    const capacity = parseFloat(systemCapacity) || 10
    const avgSunHours = 5.8
    const systemEfficiency = (100 - parseFloat(losses)) / 100
    const daysPerYear = 365
    const annualKwh = capacity * avgSunHours * daysPerYear * systemEfficiency
    const rate = parseFloat(electricityRate) || 0.12
    const annualSavings = annualKwh * rate
    const co2Offset = annualKwh * 0.0007
    const cost = parseFloat(systemCost) || 25000
    return {
      annualKwh: Math.round(annualKwh),
      monthlyOutput: FORECAST_MONTHLY_BASELINE.map((m) => ({
        ...m,
        kwh: Math.round(m.kwh * (capacity / 10)),
      })),
      annualSavings: Math.round(annualSavings),
      co2Offset: Math.round(co2Offset * 10) / 10,
      paybackYears: Math.round((cost / annualSavings) * 10) / 10,
      solarResource: 5.8,
    }
  }, [systemCapacity, losses, electricityRate, systemCost])

  const financialProjections = useMemo(() => {
    const rate = parseFloat(electricityRate) || 0.12
    const escalation = rateEscalation[0] / 100
    const cost = parseFloat(systemCost) || 25000
    const capacity = parseFloat(systemCapacity) || 10
    const avgSunHours = 5.8
    const systemEfficiency = (100 - parseFloat(losses)) / 100
    const annualKwh = capacity * avgSunHours * 365 * systemEfficiency
    let cumulative = 0
    let breakEvenYear = 25
    const savingsData = Array.from({ length: 25 }, (_, i) => {
      const yearRate = rate * Math.pow(1 + escalation, i)
      cumulative += annualKwh * yearRate
      if (cumulative >= cost && breakEvenYear === 25) breakEvenYear = i + 1
      return { year: i + 1, savings: Math.round(cumulative) }
    })
    return {
      tenYearSavings: savingsData[9]?.savings || 0,
      twentyFiveYearSavings: savingsData[24]?.savings || 0,
      breakEvenYear,
      savingsData,
    }
  }, [electricityRate, rateEscalation, systemCost, systemCapacity, losses])

  const handleMapClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const x = ((e.clientX - rect.left) / rect.width) * 100
    const y = ((e.clientY - rect.top) / rect.height) * 100
    setPinPosition({ x, y })
    setLat((33.45 + (50 - y) * 0.01).toFixed(4))
    setLng((-112.07 + (x - 50) * 0.01).toFixed(4))
  }

  return (
    <div className="space-y-6">
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">Location Selection</CardTitle>
          <p className="text-xs text-muted-foreground">Click on the map or enter coordinates</p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10" />
            <Input
              placeholder="Search address..."
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              className="pl-10 bg-card border-border"
            />
          </div>
          <div
            className="relative bg-[#B8D4E8]/30 rounded-lg overflow-hidden cursor-crosshair"
            style={{ height: 300 }}
            onClick={handleMapClick}
          >
            <svg viewBox="0 0 400 300" className="w-full h-full">
              <defs>
                <pattern id="mapGrid" width="20" height="20" patternUnits="userSpaceOnUse">
                  <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
                </pattern>
              </defs>
              <rect width="400" height="300" fill="url(#mapGrid)" />
              <line x1="0" y1="150" x2="400" y2="150" stroke="#E5E7EB" strokeWidth="3" />
              <line x1="200" y1="0" x2="200" y2="300" stroke="#E5E7EB" strokeWidth="3" />
              <line x1="100" y1="0" x2="100" y2="300" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="300" y1="0" x2="300" y2="300" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="0" y1="75" x2="400" y2="75" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="0" y1="225" x2="400" y2="225" stroke="#E5E7EB" strokeWidth="1" />
            </svg>
            <div
              className="absolute transform -translate-x-1/2 -translate-y-full"
              style={{ left: `${pinPosition.x}%`, top: `${pinPosition.y}%` }}
            >
              <MapPin className="h-8 w-8 text-[#E8C8B8] fill-[#E8C8B8]" />
            </div>
            <div className="absolute bottom-2 left-2 bg-card px-2 py-1 rounded text-xs text-muted-foreground">
              Click to place pin
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm text-muted-foreground">Latitude</label>
              <Input value={lat} onChange={(e) => setLat(e.target.value)} className="bg-card border-border mt-1" />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Longitude</label>
              <Input value={lng} onChange={(e) => setLng(e.target.value)} className="bg-card border-border mt-1" />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">System Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div>
              <label className="text-sm text-muted-foreground">System Capacity (kW)</label>
              <Input type="number" value={systemCapacity} onChange={(e) => setSystemCapacity(e.target.value)} className="bg-card border-border mt-1" />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Panel Tilt (°)</label>
              <Input type="number" value={tiltAngle} onChange={(e) => setTiltAngle(e.target.value)} className="bg-card border-border mt-1" />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Azimuth (°)</label>
              <Input type="number" value={azimuth} onChange={(e) => setAzimuth(e.target.value)} className="bg-card border-border mt-1" />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Array Type</label>
              <Select value={arrayType} onValueChange={setArrayType}>
                <SelectTrigger className="bg-card border-border mt-1"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="fixed-open-rack">Fixed Open Rack</SelectItem>
                  <SelectItem value="fixed-roof-mount">Fixed Roof Mount</SelectItem>
                  <SelectItem value="one-axis-tracking">One-Axis Tracking</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Module Type</label>
              <Select value={moduleType} onValueChange={setModuleType}>
                <SelectTrigger className="bg-card border-border mt-1"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="standard">Standard</SelectItem>
                  <SelectItem value="premium">Premium</SelectItem>
                  <SelectItem value="thin-film">Thin Film</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm text-muted-foreground">System Losses (%)</label>
              <Input type="number" value={losses} onChange={(e) => setLosses(e.target.value)} className="bg-card border-border mt-1" />
            </div>
          </div>
          <Button onClick={() => setShowResults(true)} className="mt-4 bg-[#F5E6C8] text-foreground hover:bg-[#F5E6C8]/80">
            Run Simulation
          </Button>
        </CardContent>
      </Card>

      {showResults && (
        <>
          <Card className="border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium text-foreground">Simulation Output</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Annual AC Energy</p>
                  <p className="text-xl font-semibold text-foreground">{simulationResults.annualKwh.toLocaleString()} kWh</p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Annual Savings</p>
                  <p className="text-xl font-semibold text-foreground">${simulationResults.annualSavings.toLocaleString()}</p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">CO₂ Offset</p>
                  <p className="text-xl font-semibold text-foreground">{simulationResults.co2Offset} tons/yr</p>
                </div>
                <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Payback Period</p>
                  <p className="text-xl font-semibold text-foreground">{simulationResults.paybackYears} years</p>
                </div>
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Solar Resource</p>
                  <p className="text-xl font-semibold text-foreground">{simulationResults.solarResource} hrs/day</p>
                </div>
                <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Location</p>
                  <p className="text-lg font-semibold text-foreground">{lat}°, {lng}°</p>
                </div>
              </div>
              <div>
                <h4 className="text-sm font-medium text-foreground mb-3">Monthly Energy Output</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={simulationResults.monthlyOutput}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                    <XAxis dataKey="month" stroke="#6B7280" fontSize={12} />
                    <YAxis stroke="#6B7280" fontSize={12} />
                    <Bar dataKey="kwh" fill="#F5E6C8" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card className="border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium text-foreground">What-If Financial Analysis</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="text-sm text-muted-foreground">Assumed System Cost ($)</label>
                  <Input type="number" value={systemCost} onChange={(e) => setSystemCost(e.target.value)} className="bg-card border-border mt-1" />
                </div>
                <div>
                  <label className="text-sm text-muted-foreground">Electricity Rate ($/kWh)</label>
                  <Input type="number" step="0.01" value={electricityRate} onChange={(e) => setElectricityRate(e.target.value)} className="bg-card border-border mt-1" />
                </div>
                <div>
                  <label className="text-sm text-muted-foreground">Annual Rate Escalation: {rateEscalation[0]}%</label>
                  <Slider value={rateEscalation} onValueChange={setRateEscalation} min={0} max={10} step={0.5} className="mt-3" />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">10-Year Cumulative Savings</p>
                  <p className="text-xl font-semibold text-foreground">${financialProjections.tenYearSavings.toLocaleString()}</p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">25-Year Cumulative Savings</p>
                  <p className="text-xl font-semibold text-foreground">${financialProjections.twentyFiveYearSavings.toLocaleString()}</p>
                </div>
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Break-Even Year</p>
                  <p className="text-xl font-semibold text-foreground">Year {financialProjections.breakEvenYear}</p>
                </div>
              </div>
              <div>
                <h4 className="text-sm font-medium text-foreground mb-3">25-Year Cumulative Savings</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <AreaChart data={financialProjections.savingsData}>
                    <defs>
                      <linearGradient id="savingsGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#B8E8C8" stopOpacity={0.4} />
                        <stop offset="100%" stopColor="#B8E8C8" stopOpacity={0.1} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                    <XAxis dataKey="year" stroke="#6B7280" fontSize={12} />
                    <YAxis stroke="#6B7280" fontSize={12} tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`} />
                    <ReferenceLine
                      y={parseFloat(systemCost) || 25000}
                      stroke="#E8C8B8"
                      strokeDasharray="3 3"
                      label={{ value: "System Cost", fill: "#6B7280", fontSize: 11 }}
                    />
                    <Area type="monotone" dataKey="savings" stroke="#B8E8C8" strokeWidth={2} fill="url(#savingsGradient)" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export default function LuminaGrid() {
  const [activeView, setActiveView] = useState<"home" | "analytics" | "scorecard" | "forecast">("home")
  const [selectedCity, setSelectedCity] = useState<City>(SEED_CITIES[0])
  const [isLoading, setIsLoading] = useState(false)

  const navItems = [
    { id: "home" as const, label: "Home Dashboard", icon: Home },
    { id: "analytics" as const, label: "Solar Analytics", icon: BarChart3 },
    { id: "scorecard" as const, label: "City Scorecard", icon: Award },
    { id: "forecast" as const, label: "Forecast", icon: TrendingUp },
  ]

  const handleCityChange = (city: City) => {
    setIsLoading(true)
    setSelectedCity(city)
    setTimeout(() => setIsLoading(false), 300)
  }

  const viewTitles: Record<typeof activeView, string> = {
    home: "Home Dashboard",
    analytics: "Solar Analytics",
    scorecard: "City Scorecard",
    forecast: "Forecast Simulator",
  }

  return (
    <div className="flex min-h-screen bg-background">
      {/* Sidebar */}
      <aside className="fixed left-0 top-0 z-40 h-screen w-64 bg-card border-r border-border flex flex-col">
        <div className="p-6 border-b border-border">
          <div className="flex items-center gap-2">
            <Sun className="h-6 w-6 text-[#F5E6C8]" />
            <span className="text-xl font-semibold text-foreground">LuminaGrid</span>
          </div>
        </div>
        <nav className="flex-1 p-4 space-y-1">
          {navItems.map((item) => (
            <button
              key={item.id}
              onClick={() => setActiveView(item.id)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg text-left transition-colors ${
                activeView === item.id
                  ? "bg-[#F5E6C8] text-foreground font-medium"
                  : "text-muted-foreground hover:bg-accent hover:text-foreground"
              }`}
            >
              <item.icon className="h-5 w-5" />
              <span>{item.label}</span>
            </button>
          ))}
        </nav>
        <div className="p-4 border-t border-border">
          <p className="text-xs text-muted-foreground" suppressHydrationWarning>
            Data: Databricks SQL · {new Date().toLocaleDateString()}
          </p>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 ml-64">
        <header className="sticky top-0 z-30 bg-card border-b border-border px-6 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold text-foreground">{viewTitles[activeView]}</h1>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <RefreshCw className="h-4 w-4" />
            <span suppressHydrationWarning>Live · {new Date().toLocaleTimeString()}</span>
          </div>
        </header>

        <div className="p-6">
          {isLoading ? (
            <div className="space-y-6">
              <div className="grid grid-cols-4 gap-4">
                {[1, 2, 3, 4].map((i) => <LoadingSkeleton key={i} className="h-24 rounded-lg" />)}
              </div>
              <div className="grid grid-cols-2 gap-6">
                <LoadingSkeleton className="h-64 rounded-lg" />
                <LoadingSkeleton className="h-64 rounded-lg" />
              </div>
            </div>
          ) : (
            <>
              {activeView === "home" && (
                <HomeDashboard selectedCity={selectedCity} setSelectedCity={handleCityChange} />
              )}
              {activeView === "analytics" && (
                <SolarAnalytics selectedCity={selectedCity} setSelectedCity={handleCityChange} />
              )}
              {activeView === "scorecard" && <CityScorecard />}
              {activeView === "forecast" && <Forecast />}
            </>
          )}
        </div>
      </main>
    </div>
  )
}
