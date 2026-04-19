"use client"

import { useState, useMemo } from "react"
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

// ============================================================================
// MOCK DATA
// ============================================================================

const MOCK_DATA = {
  cities: [
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
  ],
  
  kpiData: {
    totalPermits: 12847,
    totalKwh: 48520000,
    totalSavings: 7278000,
    totalCO2Offset: 34328,
  },
  
  zipCodeSavings: [
    { zip: "85001", savings: 892000 },
    { zip: "85003", savings: 756000 },
    { zip: "85004", savings: 698000 },
    { zip: "85006", savings: 645000 },
    { zip: "85007", savings: 589000 },
    { zip: "85008", savings: 534000 },
    { zip: "85009", savings: 478000 },
    { zip: "85012", savings: 421000 },
    { zip: "85013", savings: 365000 },
    { zip: "85014", savings: 312000 },
  ],
  
  permitStatus: [
    { status: "Active", count: 8234, percentage: 64 },
    { status: "Pending", count: 2876, percentage: 22 },
    { status: "Expired", count: 1737, percentage: 14 },
  ],
  
  solarOutput30Day: Array.from({ length: 60 }, (_, i) => {
    const isActual = i < 30
    const baseValue = 1200 + Math.sin(i / 5) * 200 + Math.random() * 100
    return {
      day: i - 29,
      actual: isActual ? baseValue : null,
      projected: !isActual ? baseValue + Math.random() * 50 : null,
      date: new Date(2026, 3, i - 29 + 18).toLocaleDateString("en-US", { month: "short", day: "numeric" }),
    }
  }),
  
  zipMapData: [
    { zip: "85001", savings: 892000, lat: 33.4484, lng: -112.074, intensity: 0.95 },
    { zip: "85003", savings: 756000, lat: 33.4520, lng: -112.078, intensity: 0.82 },
    { zip: "85004", savings: 698000, lat: 33.4450, lng: -112.070, intensity: 0.75 },
    { zip: "85006", savings: 645000, lat: 33.4600, lng: -112.065, intensity: 0.68 },
    { zip: "85007", savings: 589000, lat: 33.4380, lng: -112.085, intensity: 0.62 },
    { zip: "85008", savings: 534000, lat: 33.4700, lng: -112.055, intensity: 0.55 },
    { zip: "85009", savings: 478000, lat: 33.4300, lng: -112.095, intensity: 0.48 },
    { zip: "85012", savings: 421000, lat: 33.4800, lng: -112.080, intensity: 0.42 },
    { zip: "85013", savings: 365000, lat: 33.4900, lng: -112.090, intensity: 0.35 },
    { zip: "85014", savings: 312000, lat: 33.5000, lng: -112.070, intensity: 0.28 },
  ],
  
  trendForecast: Array.from({ length: 36 }, (_, i) => {
    const isHistorical = i < 12
    const baseCapacity = 15000 + i * 800 + Math.random() * 500
    const baseSavings = 2200000 + i * 180000 + Math.random() * 50000
    const baseCO2 = 10500 + i * 950 + Math.random() * 200
    return {
      month: new Date(2025, i + 4, 1).toLocaleDateString("en-US", { month: "short", year: "2-digit" }),
      capacity: baseCapacity,
      savings: baseSavings,
      co2: baseCO2,
      isHistorical,
    }
  }),
  
  permitVelocity: [
    { zip: "85001", data: [45, 52, 48, 61, 58, 72], change: 12.5 },
    { zip: "85003", data: [38, 41, 45, 42, 49, 53], change: 8.2 },
    { zip: "85004", data: [32, 28, 35, 38, 41, 44], change: 7.3 },
    { zip: "85006", data: [28, 31, 29, 33, 36, 39], change: 8.1 },
    { zip: "85007", data: [25, 27, 24, 28, 31, 33], change: 6.5 },
    { zip: "85008", data: [22, 24, 26, 23, 27, 30], change: 11.1 },
    { zip: "85009", data: [19, 21, 18, 22, 24, 26], change: 8.3 },
    { zip: "85012", data: [16, 18, 20, 17, 21, 23], change: 9.5 },
  ],
  
  permits: Array.from({ length: 200 }, (_, i) => ({
    id: `P${10000 + i}`,
    zip: ["85001", "85003", "85004", "85006", "85007", "85008", "85009", "85012"][i % 8],
    systemSize: Math.round(5 + Math.random() * 15 * 10) / 10,
    installDate: new Date(2025, Math.floor(Math.random() * 12), Math.floor(Math.random() * 28) + 1).toLocaleDateString(),
    annualKwh: Math.round(8000 + Math.random() * 20000),
    annualSavings: Math.round(1200 + Math.random() * 3000),
    co2Offset: Math.round(5 + Math.random() * 15 * 10) / 10,
    status: ["Active", "Pending", "Expired"][Math.floor(Math.random() * 3)],
  })),
  
  cityScorecard: [
    { rank: 1, city: "Phoenix", state: "AZ", capacity: 245000, co2: 172900, target: 85, progress: 72, status: "On Track", hyiScore: 94, rate: 0.12, approvalDays: 14, installedKw: 245000, velocity: [120, 135, 142, 158, 165, 178] },
    { rank: 2, city: "Los Angeles", state: "CA", capacity: 312000, co2: 220100, target: 90, progress: 68, status: "On Track", hyiScore: 91, rate: 0.24, approvalDays: 21, installedKw: 312000, velocity: [95, 102, 98, 115, 122, 130] },
    { rank: 3, city: "San Diego", state: "CA", capacity: 198000, co2: 139800, target: 80, progress: 75, status: "Ahead", hyiScore: 89, rate: 0.22, approvalDays: 18, installedKw: 198000, velocity: [88, 92, 95, 101, 108, 115] },
    { rank: 4, city: "Austin", state: "TX", capacity: 156000, co2: 110100, target: 75, progress: 62, status: "On Track", hyiScore: 86, rate: 0.11, approvalDays: 16, installedKw: 156000, velocity: [72, 78, 82, 88, 95, 102] },
    { rank: 5, city: "Denver", state: "CO", capacity: 134000, co2: 94600, target: 70, progress: 58, status: "At Risk", hyiScore: 82, rate: 0.14, approvalDays: 25, installedKw: 134000, velocity: [62, 65, 68, 72, 75, 80] },
    { rank: 6, city: "Miami", state: "FL", capacity: 112000, co2: 79100, target: 65, progress: 54, status: "On Track", hyiScore: 79, rate: 0.13, approvalDays: 19, installedKw: 112000, velocity: [55, 58, 62, 65, 70, 74] },
    { rank: 7, city: "Las Vegas", state: "NV", capacity: 189000, co2: 133400, target: 82, progress: 71, status: "On Track", hyiScore: 88, rate: 0.11, approvalDays: 12, installedKw: 189000, velocity: [82, 88, 92, 98, 105, 112] },
    { rank: 8, city: "Sacramento", state: "CA", capacity: 98000, co2: 69200, target: 72, progress: 48, status: "At Risk", hyiScore: 75, rate: 0.21, approvalDays: 28, installedKw: 98000, velocity: [42, 45, 48, 52, 55, 58] },
    { rank: 9, city: "Albuquerque", state: "NM", capacity: 87000, co2: 61400, target: 68, progress: 52, status: "On Track", hyiScore: 77, rate: 0.13, approvalDays: 20, installedKw: 87000, velocity: [38, 42, 45, 48, 52, 55] },
    { rank: 10, city: "Tucson", state: "AZ", capacity: 76000, co2: 53600, target: 65, progress: 56, status: "On Track", hyiScore: 80, rate: 0.12, approvalDays: 15, installedKw: 76000, velocity: [35, 38, 42, 45, 48, 52] },
  ],
  
  forecastMonthlyOutput: [
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
  ],
  
  cumulativeSavings: Array.from({ length: 25 }, (_, i) => ({
    year: i + 1,
    savings: Math.round((i + 1) * 1850 * (1 + 0.03 * i)),
  })),
  
  states: ["AZ", "CA", "CO", "FL", "NM", "NV", "TX"],
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
    Pending: "bg-[#F5E6C8] text-foreground",
    Expired: "bg-[#E8C8B8] text-foreground",
    "On Track": "bg-[#B8E8C8] text-foreground",
    Ahead: "bg-[#B8D4E8] text-foreground",
    "At Risk": "bg-[#E8C8B8] text-foreground",
  }
  return (
    <Badge variant="secondary" className={`${styles[status] || "bg-muted"} font-medium`}>
      {status}
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
  selectedCity: typeof MOCK_DATA.cities[0]
  setSelectedCity: (city: typeof MOCK_DATA.cities[0]) => void
}) {
  const [searchQuery, setSearchQuery] = useState("")
  const [isSearchOpen, setIsSearchOpen] = useState(false)

  const filteredCities = MOCK_DATA.cities.filter(
    (city) =>
      city.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      city.state.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const pieColors = ["#B8E8C8", "#F5E6C8", "#E8C8B8"]

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
        <p className="text-muted-foreground">April 18, 2026</p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          title="Total Active Permits"
          value={MOCK_DATA.kpiData.totalPermits.toLocaleString()}
          source="ZenPower"
          icon={Sun}
          bgColor="bg-[#F5E6C8]"
        />
        <KPICard
          title="Total Annual kWh Generated"
          value={`${(MOCK_DATA.kpiData.totalKwh / 1000000).toFixed(1)}M`}
          source="NREL PVWatts"
          icon={Zap}
          bgColor="bg-[#B8D4E8]"
        />
        <KPICard
          title="Total Annual Community Savings"
          value={`$${(MOCK_DATA.kpiData.totalSavings / 1000000).toFixed(2)}M`}
          source="EIA"
          icon={DollarSign}
          bgColor="bg-[#B8E8C8]"
        />
        <KPICard
          title="Total CO₂ Offset"
          value={`${(MOCK_DATA.kpiData.totalCO2Offset / 1000).toFixed(1)}K tons/yr`}
          source="EIA Carbon Index"
          icon={Leaf}
          bgColor="bg-[#B8E8C8]"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 10 ZIP Codes by Savings */}
        <Card className="border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base font-medium text-foreground">
              Top 10 ZIP Codes by Annual Savings
            </CardTitle>
            <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart
                data={MOCK_DATA.zipCodeSavings}
                layout="vertical"
                margin={{ left: 50, right: 20 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis
                  type="number"
                  tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
                  stroke="#6B7280"
                  fontSize={12}
                />
                <YAxis
                  type="category"
                  dataKey="zip"
                  stroke="#6B7280"
                  fontSize={12}
                />
                <Bar dataKey="savings" fill="#B8E8C8" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Permit Status Breakdown */}
        <Card className="border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base font-medium text-foreground">
              Permit Status Breakdown
            </CardTitle>
            <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={MOCK_DATA.permitStatus}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={2}
                  dataKey="count"
                  label={({ status, percentage }) => `${status}: ${percentage}%`}
                  labelLine={false}
                >
                  {MOCK_DATA.permitStatus.map((entry, index) => (
                    <Cell key={entry.status} fill={pieColors[index]} />
                  ))}
                </Pie>
                <Legend
                  verticalAlign="bottom"
                  height={36}
                  formatter={(value) => <span className="text-foreground text-sm">{value}</span>}
                />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      {/* 30-Day Solar Output Prediction */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            30-Day Solar Output Prediction
          </CardTitle>
          <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={280}>
            <ComposedChart data={MOCK_DATA.solarOutput30Day} margin={{ left: 10, right: 10 }}>
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
          <div className="grid grid-cols-3 gap-4 mt-4 pt-4 border-t border-border">
            <div className="text-center">
              <p className="text-sm text-muted-foreground">Projected Total kWh</p>
              <p className="text-xl font-semibold text-foreground">42,500 kWh</p>
            </div>
            <div className="text-center">
              <p className="text-sm text-muted-foreground">Estimated Savings</p>
              <p className="text-xl font-semibold text-foreground">$5,100</p>
            </div>
            <div className="text-center">
              <p className="text-sm text-muted-foreground">CO₂ Avoided</p>
              <p className="text-xl font-semibold text-foreground">30.1 tons</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Choropleth Map */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            ZIP-Level Annual Savings Intensity
          </CardTitle>
          <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
        </CardHeader>
        <CardContent>
          <div className="relative bg-[#B8D4E8]/20 rounded-lg overflow-hidden" style={{ height: 400 }}>
            {/* Simplified Map Visualization */}
            <svg viewBox="0 0 400 300" className="w-full h-full">
              {/* Grid pattern */}
              <defs>
                <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
                  <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
                </pattern>
              </defs>
              <rect width="400" height="300" fill="url(#grid)" />
              
              {/* ZIP code regions */}
              {MOCK_DATA.zipMapData.map((zip, i) => {
                const x = 50 + (i % 5) * 65
                const y = 50 + Math.floor(i / 5) * 100
                const opacity = 0.3 + zip.intensity * 0.7
                return (
                  <g key={zip.zip}>
                    <rect
                      x={x}
                      y={y}
                      width={55}
                      height={80}
                      rx={4}
                      fill="#B8E8C8"
                      fillOpacity={opacity}
                      stroke="#1F2937"
                      strokeWidth={1}
                    />
                    <text
                      x={x + 27}
                      y={y + 40}
                      textAnchor="middle"
                      fontSize={11}
                      fill="#1F2937"
                      fontWeight="500"
                    >
                      {zip.zip}
                    </text>
                    <text
                      x={x + 27}
                      y={y + 55}
                      textAnchor="middle"
                      fontSize={9}
                      fill="#6B7280"
                    >
                      ${(zip.savings / 1000).toFixed(0)}K
                    </text>
                  </g>
                )
              })}
            </svg>
            
            {/* Legend */}
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
  selectedCity: typeof MOCK_DATA.cities[0]
  setSelectedCity: (city: typeof MOCK_DATA.cities[0]) => void
}) {
  const [searchQuery, setSearchQuery] = useState("")
  const [isSearchOpen, setIsSearchOpen] = useState(false)
  const [electricityRate, setElectricityRate] = useState([0.12])
  const [newPermitsPerMonth, setNewPermitsPerMonth] = useState([50])
  const [zipFilter, setZipFilter] = useState("")
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [sortField, setSortField] = useState<string>("annualSavings")
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc")

  const filteredCities = MOCK_DATA.cities.filter(
    (city) =>
      city.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      city.state.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const scenarioMetrics = useMemo(() => {
    const rate = electricityRate[0]
    const permits = newPermitsPerMonth[0]
    const avgSystemSize = 8.5 // kW
    const avgSunHours = 5.5
    const daysPerYear = 365
    
    const annualKwh = permits * 12 * avgSystemSize * avgSunHours * daysPerYear * 0.85
    const annualSavings = annualKwh * rate
    const co2Offset = annualKwh * 0.0007
    const systemCost = permits * 12 * avgSystemSize * 2500
    const paybackYears = systemCost / annualSavings

    return {
      annualSavings: annualSavings,
      annualKwh: annualKwh,
      co2Offset: co2Offset,
      paybackYears: paybackYears,
    }
  }, [electricityRate, newPermitsPerMonth])

  const filteredPermits = useMemo(() => {
    return MOCK_DATA.permits
      .filter((p) => {
        if (zipFilter && !p.zip.includes(zipFilter)) return false
        if (statusFilter && p.status !== statusFilter) return false
        return true
      })
      .sort((a, b) => {
        const aVal = a[sortField as keyof typeof a]
        const bVal = b[sortField as keyof typeof b]
        if (typeof aVal === "number" && typeof bVal === "number") {
          return sortDirection === "asc" ? aVal - bVal : bVal - aVal
        }
        return sortDirection === "asc"
          ? String(aVal).localeCompare(String(bVal))
          : String(bVal).localeCompare(String(aVal))
      })
  }, [zipFilter, statusFilter, sortField, sortDirection])

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

      {/* 24-Month Trend Forecast */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            24-Month Trend Forecast — {selectedCity.name}, {selectedCity.state}
          </CardTitle>
          <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={MOCK_DATA.trendForecast} margin={{ left: 10, right: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                dataKey="month"
                stroke="#6B7280"
                fontSize={11}
                tickFormatter={(v, i) => (i % 3 === 0 ? v : "")}
              />
              <YAxis
                yAxisId="left"
                stroke="#6B7280"
                fontSize={12}
                tickFormatter={(v) => `${(v / 1000).toFixed(0)}K`}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                stroke="#6B7280"
                fontSize={12}
                tickFormatter={(v) => `$${(v / 1000000).toFixed(1)}M`}
              />
              <ReferenceLine x="Apr 25" stroke="#6B7280" strokeDasharray="3 3" yAxisId="left" label={{ value: "Forecast Start", fill: "#6B7280", fontSize: 10 }} />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="capacity"
                stroke="#F5E6C8"
                strokeWidth={2}
                dot={false}
                name="Capacity (kW)"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="savings"
                stroke="#B8E8C8"
                strokeWidth={2}
                dot={false}
                name="Savings ($)"
              />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="co2"
                stroke="#B8D4E8"
                strokeWidth={2}
                dot={false}
                name="CO₂ (tons)"
              />
              <Legend />
            </ComposedChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Scenario Modeler */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            Scenario Modeler
          </CardTitle>
          <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <label className="text-sm text-muted-foreground">
                  Assumed Electricity Rate: ${electricityRate[0].toFixed(2)}/kWh
                </label>
                <Slider
                  value={electricityRate}
                  onValueChange={setElectricityRate}
                  min={0.1}
                  max={0.5}
                  step={0.01}
                  className="mt-2"
                />
                <div className="flex justify-between text-xs text-muted-foreground mt-1">
                  <span>$0.10</span>
                  <span>$0.50</span>
                </div>
              </div>
              <div>
                <label className="text-sm text-muted-foreground">
                  Projected New Permits/Month: {newPermitsPerMonth[0]}
                </label>
                <Slider
                  value={newPermitsPerMonth}
                  onValueChange={setNewPermitsPerMonth}
                  min={0}
                  max={200}
                  step={5}
                  className="mt-2"
                />
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
                  ${(scenarioMetrics.annualSavings / 1000000).toFixed(2)}M
                </p>
              </div>
              <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                <p className="text-sm text-muted-foreground">Projected kWh Generated</p>
                <p className="text-xl font-semibold text-foreground">
                  {(scenarioMetrics.annualKwh / 1000000).toFixed(1)}M
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

      {/* Permit Velocity Sparklines */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {MOCK_DATA.permitVelocity.map((zip) => (
          <Card key={zip.zip} className="border-border">
            <CardContent className="p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="font-medium text-foreground">{zip.zip}</span>
                <div className={`flex items-center gap-1 text-sm ${zip.change >= 0 ? "text-[#1F2937]" : "text-[#1F2937]"}`}>
                  {zip.change >= 0 ? (
                    <ArrowUpRight className="h-4 w-4" />
                  ) : (
                    <ArrowDownRight className="h-4 w-4" />
                  )}
                  <span>{Math.abs(zip.change)}%</span>
                </div>
              </div>
              <SparklineChart data={zip.data} color="#B8E8C8" />
              <p className="text-xs text-muted-foreground mt-2">6-month trend</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Permit Explorer Table */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <CardTitle className="text-base font-medium text-foreground">
                Permit Explorer
              </CardTitle>
              <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Input
                placeholder="Search ZIP..."
                value={zipFilter}
                onChange={(e) => setZipFilter(e.target.value)}
                className="w-32 bg-card border-border"
              />
              <div className="flex gap-1">
                {["Active", "Pending", "Expired"].map((status) => (
                  <Button
                    key={status}
                    variant={statusFilter === status ? "default" : "outline"}
                    size="sm"
                    onClick={() => setStatusFilter(statusFilter === status ? null : status)}
                    className={statusFilter === status ? "bg-[#F5E6C8] text-foreground hover:bg-[#F5E6C8]/80" : ""}
                  >
                    {status}
                  </Button>
                ))}
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <ScrollArea className="h-[400px]">
            <Table>
              <TableHeader>
                <TableRow className="border-border">
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("zip")}
                  >
                    ZIP Code {sortField === "zip" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("systemSize")}
                  >
                    System Size (kW) {sortField === "systemSize" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("installDate")}
                  >
                    Install Date {sortField === "installDate" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("annualKwh")}
                  >
                    Annual kWh {sortField === "annualKwh" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("annualSavings")}
                  >
                    Annual Savings {sortField === "annualSavings" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead
                    className="cursor-pointer hover:bg-accent text-foreground"
                    onClick={() => handleSort("co2Offset")}
                  >
                    CO₂ Offset (tons) {sortField === "co2Offset" && (sortDirection === "asc" ? "↑" : "↓")}
                  </TableHead>
                  <TableHead className="text-foreground">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredPermits.map((permit) => (
                  <TableRow key={permit.id} className="border-border">
                    <TableCell className="font-medium text-foreground">{permit.zip}</TableCell>
                    <TableCell className="text-foreground">{permit.systemSize}</TableCell>
                    <TableCell className="text-foreground">{permit.installDate}</TableCell>
                    <TableCell className="text-foreground">{permit.annualKwh.toLocaleString()}</TableCell>
                    <TableCell className="text-foreground">${permit.annualSavings.toLocaleString()}</TableCell>
                    <TableCell className="text-foreground">{permit.co2Offset}</TableCell>
                    <TableCell>
                      <StatusBadge status={permit.status} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </ScrollArea>
        </CardContent>
      </Card>
    </div>
  )
}

function CityScorecard() {
  const [stateFilter, setStateFilter] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [sortBy, setSortBy] = useState("rank")

  const filteredData = useMemo(() => {
    return MOCK_DATA.cityScorecard
      .filter((city) => {
        if (stateFilter && city.state !== stateFilter) return false
        if (statusFilter && city.status !== statusFilter) return false
        return true
      })
      .sort((a, b) => {
        switch (sortBy) {
          case "capacity":
            return b.capacity - a.capacity
          case "hyiScore":
            return b.hyiScore - a.hyiScore
          case "progress":
            return b.progress - a.progress
          default:
            return a.rank - b.rank
        }
      })
  }, [stateFilter, statusFilter, sortBy])

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
            {MOCK_DATA.states.map((state) => (
              <SelectItem key={state} value={state}>
                {state}
              </SelectItem>
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
            <SelectItem value="capacity">Capacity</SelectItem>
            <SelectItem value="hyiScore">HYI Score</SelectItem>
            <SelectItem value="progress">Progress</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Scorecard Table */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            City Rankings
          </CardTitle>
          <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
        </CardHeader>
        <CardContent>
          <ScrollArea className="w-full">
            <div className="min-w-[1200px]">
              <Table>
                <TableHeader>
                  <TableRow className="border-border">
                    <TableHead className="text-foreground">Rank</TableHead>
                    <TableHead className="text-foreground">City</TableHead>
                    <TableHead className="text-foreground">State</TableHead>
                    <TableHead className="text-foreground">Installed kW</TableHead>
                    <TableHead className="text-foreground">CO₂ (tons/yr)</TableHead>
                    <TableHead className="text-foreground">2035 Target</TableHead>
                    <TableHead className="text-foreground">Progress</TableHead>
                    <TableHead className="text-foreground">Status</TableHead>
                    <TableHead className="text-foreground">
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger className="flex items-center gap-1">
                            HYI Score
                            <Info className="h-3 w-3" />
                          </TooltipTrigger>
                          <TooltipContent className="max-w-xs">
                            <p className="text-sm">
                              High-Yield Index: EIA electricity rate (40%) + permit approval speed (30%) + total installed kW (30%)
                            </p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </TableHead>
                    <TableHead className="text-foreground">Rate ($/kWh)</TableHead>
                    <TableHead className="text-foreground">Approval Days</TableHead>
                    <TableHead className="text-foreground">6-Month Velocity</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredData.map((city) => (
                    <TableRow key={city.city} className="border-border">
                      <TableCell>
                        <RankBadge rank={city.rank} />
                      </TableCell>
                      <TableCell className="font-medium text-foreground">{city.city}</TableCell>
                      <TableCell className="text-foreground">{city.state}</TableCell>
                      <TableCell className="text-foreground">{(city.capacity / 1000).toFixed(0)}K</TableCell>
                      <TableCell className="text-foreground">{(city.co2 / 1000).toFixed(1)}K</TableCell>
                      <TableCell className="text-foreground">{city.target}%</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Progress value={city.progress} className="w-20 h-2" />
                          <span className="text-sm text-muted-foreground">{city.progress}%</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={city.status} />
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Progress value={city.hyiScore} className="w-16 h-2" />
                          <span className="text-sm text-foreground">{city.hyiScore}</span>
                        </div>
                      </TableCell>
                      <TableCell className="text-foreground">${city.rate.toFixed(2)}</TableCell>
                      <TableCell className="text-foreground">{city.approvalDays}</TableCell>
                      <TableCell>
                        <div className="w-24">
                          <SparklineChart data={city.velocity} color="#B8D4E8" />
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </ScrollArea>
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
    const paybackYears = cost / annualSavings

    return {
      annualKwh: Math.round(annualKwh),
      monthlyOutput: MOCK_DATA.forecastMonthlyOutput.map((m) => ({
        ...m,
        kwh: Math.round(m.kwh * (capacity / 10)),
      })),
      annualSavings: Math.round(annualSavings),
      co2Offset: Math.round(co2Offset * 10) / 10,
      paybackYears: Math.round(paybackYears * 10) / 10,
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
    const daysPerYear = 365
    const annualKwh = capacity * avgSunHours * daysPerYear * systemEfficiency

    let cumulative = 0
    let breakEvenYear = 25
    const savingsData = Array.from({ length: 25 }, (_, i) => {
      const yearRate = rate * Math.pow(1 + escalation, i)
      const yearSavings = annualKwh * yearRate
      cumulative += yearSavings
      if (cumulative >= cost && breakEvenYear === 25) {
        breakEvenYear = i + 1
      }
      return {
        year: i + 1,
        savings: Math.round(cumulative),
      }
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
    
    // Simulate coordinate update
    const newLat = (33.45 + (50 - y) * 0.01).toFixed(4)
    const newLng = (-112.07 + (x - 50) * 0.01).toFixed(4)
    setLat(newLat)
    setLng(newLng)
  }

  return (
    <div className="space-y-6">
      {/* Map Input Section */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            Location Selection
          </CardTitle>
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
          
          {/* Interactive Map */}
          <div
            className="relative bg-[#B8D4E8]/30 rounded-lg overflow-hidden cursor-crosshair"
            style={{ height: 300 }}
            onClick={handleMapClick}
          >
            <svg viewBox="0 0 400 300" className="w-full h-full">
              {/* Grid pattern */}
              <defs>
                <pattern id="mapGrid" width="20" height="20" patternUnits="userSpaceOnUse">
                  <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#E5E7EB" strokeWidth="0.5" />
                </pattern>
              </defs>
              <rect width="400" height="300" fill="url(#mapGrid)" />
              
              {/* Simulated roads */}
              <line x1="0" y1="150" x2="400" y2="150" stroke="#E5E7EB" strokeWidth="3" />
              <line x1="200" y1="0" x2="200" y2="300" stroke="#E5E7EB" strokeWidth="3" />
              <line x1="100" y1="0" x2="100" y2="300" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="300" y1="0" x2="300" y2="300" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="0" y1="75" x2="400" y2="75" stroke="#E5E7EB" strokeWidth="1" />
              <line x1="0" y1="225" x2="400" y2="225" stroke="#E5E7EB" strokeWidth="1" />
            </svg>
            
            {/* Pin marker */}
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
              <Input
                value={lat}
                onChange={(e) => setLat(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Longitude</label>
              <Input
                value={lng}
                onChange={(e) => setLng(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* System Configuration */}
      <Card className="border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium text-foreground">
            System Configuration
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div>
              <label className="text-sm text-muted-foreground">System Capacity (kW)</label>
              <Input
                type="number"
                value={systemCapacity}
                onChange={(e) => setSystemCapacity(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Panel Tilt (°)</label>
              <Input
                type="number"
                value={tiltAngle}
                onChange={(e) => setTiltAngle(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Azimuth (°)</label>
              <Input
                type="number"
                value={azimuth}
                onChange={(e) => setAzimuth(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
            <div>
              <label className="text-sm text-muted-foreground">Array Type</label>
              <Select value={arrayType} onValueChange={setArrayType}>
                <SelectTrigger className="bg-card border-border mt-1">
                  <SelectValue />
                </SelectTrigger>
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
                <SelectTrigger className="bg-card border-border mt-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="standard">Standard</SelectItem>
                  <SelectItem value="premium">Premium</SelectItem>
                  <SelectItem value="thin-film">Thin Film</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm text-muted-foreground">System Losses (%)</label>
              <Input
                type="number"
                value={losses}
                onChange={(e) => setLosses(e.target.value)}
                className="bg-card border-border mt-1"
              />
            </div>
          </div>
          <Button
            onClick={() => setShowResults(true)}
            className="mt-4 bg-[#F5E6C8] text-foreground hover:bg-[#F5E6C8]/80"
          >
            Run Simulation
          </Button>
        </CardContent>
      </Card>

      {/* Simulation Results */}
      {showResults && (
        <>
          <Card className="border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium text-foreground">
                Simulation Output
              </CardTitle>
              <p className="text-xs text-muted-foreground">Updated: Apr 18, 2026 09:00</p>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Annual AC Energy</p>
                  <p className="text-xl font-semibold text-foreground">
                    {simulationResults.annualKwh.toLocaleString()} kWh
                  </p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Annual Savings</p>
                  <p className="text-xl font-semibold text-foreground">
                    ${simulationResults.annualSavings.toLocaleString()}
                  </p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">CO₂ Offset</p>
                  <p className="text-xl font-semibold text-foreground">
                    {simulationResults.co2Offset} tons/yr
                  </p>
                </div>
                <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Payback Period</p>
                  <p className="text-xl font-semibold text-foreground">
                    {simulationResults.paybackYears} years
                  </p>
                </div>
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Solar Resource</p>
                  <p className="text-xl font-semibold text-foreground">
                    {simulationResults.solarResource} hrs/day
                  </p>
                </div>
                <div className="bg-[#B8D4E8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Location</p>
                  <p className="text-lg font-semibold text-foreground">
                    {lat}°, {lng}°
                  </p>
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

          {/* What-If Financial Panel */}
          <Card className="border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium text-foreground">
                What-If Financial Analysis
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="text-sm text-muted-foreground">Assumed System Cost ($)</label>
                  <Input
                    type="number"
                    value={systemCost}
                    onChange={(e) => setSystemCost(e.target.value)}
                    className="bg-card border-border mt-1"
                  />
                </div>
                <div>
                  <label className="text-sm text-muted-foreground">Electricity Rate ($/kWh)</label>
                  <Input
                    type="number"
                    step="0.01"
                    value={electricityRate}
                    onChange={(e) => setElectricityRate(e.target.value)}
                    className="bg-card border-border mt-1"
                  />
                </div>
                <div>
                  <label className="text-sm text-muted-foreground">
                    Annual Rate Escalation: {rateEscalation[0]}%
                  </label>
                  <Slider
                    value={rateEscalation}
                    onValueChange={setRateEscalation}
                    min={0}
                    max={10}
                    step={0.5}
                    className="mt-3"
                  />
                </div>
              </div>

              <div className="grid grid-cols-3 gap-4">
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">10-Year Cumulative Savings</p>
                  <p className="text-xl font-semibold text-foreground">
                    ${financialProjections.tenYearSavings.toLocaleString()}
                  </p>
                </div>
                <div className="bg-[#B8E8C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">25-Year Cumulative Savings</p>
                  <p className="text-xl font-semibold text-foreground">
                    ${financialProjections.twentyFiveYearSavings.toLocaleString()}
                  </p>
                </div>
                <div className="bg-[#F5E6C8]/20 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Break-Even Year</p>
                  <p className="text-xl font-semibold text-foreground">
                    Year {financialProjections.breakEvenYear}
                  </p>
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
                    <YAxis
                      stroke="#6B7280"
                      fontSize={12}
                      tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
                    />
                    <ReferenceLine
                      y={parseFloat(systemCost) || 25000}
                      stroke="#E8C8B8"
                      strokeDasharray="3 3"
                      label={{ value: "System Cost", fill: "#6B7280", fontSize: 11 }}
                    />
                    <Area
                      type="monotone"
                      dataKey="savings"
                      stroke="#B8E8C8"
                      strokeWidth={2}
                      fill="url(#savingsGradient)"
                    />
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
  const [selectedCity, setSelectedCity] = useState(MOCK_DATA.cities[0])
  const [isLoading, setIsLoading] = useState(false)

  const navItems = [
    { id: "home" as const, label: "Home Dashboard", icon: Home },
    { id: "analytics" as const, label: "Solar Analytics", icon: BarChart3 },
    { id: "scorecard" as const, label: "City Scorecard", icon: Award },
    { id: "forecast" as const, label: "Forecast", icon: TrendingUp },
  ]

  const handleCityChange = (city: typeof MOCK_DATA.cities[0]) => {
    setIsLoading(true)
    setSelectedCity(city)
    setTimeout(() => setIsLoading(false), 500)
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
          <p className="text-xs text-muted-foreground">
            Data last updated: Apr 18, 2026
          </p>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 ml-64">
        {/* Top Bar */}
        <header className="sticky top-0 z-30 bg-card border-b border-border px-6 py-4 flex items-center justify-between">
          <h1 className="text-lg font-semibold text-foreground">{viewTitles[activeView]}</h1>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <RefreshCw className="h-4 w-4" />
            <span>Last Synced: Apr 18, 2026 09:00</span>
          </div>
        </header>

        {/* Content */}
        <div className="p-6">
          {isLoading ? (
            <div className="space-y-6">
              <div className="grid grid-cols-4 gap-4">
                {[1, 2, 3, 4].map((i) => (
                  <LoadingSkeleton key={i} className="h-24 rounded-lg" />
                ))}
              </div>
              <div className="grid grid-cols-2 gap-6">
                <LoadingSkeleton className="h-64 rounded-lg" />
                <LoadingSkeleton className="h-64 rounded-lg" />
              </div>
            </div>
          ) : (
            <>
              {activeView === "home" && (
                <HomeDashboard
                  selectedCity={selectedCity}
                  setSelectedCity={handleCityChange}
                />
              )}
              {activeView === "analytics" && (
                <SolarAnalytics
                  selectedCity={selectedCity}
                  setSelectedCity={handleCityChange}
                />
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
