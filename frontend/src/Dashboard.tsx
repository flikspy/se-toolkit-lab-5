import { useState, useEffect, useReducer, FormEvent } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'
import './App.css'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// API response types
interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface LabItem {
  id: number
  type: string
  title: string
  parent_id: number | null
}

type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string }

type FetchAction<T> =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: T }
  | { type: 'fetch_error'; message: string }

function createFetchReducer<T>() {
  return function reducer(
    _state: FetchState<T>,
    action: FetchAction<T>,
  ): FetchState<T> {
    switch (action.type) {
      case 'fetch_start':
        return { status: 'loading' }
      case 'fetch_success':
        return { status: 'success', data: action.data }
      case 'fetch_error':
        return { status: 'error', message: action.message }
    }
  }
}

const STORAGE_KEY = 'api_key'
const API_BASE = '/analytics'

interface DashboardState {
  token: string
  selectedLab: string
  labs: LabItem[]
}

function Dashboard() {
  const [token, setToken] = useState(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [selectedLab, setSelectedLab] = useState<string>('')
  const [labs, setLabs] = useState<LabItem[]>([])

  const [scoresState, scoresDispatch] = useReducer(
    createFetchReducer<ScoreBucket[]>(),
    { status: 'idle' },
  )
  const [timelineState, timelineDispatch] = useReducer(
    createFetchReducer<TimelineEntry[]>(),
    { status: 'idle' },
  )
  const [passRatesState, passRatesDispatch] = useReducer(
    createFetchReducer<PassRate[]>(),
    { status: 'idle' },
  )

  // Fetch labs list on mount
  useEffect(() => {
    if (!token) return

    fetch('/items/?type=lab', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: LabItem[]) => {
        setLabs(data)
        if (data.length > 0 && !selectedLab) {
          // Set default selected lab (e.g., "lab-04" from title "Lab 04 — Testing")
          const firstLab = data[0]
          const labId = firstLab.title.toLowerCase().replace('lab ', 'lab-').split(' ')[0]
          setSelectedLab(labId)
        }
      })
      .catch(() => {
        // If labs fetch fails, use default lab IDs
        setLabs([
          { id: 1, type: 'lab', title: 'Lab 04 — Testing', parent_id: null },
          { id: 5, type: 'lab', title: 'Lab 03 — Backend', parent_id: null },
        ])
        if (!selectedLab) setSelectedLab('lab-04')
      })
  }, [token, selectedLab])

  // Fetch analytics data when lab changes
  useEffect(() => {
    if (!token || !selectedLab) return

    const headers = { Authorization: `Bearer ${token}` }

    // Fetch scores
    scoresDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE}/scores?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: ScoreBucket[]) =>
        scoresDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        scoresDispatch({ type: 'fetch_error', message: err.message }),
      )

    // Fetch timeline
    timelineDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE}/timeline?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: TimelineEntry[]) =>
        timelineDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        timelineDispatch({ type: 'fetch_error', message: err.message }),
      )

    // Fetch pass rates
    passRatesDispatch({ type: 'fetch_start' })
    fetch(`${API_BASE}/pass-rates?lab=${selectedLab}`, { headers })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: PassRate[]) =>
        passRatesDispatch({ type: 'fetch_success', data }),
      )
      .catch((err: Error) =>
        passRatesDispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token, selectedLab])

  function handleLabChange(e: FormEvent<HTMLSelectElement>) {
    setSelectedLab((e.target as HTMLSelectElement).value)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setSelectedLab('')
    setLabs([])
  }

  if (!token) {
    return (
      <div className="token-form">
        <h1>Dashboard</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={token}
          onChange={(e) => {
            localStorage.setItem(STORAGE_KEY, e.target.value)
            setToken(e.target.value)
          }}
        />
        <button type="button" onClick={() => setToken(token)}>
          Connect
        </button>
      </div>
    )
  }

  // Prepare chart data for scores
  const scoresChartData = {
    labels:
      scoresState.status === 'success'
        ? scoresState.data.map((b) => b.bucket)
        : [],
    datasets: [
      {
        label: 'Students',
        data:
          scoresState.status === 'success'
            ? scoresState.data.map((b) => b.count)
            : [],
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  // Prepare chart data for timeline
  const timelineChartData = {
    labels:
      timelineState.status === 'success'
        ? timelineState.data.map((d) => d.date)
        : [],
    datasets: [
      {
        label: 'Submissions',
        data:
          timelineState.status === 'success'
            ? timelineState.data.map((d) => d.submissions)
            : [],
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
      },
    ],
  }

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  }

  return (
    <div>
      <header className="app-header">
        <h1>Analytics Dashboard</h1>
        <button className="btn-disconnect" onClick={handleDisconnect}>
          Disconnect
        </button>
      </header>

      <div className="controls" style={{ marginBottom: '1rem' }}>
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={handleLabChange}
          style={{ padding: '0.5rem' }}
        >
          {labs.map((lab) => {
            const labId = lab.title.toLowerCase().replace('lab ', 'lab-').split(' ')[0]
            return (
              <option key={lab.id} value={labId}>
                {lab.title}
              </option>
            )
          })}
        </select>
      </div>

      <div className="dashboard-grid">
        {/* Score Distribution Chart */}
        <section className="chart-section">
          <h2>Score Distribution</h2>
          {scoresState.status === 'loading' && <p>Loading...</p>}
          {scoresState.status === 'error' && <p>Error: {scoresState.message}</p>}
          {scoresState.status === 'success' && (
            <Bar data={scoresChartData} options={chartOptions} />
          )}
        </section>

        {/* Timeline Chart */}
        <section className="chart-section">
          <h2>Submissions Timeline</h2>
          {timelineState.status === 'loading' && <p>Loading...</p>}
          {timelineState.status === 'error' && (
            <p>Error: {timelineState.message}</p>
          )}
          {timelineState.status === 'success' && (
            <Line data={timelineChartData} options={chartOptions} />
          )}
        </section>
      </div>

      {/* Pass Rates Table */}
      <section className="table-section">
        <h2>Pass Rates by Task</h2>
        {passRatesState.status === 'loading' && <p>Loading...</p>}
        {passRatesState.status === 'error' && (
          <p>Error: {passRatesState.message}</p>
        )}
        {passRatesState.status === 'success' && (
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRatesState.data.map((rate, index) => (
                <tr key={index}>
                  <td>{rate.task}</td>
                  <td>{rate.avg_score.toFixed(1)}</td>
                  <td>{rate.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}

export default Dashboard
