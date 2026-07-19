use crate::models::{AppError, AppState, DashboardJob, RequestMetric, SharedDashboardState, update_job_status};
use axum::{
    extract::State,
    response::Html,
    Json,
};
use chrono::TimeZone;
use tracing::{error, info};

pub async fn dashboard_page() -> Html<String> {
    Html(r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chambapro FFmpeg API - Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    <style>
        :root {
            --bg-base: #0b0d13;
            --bg-surface: rgba(20, 24, 38, 0.6);
            --border-glow: rgba(99, 102, 241, 0.2);
            --primary: #6366f1;
            --primary-glow: rgba(99, 102, 241, 0.4);
            --success: #10b981;
            --success-glow: rgba(16, 185, 129, 0.2);
            --error: #ef4444;
            --error-glow: rgba(239, 68, 68, 0.2);
            --warning: #f59e0b;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background-color: var(--bg-base);
            color: var(--text-main);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            padding: 2rem;
            padding-bottom: 5rem;
            background-image: radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
                              radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 40%);
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }

        h1 {
            font-size: 2.2rem;
            font-weight: 700;
            background: linear-gradient(135deg, #a5b4fc, #818cf8, #6366f1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .badge-live {
            background: var(--success-glow);
            color: var(--success);
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.35rem;
            border: 1px solid rgba(16, 185, 129, 0.3);
            box-shadow: 0 0 10px var(--success-glow);
        }

        .badge-live::before {
            content: '';
            display: inline-block;
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 0.6; }
            50% { transform: scale(1.2); opacity: 1; }
            100% { transform: scale(0.9); opacity: 0.6; }
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .stat-card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 16px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            transition: transform 0.3s ease, border-color 0.3s ease;
        }

        .stat-card:hover {
            transform: translateY(-2px);
            border-color: var(--primary-glow);
        }

        .stat-label {
            font-size: 0.9rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .stat-value {
            font-size: 2.2rem;
            font-weight: 700;
        }

        .kpis-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .kpi-card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 16px;
            padding: 1.25rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .kpi-title {
            font-size: 1rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .kpi-bar-row {
            margin-bottom: 0.75rem;
        }

        .kpi-bar-label {
            display: flex;
            justify-content: space-between;
            font-size: 0.85rem;
            margin-bottom: 0.25rem;
            font-family: 'JetBrains Mono', monospace;
        }

        .kpi-bar-outer {
            width: 100%;
            height: 6px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 9999px;
            overflow: hidden;
        }

        .kpi-bar-inner {
            height: 100%;
            background: var(--primary);
            border-radius: 9999px;
            width: 0%;
            transition: width 0.5s ease;
        }

        .grid-layout {
            display: grid;
            grid-template-columns: 1.3fr 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }

        @media (max-width: 1024px) {
            .grid-layout {
                grid-template-columns: 1fr;
            }
        }

        .card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 20px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            display: flex;
            flex-direction: column;
            margin-bottom: 2rem;
        }

        .card-header {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.25rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .table-container {
            overflow-y: auto;
            max-height: 400px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            padding: 0.75rem 1rem;
            font-size: 0.85rem;
            color: var(--text-muted);
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
            font-weight: 600;
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            font-size: 0.9rem;
            font-family: 'JetBrains Mono', monospace;
        }

        tr:hover td {
            background: rgba(255, 255, 255, 0.02);
        }

        .status-badge {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-enqueued { background: rgba(99, 102, 241, 0.15); color: #818cf8; }
        .status-processing { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
        .status-success { background: rgba(16, 185, 129, 0.15); color: #34d399; }
        .status-failed { background: rgba(239, 68, 68, 0.15); color: #f87171; }

        .drawer-toggle-btn {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: rgba(20, 24, 38, 0.95);
            border-top: 1px solid var(--primary-glow);
            padding: 1rem;
            text-align: center;
            cursor: pointer;
            z-index: 100;
            font-weight: 600;
            color: #818cf8;
            box-shadow: 0 -5px 20px rgba(0,0,0,0.5);
            transition: background 0.3s;
        }

        .drawer-toggle-btn:hover {
            background: rgba(30, 36, 56, 0.98);
        }

        .drawer {
            position: fixed;
            bottom: -500px;
            left: 0;
            right: 0;
            height: 450px;
            background: #090b10;
            border-top: 1px solid var(--primary-glow);
            box-shadow: 0 -10px 40px rgba(0,0,0,0.8);
            z-index: 101;
            transition: bottom 0.4s cubic-bezier(0.16, 1, 0.3, 1);
            display: flex;
            flex-direction: column;
            padding: 1.5rem;
        }

        .drawer.open {
            bottom: 0;
        }

        .drawer-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
        }

        .drawer-close-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: var(--text-main);
            padding: 0.3rem 0.8rem;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.85rem;
            transition: background 0.3s;
        }

        .drawer-close-btn:hover {
            background: rgba(255, 255, 255, 0.1);
        }

        .terminal {
            flex-grow: 1;
            background: #050608;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 1rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            line-height: 1.5;
            overflow-y: auto;
            color: #d1d5db;
        }

        .log-line {
            margin-bottom: 0.35rem;
            word-break: break-all;
        }

        .log-line.log-info { color: #818cf8; }
        .log-line.log-warn { color: #fbbf24; }
        .log-line.log-error { color: #f87171; }

        select.chart-selector {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.15);
            color: var(--text-main);
            padding: 0.4rem 0.8rem;
            border-radius: 8px;
            font-family: inherit;
            cursor: pointer;
        }

        select.chart-selector:focus {
            outline: none;
            border-color: var(--primary);
        }
    </style>
</head>
<body>

    <header>
        <h1>Chambapro FFmpeg API 🚀</h1>
        <div class="badge-live">LIVE FEED</div>
    </header>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total Jobs</div>
            <div id="stat-total" class="stat-value">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Processing</div>
            <div id="stat-processing" class="stat-value" style="color: var(--warning);">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Success</div>
            <div id="stat-success" class="stat-value" style="color: var(--success);">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Failed</div>
            <div id="stat-failed" class="stat-value" style="color: var(--error);">0</div>
        </div>
    </div>

    <div class="kpis-grid">
        <div class="kpi-card">
            <div class="kpi-title">Execution Mode (Requests) <span style="font-size: 0.8rem; color:#818cf8;">Sync vs Async</span></div>
            <div id="kpi-modes-container"></div>
        </div>

        <div class="kpi-card">
            <div class="kpi-title">Webhooks Processed <span id="kpi-webhook-rate" style="font-size: 0.95rem; color:#10b981; font-weight:700;">100% Ok</span></div>
            <div id="kpi-webhooks-container"></div>
        </div>

        <div class="kpi-card">
            <div class="kpi-title">Top Format Pairs <span style="font-size: 0.8rem; color:#818cf8;">Input → Output</span></div>
            <div id="kpi-pairs-container"></div>
        </div>
    </div>

    <div class="grid-layout">
        <div class="card" style="height: 380px;">
            <div class="card-header">
                <span>API Traffic & Latency</span>
                <select id="granularity" class="chart-selector" onchange="updateMetricChart()">
                    <option value="minute">Minute</option>
                    <option value="hour" selected>Hour</option>
                    <option value="day">Day</option>
                </select>
            </div>
            <div id="metric-chart" style="height: 280px;"></div>
        </div>

        <div class="card" style="height: 380px;">
            <div class="card-header">
                <span>Activity (GitHub style)</span>
            </div>
            <div id="heatmap-chart" style="height: 280px;"></div>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <span>Recent Processes</span>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>UUID</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Retries</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody id="jobs-tbody"></tbody>
            </table>
        </div>
    </div>

    <div id="toggle-drawer-btn" class="drawer-toggle-btn" onclick="openDrawer()">
        📁 Show Live stdout & process logs
    </div>

    <div id="logs-drawer" class="drawer">
        <div class="drawer-header">
            <span style="font-weight: 600; font-size: 1.1rem; color: #818cf8;">stdout & process logs</span>
            <button class="drawer-close-btn" onclick="closeDrawer()">Collapse Drawer ✕</button>
        </div>
        <div id="log-terminal" class="terminal"></div>
    </div>

    <script>
        let metricChartObj = null;
        let heatmapChartObj = null;
        let cachedMetrics = [];
        let cachedJobs = [];

        function openDrawer() {
            document.getElementById('logs-drawer').classList.add('open');
            document.getElementById('toggle-drawer-btn').style.display = 'none';
            const term = document.getElementById('log-terminal');
            term.scrollTop = term.scrollHeight;
        }

        function closeDrawer() {
            document.getElementById('logs-drawer').classList.remove('open');
            setTimeout(() => {
                document.getElementById('toggle-drawer-btn').style.display = 'block';
            }, 300);
        }

        function updateMetricChart() {
            const granularity = document.getElementById('granularity').value;
            const buckets = {};

            cachedMetrics.forEach(m => {
                const date = new Date(m.timestamp);
                let key = '';

                if (granularity === 'minute') {
                    key = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
                } else if (granularity === 'hour') {
                    key = `${date.getHours().toString().padStart(2, '0')}:00`;
                } else {
                    key = `${date.getMonth() + 1}/${date.getDate()}`;
                }

                if (!buckets[key]) {
                    buckets[key] = { count: 0, total_duration: 0 };
                }
                buckets[key].count += 1;
                buckets[key].total_duration += m.duration_ms;
            });

            const sortedKeys = Object.keys(buckets).sort().slice(-15);
            const counts = sortedKeys.map(k => buckets[k].count);
            const avgDurations = sortedKeys.map(k => Math.round(buckets[k].total_duration / buckets[k].count));

            const options = {
                series: [
                    { name: 'Requests', type: 'column', data: counts },
                    { name: 'Avg Latency (ms)', type: 'line', data: avgDurations }
                ],
                chart: {
                    height: 280,
                    type: 'line',
                    toolbar: { show: false },
                    background: 'transparent'
                },
                theme: { mode: 'dark' },
                stroke: { width: [0, 3], curve: 'smooth' },
                colors: ['#6366f1', '#10b981'],
                dataLabels: { enabled: false },
                labels: sortedKeys,
                yaxis: [
                    { title: { text: 'Requests' } },
                    { opposite: true, title: { text: 'Latency (ms)' } }
                ],
                grid: { borderColor: 'rgba(255,255,255,0.05)' }
            };

            if (metricChartObj) {
                metricChartObj.updateOptions(options);
            } else {
                metricChartObj = new ApexCharts(document.getElementById('metric-chart'), options);
                metricChartObj.render();
            }
        }

        function updateHeatmap() {
            const now = new Date();
            const daysData = {};
            
            for (let i = 29; i >= 0; i--) {
                const d = new Date();
                d.setDate(now.getDate() - i);
                const dayKey = `${d.getFullYear()}-${(d.getMonth() + 1).toString().padStart(2, '0')}-${d.getDate().toString().padStart(2, '0')}`;
                daysData[dayKey] = 0;
            }

            cachedMetrics.forEach(m => {
                const date = new Date(m.timestamp);
                const dayKey = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}`;
                if (daysData[dayKey] !== undefined) {
                    daysData[dayKey] += 1;
                }
            });

            const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
            const series = daysOfWeek.map((dayName, idx) => {
                const data = [];
                for (let week = 0; week < 5; week++) {
                    const d = new Date();
                    d.setDate(now.getDate() - (4 - week) * 7 + (idx - now.getDay()));
                    const dayKey = `${d.getFullYear()}-${(d.getMonth() + 1).toString().padStart(2, '0')}-${d.getDate().toString().padStart(2, '0')}`;
                    const count = daysData[dayKey] || 0;
                    data.push({ x: `W${week+1}`, y: count });
                }
                return { name: dayName, data: data };
            });

            const options = {
                series: series,
                chart: {
                    height: 280,
                    type: 'heatmap',
                    toolbar: { show: false }
                },
                theme: { mode: 'dark' },
                dataLabels: { enabled: false },
                plotOptions: {
                    heatmap: {
                        shadeIntensity: 0.5,
                        radius: 2,
                        useFillColorAsStroke: true,
                        colorScale: {
                            ranges: [
                                { from: 0, to: 0, name: 'No activity', color: '#161b22' },
                                { from: 1, to: 3, name: 'Low', color: '#0e4429' },
                                { from: 4, to: 7, name: 'Medium', color: '#006d32' },
                                { from: 8, to: 12, name: 'High', color: '#26a641' },
                                { from: 13, to: 1000, name: 'Very High', color: '#39d353' }
                            ]
                        }
                    }
                }
            };

            if (heatmapChartObj) {
                heatmapChartObj.updateOptions(options);
            } else {
                heatmapChartObj = new ApexCharts(document.getElementById('heatmap-chart'), options);
                heatmapChartObj.render();
            }
        }

        function updateKPIs() {
            let syncCount = cachedMetrics.filter(m => m.endpoint === '/convert').length;
            let asyncCount = cachedMetrics.filter(m => m.endpoint === '/convert-async').length;
            let totalRequests = syncCount + asyncCount || 1;

            let syncPercent = Math.round((syncCount / totalRequests) * 100);
            let asyncPercent = Math.round((asyncCount / totalRequests) * 100);

            document.getElementById('kpi-modes-container').innerHTML = `
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Synchronous (/convert)</span><span>${syncCount} (${syncPercent}%)</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${syncPercent}%; background:#6366f1;"></div></div>
                </div>
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Asynchronous (/convert-async)</span><span>${asyncCount} (${asyncPercent}%)</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${asyncPercent}%; background:#10b981;"></div></div>
                </div>
            `;

            let webhooks = cachedJobs.filter(j => j.job_type === 'Webhook');
            let successWebhooks = webhooks.filter(j => j.status === 'Success').length;
            let failedWebhooks = webhooks.filter(j => j.status === 'Failed').length;
            let totalWebhooks = webhooks.length || 1;

            let successRate = Math.round((successWebhooks / totalWebhooks) * 100);
            document.getElementById('kpi-webhook-rate').innerText = `${successRate}% Ok`;
            if (successRate < 90) {
                document.getElementById('kpi-webhook-rate').style.color = 'var(--error)';
            } else {
                document.getElementById('kpi-webhook-rate').style.color = 'var(--success)';
            }

            let webOkPercent = Math.round((successWebhooks / totalWebhooks) * 100);
            let webFailPercent = Math.round((failedWebhooks / totalWebhooks) * 100);

            document.getElementById('kpi-webhooks-container').innerHTML = `
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Webhook Deliveries Success</span><span>${successWebhooks}</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${webOkPercent}%; background:#10b981;"></div></div>
                </div>
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Webhook Deliveries Failed</span><span>${failedWebhooks}</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${webFailPercent}%; background:#ef4444;"></div></div>
                </div>
            `;

            let formatPairs = {};
            cachedJobs.forEach(j => {
                if (j.job_type.includes('->')) {
                    let parts = j.job_type.split(':');
                    if (parts.length > 1) {
                        let pair = parts[1].replace(')', '').trim();
                        formatPairs[pair] = (formatPairs[pair] || 0) + 1;
                    }
                }
            });

            let sortedPairs = Object.entries(formatPairs)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 3);

            let maxCount = sortedPairs.length > 0 ? sortedPairs[0][1] : 1;
            let pairsHTML = '';

            if (sortedPairs.length === 0) {
                pairsHTML = '<div class="kpi-bar-label" style="color:var(--text-muted);">No conversion pairs recorded yet.</div>';
            } else {
                sortedPairs.forEach(([pair, count]) => {
                    let pct = Math.round((count / maxCount) * 100);
                    pairsHTML += `
                        <div class="kpi-bar-row">
                            <div class="kpi-bar-label"><span>${pair}</span><span>${count} jobs</span></div>
                            <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${pct}%; background:#818cf8;"></div></div>
                        </div>
                    `;
                });
            }
            document.getElementById('kpi-pairs-container').innerHTML = pairsHTML;
        }

        async function fetchDashboardData() {
            try {
                const response = await fetch('/api/dashboard');
                if (!response.ok) return;
                const data = await response.json();
                
                cachedMetrics = data.metrics || [];
                cachedJobs = data.jobs || [];

                let total = data.jobs.length;
                let processing = data.jobs.filter(j => j.status === 'Processing').length;
                let success = data.jobs.filter(j => j.status === 'Success').length;
                let failed = data.jobs.filter(j => j.status === 'Failed').length;

                document.getElementById('stat-total').innerText = total;
                document.getElementById('stat-processing').innerText = processing;
                document.getElementById('stat-success').innerText = success;
                document.getElementById('stat-failed').innerText = failed;

                const tbody = document.getElementById('jobs-tbody');
                tbody.innerHTML = '';
                data.jobs.reverse().forEach(job => {
                    const tr = document.createElement('tr');
                    const uuidShort = job.uuid.substring(0, 8) + '...';
                    const statusClass = 'status-' + job.status.toLowerCase();
                    
                    tr.innerHTML = `
                        <td title="${job.uuid}">${uuidShort}</td>
                        <td>${job.job_type}</td>
                        <td><span class="status-badge ${statusClass}">${job.status}</span></td>
                        <td>${job.retries}</td>
                        <td>${job.timestamp}</td>
                    `;
                    tbody.appendChild(tr);
                });

                const terminal = document.getElementById('log-terminal');
                const wasScrolledToBottom = terminal.scrollHeight - terminal.clientHeight <= terminal.scrollTop + 1;
                
                terminal.innerHTML = '';
                data.logs.forEach(line => {
                    const div = document.createElement('div');
                    div.className = 'log-line';
                    
                    if (line.includes('INFO')) {
                        div.classList.add('log-info');
                    } else if (line.includes('WARN')) {
                        div.classList.add('log-warn');
                    } else if (line.includes('ERROR')) {
                        div.classList.add('log-error');
                    }
                    
                    div.innerText = line.trim();
                    terminal.appendChild(div);
                });

                if (wasScrolledToBottom) {
                    terminal.scrollTop = terminal.scrollHeight;
                }

                updateMetricChart();
                updateHeatmap();
                updateKPIs();

            } catch (err) {
                console.error("Dashboard poll error:", err);
            }
        }

        setInterval(fetchDashboardData, 2000);
        fetchDashboardData();
    </script>
</body>
</html>"##.to_string())
}

pub async fn dashboard_api(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    if let Ok(data) = state.dashboard.0.read() {
        Ok(Json(serde_json::json!({
            "jobs": data.jobs,
            "logs": data.logs,
            "metrics": data.metrics,
        })))
    } else {
        Err(anyhow::anyhow!("Failed to read dashboard state").into())
    }
}

async fn perform_dashboard_disk_cleanup(storage_dir: &str) -> anyhow::Result<()> {
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(30 * 24 * 3600);
    let mut cleaned_count = 0;

    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    if let Ok(mut entries) = tokio::fs::read_dir(&jobs_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    if let Ok(mut entries) = tokio::fs::read_dir(&metrics_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if cleaned_count > 0 {
        info!("Dashboard retention cleanup: removed {} expired data cache files", cleaned_count);
    }
    Ok(())
}

pub async fn perform_directory_cleanup(
    storage_dir: &str,
    cleanup_hours: u64,
    dashboard: &SharedDashboardState,
) -> anyhow::Result<()> {
    let mut dir = tokio::fs::read_dir(storage_dir).await?;
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(cleanup_hours * 3600);
    let mut cleaned_count = 0;

    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            if let Ok(metadata) = entry.metadata().await {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(age) = now.duration_since(modified) {
                        if age > max_age {
                            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                                let uuid_part = file_name.split('.').next().unwrap_or(file_name);
                                info!("Cleaning up expired file: {:?}", file_name);
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                    update_job_status(
                                        dashboard,
                                        uuid_part.to_string(),
                                        "Cleanup (Auto)",
                                        "Success",
                                        0,
                                        None,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if cleaned_count > 0 {
        info!("Directory cleanup finished. Removed {} expired files.", cleaned_count);
    }

    let _ = perform_dashboard_disk_cleanup(storage_dir).await;

    if let Ok(mut state) = dashboard.0.write() {
        let max_age_metrics = chrono::Duration::days(30);
        let now_time = chrono::Local::now();
        
        state.metrics.retain(|m| {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&m.timestamp) {
                let age = now_time.signed_duration_since(dt);
                age < max_age_metrics
            } else {
                true
            }
        });
        
        state.jobs.retain(|j| {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&j.timestamp, "%Y-%m-%d %H:%M:%S") {
                if let Some(dt_local) = chrono::Local.from_local_datetime(&dt).single() {
                    let age = now_time.signed_duration_since(dt_local);
                    age < max_age_metrics
                } else {
                    true
                }
            } else {
                true
            }
        });
    }

    Ok(())
}

pub async fn load_dashboard_from_disk(storage_dir: &str) -> crate::models::DashboardState {
    let mut jobs = Vec::new();
    let mut metrics = Vec::new();

    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    let _ = tokio::fs::create_dir_all(&jobs_dir).await;
    let _ = tokio::fs::create_dir_all(&metrics_dir).await;

    if let Ok(mut entries) = tokio::fs::read_dir(&jobs_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().is_file() {
                if let Ok(content) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(job) = serde_json::from_str::<DashboardJob>(&content) {
                        jobs.push(job);
                    }
                }
            }
        }
    }
    jobs.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    if let Ok(mut entries) = tokio::fs::read_dir(&metrics_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().is_file() {
                if let Ok(content) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(metric) = serde_json::from_str::<RequestMetric>(&content) {
                        metrics.push(metric);
                    }
                }
            }
        }
    }
    metrics.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    info!("Loaded {} jobs and {} request metrics from disk cache", jobs.len(), metrics.len());

    crate::models::DashboardState {
        jobs,
        logs: Vec::new(),
        metrics,
    }
}
