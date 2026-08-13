/**
 * dashboard.js
 * AdventureWorks DE Dashboard — client-side utilities
 */

'use strict';

// ── Plotly default config for all charts ─────────────────────
window.PLOTLY_CONFIG = {
  responsive: true,
  displayModeBar: false,
};

window.DARK_LAYOUT = {
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font: { family: 'Inter, sans-serif', color: '#90c2a2', size: 12 },
  xaxis: {
    color: '#638c73',
    gridcolor: '#122018',
    linecolor: '#1b3325',
    zerolinecolor: '#1b3325',
  },
  yaxis: {
    color: '#638c73',
    gridcolor: '#122018',
    linecolor: '#1b3325',
    zerolinecolor: '#1b3325',
  },
  legend: { bgcolor: 'transparent', font: { color: '#90c2a2' } },
  margin: { t: 20, b: 40, l: 55, r: 20 },
};

// ── Colour palettes ───────────────────────────────────────────
window.PALETTE = [
  '#00ff66', '#00e5ff', '#ffab00', '#1de9b6',
  '#d500f9', '#ff6d00', '#ff1744', '#4dff94',
];

// ── Number formatter ─────────────────────────────────────────
function fmt(n) {
  if (n === null || n === undefined || n === '') return '—';
  const num = parseFloat(n);
  if (isNaN(num)) return n;
  if (Math.abs(num) >= 1e6) return '$' + (num/1e6).toFixed(2) + 'M';
  return num.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

// ── Pipeline status poller ────────────────────────────────────
function startPipelinePoller(onComplete) {
  const poller = setInterval(() => {
    fetch('/api/pipeline/status')
      .then(r => r.json())
      .then(data => {
        if (!data.running) {
          clearInterval(poller);
          if (typeof onComplete === 'function') onComplete(data.last_report);
        }
      })
      .catch(() => clearInterval(poller));
  }, 2500);
  return poller;
}

// ── Table search (client-side) ────────────────────────────────
function initTableSearch(inputId, tableId) {
  const input = document.getElementById(inputId);
  const table = document.getElementById(tableId);
  if (!input || !table) return;

  input.addEventListener('input', () => {
    const q = input.value.toLowerCase();
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
      row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
    });
  });
}

// ── Auto-dismiss flash alerts after 6 seconds ─────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.alert-de').forEach(el => {
    setTimeout(() => {
      el.style.transition = 'opacity 0.5s ease';
      el.style.opacity = '0';
      setTimeout(() => el.remove(), 500);
    }, 6000);
  });
});
