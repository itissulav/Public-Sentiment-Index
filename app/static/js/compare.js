// app/static/js/compare.js
// All Jinja2 template data is read from the #psi-compare-data JSON block.

(function () {
  var _data = {};
  try {
    _data = JSON.parse(document.getElementById('psi-compare-data').textContent);
  } catch (e) {
    console.error('[PSI] Failed to parse compare data block:', e);
  }
  var SUGGESTIONS   = _data.suggestions  || [];
  var _comparisonRaw = _data.comparison || null;

  // ── Custom topic dropdown ───────────────────────────────────
  var pairs = [
    { inputId: 'inputA', dropdownId: 'dropdownA', accentColor: '#4facfe' },
    { inputId: 'inputB', dropdownId: 'dropdownB', accentColor: '#a855f7' },
  ];

  pairs.forEach(function ({ inputId, dropdownId, accentColor }) {
    const input    = document.getElementById(inputId);
    const dropdown = document.getElementById(dropdownId);
    if (!input || !dropdown) return;

    let activeIdx = -1;

    function getFiltered(q) {
      const lq = q.trim().toLowerCase();
      if (!lq) return SUGGESTIONS;
      return SUGGESTIONS.filter(s => s.name.toLowerCase().includes(lq));
    }

    function renderDropdown(items) {
      if (!items.length) { dropdown.classList.remove('open'); return; }

      const featured = items.filter(s => s.kind === 'featured');
      const saved    = items.filter(s => s.kind !== 'featured');
      let html = '';

      if (featured.length) {
        html += `<div class="dd-section-head">Featured Topics</div>`;
        html += featured.map(s => {
          const img = s.img
            ? `<div class="dd-img-wrap"><img class="dd-thumb" src="/static/images/${s.img}" alt="" loading="lazy"></div>`
            : `<div class="dd-img-wrap dd-img-placeholder"></div>`;
          return `<div class="topic-option dd-featured" data-name="${s.name}">
            ${img}
            <span class="topic-option-name">${s.name}</span>
            <span class="topic-badge topic-badge-featured">Featured</span>
          </div>`;
        }).join('');
      }

      if (saved.length) {
        html += `<div class="dd-section-head">Your Scans</div>`;
        html += saved.map(s => {
          const initial = s.name.trim().charAt(0).toUpperCase();
          return `<div class="topic-option dd-saved" data-name="${s.name}">
            <div class="dd-initials">${initial}</div>
            <span class="topic-option-name">${s.name}</span>
            <span class="topic-badge topic-badge-saved">Scan</span>
          </div>`;
        }).join('');
      }

      dropdown.innerHTML = html;

      dropdown.querySelectorAll('.topic-option').forEach(el => {
        el.addEventListener('mousedown', e => {
          e.preventDefault();
          input.value = el.dataset.name;
          dropdown.classList.remove('open');
          activeIdx = -1;
        });
      });

      activeIdx = -1;
      dropdown.classList.add('open');
    }

    function updateActive() {
      dropdown.querySelectorAll('.topic-option').forEach((el, i) => {
        el.classList.toggle('active', i === activeIdx);
        if (i === activeIdx) el.scrollIntoView({ block: 'nearest' });
      });
    }

    input.addEventListener('focus', () => renderDropdown(getFiltered(input.value)));
    input.addEventListener('input', () => renderDropdown(getFiltered(input.value)));

    input.addEventListener('keydown', e => {
      const opts = dropdown.querySelectorAll('.topic-option');
      if (!dropdown.classList.contains('open')) return;
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        activeIdx = Math.min(activeIdx + 1, opts.length - 1);
        updateActive();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        activeIdx = Math.max(activeIdx - 1, 0);
        updateActive();
      } else if (e.key === 'Enter' && activeIdx >= 0) {
        e.preventDefault();
        input.value = opts[activeIdx].dataset.name;
        dropdown.classList.remove('open');
        activeIdx = -1;
      } else if (e.key === 'Escape') {
        dropdown.classList.remove('open');
        activeIdx = -1;
      }
    });

    input.addEventListener('blur', () => {
      setTimeout(() => { dropdown.classList.remove('open'); activeIdx = -1; }, 150);
    });
  });

  document.addEventListener('DOMContentLoaded', function () {

    // ── Loading indicator on form submit ─────────────────────
    const form = document.getElementById('compareForm');
    if (form) {
      form.addEventListener('submit', function () {
        const btn    = document.getElementById('compareBtn');
        const loader = document.getElementById('compareLoader');
        const bar    = document.getElementById('loaderBar');
        const pct    = document.getElementById('loaderPct');
        const txt    = document.getElementById('loaderText');
        if (btn && loader) {
          btn.disabled  = true;
          btn.textContent = 'Comparing…';
          loader.style.display = 'block';
          let p = 0;
          const iv = setInterval(async () => {
            try {
              const res  = await fetch('/api/fetch_progress');
              const data = await res.json();
              if (data.status === 'fetching' && data.total > 0) {
                p = Math.floor((data.current / data.total) * 85);
                txt.innerHTML = `<span class="c-spinner"></span> ${data.message} (${data.current}/${data.total})`;
              } else if (data.status === 'analyzing') {
                p = 90;
                txt.innerHTML = `<span class="c-spinner"></span> ${data.message}`;
              } else if (data.status === 'complete') {
                p = 100;
                clearInterval(iv);
              } else {
                p = Math.min(p + 2, 88);
              }
            } catch (_) { p = Math.min(p + 1, 88); }
            bar.style.width = p + '%';
            pct.textContent = p + '%';
          }, 500);
        }
      });
    }

    // ── Chart rendering ───────────────────────────────────────
    const allRaw = _comparisonRaw;
    if (!allRaw) return;

    Chart.defaults.color = '#94a3b8';
    Chart.defaults.font.family = "'Inter', -apple-system, sans-serif";
    Chart.defaults.font.size   = 12;
    Chart.defaults.plugins.tooltip.backgroundColor  = 'rgba(11,15,25,0.95)';
    Chart.defaults.plugins.tooltip.titleColor       = '#fff';
    Chart.defaults.plugins.tooltip.bodyColor        = '#94a3b8';
    Chart.defaults.plugins.tooltip.padding          = 12;
    Chart.defaults.plugins.tooltip.borderColor      = 'rgba(0,242,254,0.25)';
    Chart.defaults.plugins.tooltip.borderWidth      = 1;
    Chart.defaults.plugins.tooltip.cornerRadius     = 8;
    Chart.defaults.scale.grid.color = 'rgba(255,255,255,0.04)';

    // Color palette: Topic A = cyan/blue, Topic B = purple
    const CA = {
      solid: '#4facfe', area: 'rgba(79,172,254,0.15)',
      pos:   'rgba(79,172,254,0.75)', neg: 'rgba(79,172,254,0.4)',
    };
    const CB = {
      solid: '#a855f7', area: 'rgba(168,85,247,0.15)',
      pos:   'rgba(168,85,247,0.75)', neg: 'rgba(168,85,247,0.4)',
    };

    // ── Hero Banner (uses overall data) ─────────────────────
    const hero = document.getElementById('compareHero');
    if (hero && allRaw.overall) {
      const ta = allRaw.overall.topic_a, tb = allRaw.overall.topic_b;
      const nameA = ta.name, nameB = tb.name;
      const fmtRating = r => (r > 0 ? '+' : '') + r.toFixed(1);
      const sentColor = s => s === 'Positive' ? '#10b981' : (s === 'Negative' ? '#ef4444' : '#4facfe');
      const pctA = ((ta.rating + 100) / 200 * 100).toFixed(1);
      const pctB = ((tb.rating + 100) / 200 * 100).toFixed(1);

      const TOPIC_IMAGES = {
        "Donald Trump": "donaldtrump.png", "The Boys": "theboys.png",
        "Avengers Doomsday": "avengersdoomsday.png", "Macbook Neo": "macbookneo.png",
        "America vs Iran": "americavsiran.png"
      };
      const imgA = TOPIC_IMAGES[nameA] ? `/static/images/${TOPIC_IMAGES[nameA]}` : null;
      const imgB = TOPIC_IMAGES[nameB] ? `/static/images/${TOPIC_IMAGES[nameB]}` : null;
      const bgA  = imgA ? `url('${imgA}')` : 'linear-gradient(135deg,rgba(79,172,254,0.3),rgba(0,242,254,0.1))';
      const bgB  = imgB ? `url('${imgB}')` : 'linear-gradient(135deg,rgba(168,85,247,0.3),rgba(236,72,153,0.1))';

      hero.innerHTML = `
        <div class="hero-panel hero-a" style="--panel-bg:${bgA}">
          <div class="hero-panel-overlay"></div>
          <div class="hero-panel-content">
            <span class="hero-topic-label">Topic A</span>
            <h2 class="hero-topic-name">${nameA}</h2>
            <div class="hero-rating" style="color:${sentColor(ta.sentiment)}">${fmtRating(ta.rating)}</div>
            <span class="hero-sentiment-pill" style="background:${sentColor(ta.sentiment)}22;color:${sentColor(ta.sentiment)};border-color:${sentColor(ta.sentiment)}66">${ta.sentiment.toUpperCase()}</span>
            <p class="hero-comments">${ta.total_comments.toLocaleString()} comments</p>
            <div class="hero-bar-track">
              <div class="hero-bar-center"></div>
              ${ta.rating >= 0
                ? `<div class="hero-bar-fill" style="left:50%;width:${(pctA-50).toFixed(1)}%;background:linear-gradient(90deg,rgba(16,185,129,0.5),rgba(16,185,129,1));"></div>`
                : `<div class="hero-bar-fill" style="left:${pctA}%;width:${(50-pctA).toFixed(1)}%;background:linear-gradient(90deg,rgba(239,68,68,1),rgba(239,68,68,0.5));"></div>`}
            </div>
            <div class="hero-bar-axis"><span>-100</span><span>0</span><span>+100</span></div>
          </div>
        </div>
        <div class="hero-vs">VS</div>
        <div class="hero-panel hero-b" style="--panel-bg:${bgB}">
          <div class="hero-panel-overlay"></div>
          <div class="hero-panel-content">
            <span class="hero-topic-label" style="color:#a855f7">Topic B</span>
            <h2 class="hero-topic-name">${nameB}</h2>
            <div class="hero-rating" style="color:${sentColor(tb.sentiment)}">${fmtRating(tb.rating)}</div>
            <span class="hero-sentiment-pill" style="background:${sentColor(tb.sentiment)}22;color:${sentColor(tb.sentiment)};border-color:${sentColor(tb.sentiment)}66">${tb.sentiment.toUpperCase()}</span>
            <p class="hero-comments">${tb.total_comments.toLocaleString()} comments</p>
            <div class="hero-bar-track">
              <div class="hero-bar-center"></div>
              ${tb.rating >= 0
                ? `<div class="hero-bar-fill" style="left:50%;width:${(pctB-50).toFixed(1)}%;background:linear-gradient(90deg,rgba(16,185,129,0.5),rgba(16,185,129,1));"></div>`
                : `<div class="hero-bar-fill" style="left:${pctB}%;width:${(50-pctB).toFixed(1)}%;background:linear-gradient(90deg,rgba(239,68,68,1),rgba(239,68,68,0.5));"></div>`}
            </div>
            <div class="hero-bar-axis"><span>-100</span><span>0</span><span>+100</span></div>
          </div>
        </div>`;
    }

    // ── Keyword phrase card renderer (same as trends page) ────
    function renderCmpKwCards(containerId, kwData, accentColor, accentBg) {
      const el = document.getElementById(containerId);
      if (!el || !kwData || !kwData.labels || !kwData.labels.length) return;
      el.innerHTML = '';
      const upSvg = `<svg class="rc-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3H14z"/><path d="M7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3"/></svg>`;
      const maxCount = Math.max(...(kwData.values || [1]));
      kwData.labels.forEach((phrase, i) => {
        const score    = (kwData.scores  || [])[i] ?? 0;
        const quote    = (kwData.quotes  || [])[i] ?? '';
        const sub      = (kwData.subreddits || [])[i] ?? '';
        const author   = (kwData.authors || [])[i] ?? '';
        const count    = (kwData.values  || [])[i] ?? 0;
        const pct      = maxCount > 0 ? Math.round(count / maxCount * 100) : 0;
        const words    = phrase.split(' ');
        const w0 = words[0] || '', w1 = words[1] || '';
        const boldPhrase = `<strong style="color:rgba(255,255,255,0.85)">${w0}</strong> ${w1}`;
        el.innerHTML += `
          <div class="rc-card" style="--rc-border:${accentColor};--rc-accent:${accentColor};background:${accentBg}">
            <div class="rc-vote">
              ${upSvg}
              <span class="rc-score">${score > 0 ? score : pct + '%'}</span>
            </div>
            <div class="rc-content">
              <div class="rc-meta">
                ${sub ? `<span class="rc-sub">r/${sub}</span>` : ''}
                ${author ? `<span class="rc-author">u/${author}</span>` : ''}
              </div>
              <div class="rc-title">${boldPhrase}</div>
              ${quote ? `<div class="rc-body">"${quote}"</div>` : ''}
            </div>
          </div>`;
      });
    }

    // ── Tab rendering ─────────────────────────────────────────
    const _rendered = {};

    function renderCompareTab(tabKey, prefix, raw) {
      if (_rendered[tabKey]) return;
      _rendered[tabKey] = true;

      if (!raw) {
        const panel = document.getElementById('panel-' + tabKey);
        if (panel) {
          const label = tabKey.charAt(0).toUpperCase() + tabKey.slice(1);
          panel.innerHTML = `<div class="no-source-data"><p>No ${label} data available for this comparison.</p></div>`;
        }
        return;
      }

      const nameA = raw.topic_a.name;
      const nameB = raw.topic_b.name;
      const p = prefix;

      function gc(id) { return document.getElementById(p + id); }

      const sharedOpts = { responsive: true, maintainAspectRatio: false };
      const grouped = {
        ...sharedOpts,
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { position: 'top' } },
        scales: { y: { beginAtZero: true } },
      };

      // Keyword card titles
      const kwA = gc('kwTitleA');
      if (kwA) { kwA.textContent = nameA + ' — Keywords'; kwA.style.color = CA.solid; }
      const kwB = gc('kwTitleB');
      if (kwB) { kwB.textContent = nameB + ' — Keywords'; kwB.style.color = CB.solid; }

      // Keyword phrase topic headers
      const kwPhA = gc('kwPhraseHeadA');
      if (kwPhA) kwPhA.textContent = nameA;
      const kwPhB = gc('kwPhraseHeadB');
      if (kwPhB) kwPhB.textContent = nameB;

      // Render Reddit-style keyword phrase cards
      if (raw.keywords_a) {
        renderCmpKwCards(p + 'kwCardsAPos', raw.keywords_a.positive, CA.solid, 'rgba(79,172,254,0.08)');
        renderCmpKwCards(p + 'kwCardsANeg', raw.keywords_a.negative, '#ef4444', 'rgba(239,68,68,0.08)');
      }
      if (raw.keywords_b) {
        renderCmpKwCards(p + 'kwCardsBPos', raw.keywords_b.positive, CB.solid, 'rgba(168,85,247,0.08)');
        renderCmpKwCards(p + 'kwCardsBNeg', raw.keywords_b.negative, '#ef4444', 'rgba(239,68,68,0.08)');
      }

      // ── Sentiment Split ─────────────────────────────────────
      new Chart(gc('chartSplit'), {
        type: 'bar',
        data: {
          labels: raw.chart_split.labels,
          datasets: [
            { label: nameA, data: raw.chart_split.topic_a,
              backgroundColor: CA.pos, borderColor: CA.solid, borderWidth: 1, borderRadius: 6 },
            { label: nameB, data: raw.chart_split.topic_b,
              backgroundColor: CB.pos, borderColor: CB.solid, borderWidth: 1, borderRadius: 6 },
          ]
        },
        options: { ...grouped, scales: { y: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } } } },
      });

      // ── Avg Upvotes ──────────────────────────────────────────
      new Chart(gc('chartUpvotes'), {
        type: 'bar',
        data: {
          labels: raw.chart_upvotes.labels,
          datasets: [
            { label: nameA, data: raw.chart_upvotes.topic_a,
              backgroundColor: CA.pos, borderColor: CA.solid, borderWidth: 1, borderRadius: 6 },
            { label: nameB, data: raw.chart_upvotes.topic_b,
              backgroundColor: CB.pos, borderColor: CB.solid, borderWidth: 1, borderRadius: 6 },
          ]
        },
        options: grouped,
      });

      // ── Timeline ────────────────────────────────────────────
      new Chart(gc('chartTimeline'), {
        type: 'line',
        data: {
          labels: raw.chart_timeline.labels,
          datasets: [
            { label: nameA + ' — % Positive', data: raw.chart_timeline.topic_a,
              borderColor: CA.solid, backgroundColor: CA.area,
              tension: 0.4, fill: true, borderWidth: 2, pointRadius: 0, spanGaps: true },
            { label: nameB + ' — % Positive', data: raw.chart_timeline.topic_b,
              borderColor: CB.solid, backgroundColor: CB.area,
              tension: 0.4, fill: true, borderWidth: 2, pointRadius: 0, spanGaps: true },
          ]
        },
        options: {
          ...sharedOpts,
          interaction: { mode: 'index', intersect: false },
          plugins: { legend: { position: 'top' } },
          scales: { y: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } } }
        }
      });

      // ── Volatility ──────────────────────────────────────────
      new Chart(gc('chartVolatility'), {
        type: 'line',
        data: {
          labels: raw.chart_volatility.labels,
          datasets: [
            { label: nameA, data: raw.chart_volatility.topic_a,
              borderColor: CA.solid, backgroundColor: CA.area,
              tension: 0.4, fill: false, borderWidth: 2, pointRadius: 0, spanGaps: true },
            { label: nameB, data: raw.chart_volatility.topic_b,
              borderColor: CB.solid, backgroundColor: CB.area,
              tension: 0.4, fill: false, borderWidth: 2, pointRadius: 0, spanGaps: true },
          ]
        },
        options: {
          ...sharedOpts,
          interaction: { mode: 'index', intersect: false },
          plugins: { legend: { position: 'top' } },
          scales: { y: { beginAtZero: true } },
        }
      });

      // ── Momentum ────────────────────────────────────────────
      if (raw.chart_momentum) {
        new Chart(gc('chartMomentum'), {
          type: 'line',
          data: {
            labels: raw.chart_momentum.labels,
            datasets: [
              { label: nameA + ' — 7-day avg', data: raw.chart_momentum.topic_a,
                borderColor: CA.solid, backgroundColor: CA.area,
                tension: 0.4, fill: true, borderWidth: 2.5, pointRadius: 0, spanGaps: true },
              { label: nameB + ' — 7-day avg', data: raw.chart_momentum.topic_b,
                borderColor: CB.solid, backgroundColor: CB.area,
                tension: 0.4, fill: true, borderWidth: 2.5, pointRadius: 0, spanGaps: true },
            ]
          },
          options: {
            ...sharedOpts,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } } }
          }
        });
      }

      // ── Cumulative Volume ───────────────────────────────────
      if (raw.chart_cumulative) {
        new Chart(gc('chartCumulative'), {
          type: 'line',
          data: {
            labels: raw.chart_cumulative.labels,
            datasets: [
              { label: nameA, data: raw.chart_cumulative.topic_a,
                borderColor: CA.solid, backgroundColor: CA.area,
                tension: 0.3, fill: true, borderWidth: 2, pointRadius: 0, spanGaps: true },
              { label: nameB, data: raw.chart_cumulative.topic_b,
                borderColor: CB.solid, backgroundColor: CB.area,
                tension: 0.3, fill: true, borderWidth: 2, pointRadius: 0, spanGaps: true },
            ]
          },
          options: {
            ...sharedOpts,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true } }
          }
        });
      }

      // ── Keywords (4 mini charts) ────────────────────────────
      function renderKwChart(id, labels, values, color) {
        const el = gc(id);
        if (!el || !labels || !labels.length) return;
        new Chart(el, {
          type: 'bar',
          data: {
            labels,
            datasets: [{ label: 'Mentions', data: values,
              backgroundColor: color + 'aa', borderColor: color, borderWidth: 1, borderRadius: 3 }]
          },
          options: {
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true, ticks: { maxTicksLimit: 4 } } }
          }
        });
      }
      if (raw.keywords_a) {
        renderKwChart('chartKwAPos', raw.keywords_a.positive.labels, raw.keywords_a.positive.values, CA.solid);
        renderKwChart('chartKwANeg', raw.keywords_a.negative.labels, raw.keywords_a.negative.values, '#ef4444');
      }
      if (raw.keywords_b) {
        renderKwChart('chartKwBPos', raw.keywords_b.positive.labels, raw.keywords_b.positive.values, CB.solid);
        renderKwChart('chartKwBNeg', raw.keywords_b.negative.labels, raw.keywords_b.negative.values, '#ef4444');
      }

      // ── Emotion Distribution ────────────────────────────────
      if (raw.chart_emotions) {
        new Chart(gc('chartEmotionBar'), {
          type: 'bar',
          data: {
            labels: raw.chart_emotions.labels,
            datasets: [
              { label: nameA, data: raw.chart_emotions.topic_a,
                backgroundColor: CA.pos, borderColor: CA.solid, borderWidth: 1, borderRadius: 5 },
              { label: nameB, data: raw.chart_emotions.topic_b,
                backgroundColor: CB.pos, borderColor: CB.solid, borderWidth: 1, borderRadius: 5 },
            ]
          },
          options: {
            ...grouped,
            scales: { y: { beginAtZero: true, ticks: { callback: v => (v * 100).toFixed(0) + '%' } } },
            plugins: {
              legend: { position: 'top' },
              tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${(ctx.raw * 100).toFixed(1)}%` } }
            }
          }
        });

        new Chart(gc('chartEmotionRadar'), {
          type: 'radar',
          data: {
            labels: raw.chart_emotions.labels,
            datasets: [
              { label: nameA, data: raw.chart_emotions.topic_a,
                borderColor: CA.solid, backgroundColor: CA.area, borderWidth: 2, pointRadius: 4,
                pointBackgroundColor: CA.solid },
              { label: nameB, data: raw.chart_emotions.topic_b,
                borderColor: CB.solid, backgroundColor: CB.area, borderWidth: 2, pointRadius: 4,
                pointBackgroundColor: CB.solid },
            ]
          },
          options: {
            ...sharedOpts,
            plugins: { legend: { position: 'top' } },
            scales: {
              r: {
                beginAtZero: true,
                grid: { color: 'rgba(255,255,255,0.06)' },
                ticks: { display: false },
                pointLabels: { color: '#94a3b8', font: { size: 12 } }
              }
            }
          }
        });
      }

      // ── Text Length ─────────────────────────────────────────
      if (raw.chart_text_length) {
        new Chart(gc('chartTextLength'), {
          type: 'bar',
          data: {
            labels: raw.chart_text_length.labels,
            datasets: [
              { label: nameA, data: raw.chart_text_length.topic_a,
                backgroundColor: CA.pos, borderColor: CA.solid, borderWidth: 1, borderRadius: 5 },
              { label: nameB, data: raw.chart_text_length.topic_b,
                backgroundColor: CB.pos, borderColor: CB.solid, borderWidth: 1, borderRadius: 5 },
            ]
          },
          options: {
            ...grouped,
            scales: { y: { beginAtZero: true, ticks: { callback: v => v + ' chars' } } }
          }
        });
      }

      // ── Split Donut ─────────────────────────────────────────
      if (raw.chart_split) {
        const splitColors  = ['rgba(16,185,129,0.75)', 'rgba(239,68,68,0.75)'];
        const splitBorders = ['#10b981', '#ef4444'];
        new Chart(gc('chartSplitDonut'), {
          type: 'doughnut',
          data: {
            labels: raw.chart_split.labels,
            datasets: [
              { label: nameA, data: raw.chart_split.topic_a,
                backgroundColor: splitColors, borderColor: splitBorders,
                borderWidth: 1, circumference: 180, rotation: -90, weight: 1 },
              { label: nameB, data: raw.chart_split.topic_b,
                backgroundColor: splitColors.map(c => c.replace('0.75', '0.4')), borderColor: splitBorders,
                borderWidth: 1, circumference: 180, rotation: -90, weight: 1 },
            ]
          },
          options: {
            ...sharedOpts,
            plugins: {
              legend: { position: 'top' },
              tooltip: { callbacks: { label: ctx => `${ctx.dataset.label} — ${ctx.label}: ${ctx.raw}%` } }
            }
          }
        });
      }

      // ── Posting Hours ───────────────────────────────────────
      new Chart(gc('chartHours'), {
        type: 'bar',
        data: {
          labels: raw.chart_hours.labels,
          datasets: [
            { label: nameA, data: raw.chart_hours.topic_a,
              backgroundColor: CA.neg, borderColor: CA.solid, borderWidth: 1, borderRadius: 3 },
            { label: nameB, data: raw.chart_hours.topic_b,
              backgroundColor: CB.neg, borderColor: CB.solid, borderWidth: 1, borderRadius: 3 },
          ]
        },
        options: { ...grouped, scales: { y: { beginAtZero: true } } },
      });

      // ── Weekly Rhythm ───────────────────────────────────────
      new Chart(gc('chartWeekly'), {
        type: 'bar',
        data: {
          labels: raw.chart_weekly.labels,
          datasets: [
            { label: nameA, data: raw.chart_weekly.topic_a,
              backgroundColor: CA.pos, borderColor: CA.solid, borderWidth: 1, borderRadius: 4 },
            { label: nameB, data: raw.chart_weekly.topic_b,
              backgroundColor: CB.pos, borderColor: CB.solid, borderWidth: 1, borderRadius: 4 },
          ]
        },
        options: { ...grouped, scales: { y: { beginAtZero: true } } },
      });
    }

    // ── Tab switcher ─────────────────────────────────────────
    window.switchCompareTab = function(tabKey, btn) {
      document.querySelectorAll('.compare-tab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.querySelectorAll('.compare-tab-panel').forEach(p => p.style.display = 'none');
      document.getElementById('panel-' + tabKey).style.display = '';
      const prefixMap = { all: 'all_', reddit: 'reddit_', youtube: 'youtube_' };
      const rawMap    = { all: allRaw.overall, reddit: allRaw.reddit, youtube: allRaw.youtube };
      renderCompareTab(tabKey, prefixMap[tabKey], rawMap[tabKey]);
    };

    // ── P1: Render Overall tab immediately ───────────────────
    renderCompareTab('all', 'all_', allRaw.overall);

    // ── P2: Reddit in background ─────────────────────────────
    if (allRaw.reddit) {
      const rBtn = document.querySelector('.compare-tab[onclick*="reddit"]');
      if (rBtn) rBtn.classList.add('tab-loading');
      setTimeout(() => {
        renderCompareTab('reddit', 'reddit_', allRaw.reddit);
        if (rBtn) rBtn.classList.remove('tab-loading');
      }, 200);
    }

    // ── P3: YouTube in background ────────────────────────────
    if (allRaw.youtube) {
      const yBtn = document.querySelector('.compare-tab[onclick*="youtube"]');
      if (yBtn) yBtn.classList.add('tab-loading');
      setTimeout(() => {
        renderCompareTab('youtube', 'youtube_', allRaw.youtube);
        if (yBtn) yBtn.classList.remove('tab-loading');
      }, 400);
    }

  });
})();
