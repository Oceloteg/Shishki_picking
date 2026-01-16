// Frontend (vanilla JS)

const EPS = 1e-9;
const POLL_INTERVAL_MS = 30_000;

const state = {
  token: null,
  config: null,
  orders: [], // array of {order, lines}
  orderDetail: null, // {order, lines}
  activeOrderId: null,
  pollTimer: null,
};

// ==== DOM refs ====
const loginView = document.getElementById('loginView');
const appView = document.getElementById('appView');
const orderView = document.getElementById('orderView');

const passwordInput = document.getElementById('passwordInput');
const loginButton = document.getElementById('loginButton');
const loginError = document.getElementById('loginError');

const refreshButton = document.getElementById('refreshButton');
const logoutButton = document.getElementById('logoutButton');

const boardEl = document.getElementById('board');

const backButton = document.getElementById('backButton');
const orderTopInfo = document.getElementById('orderTopInfo');
const orderCardInfo = document.getElementById('orderCardInfo');
const orderComment = document.getElementById('orderComment');
const orderLinesEl = document.getElementById('orderLines');
const completeOverlay = document.getElementById('completeOverlay');

// ==== helpers ====
function show(el) {
  el.classList.remove('hidden');
}
function hide(el) {
  el.classList.add('hidden');
}

function norm(s) {
  return String(s || '').trim().toLowerCase();
}

function fmtDateOnly(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  if (Number.isNaN(d.getTime())) return '';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

function fmtQty(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '0';
  // If it's essentially an integer
  if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
  // Otherwise show up to 3 decimals, trim trailing zeros
  return n.toFixed(3).replace(/0+$/, '').replace(/\.$/, '');
}

function stepDecimals(step) {
  const s = String(step);
  if (s.includes('e') || s.includes('E')) {
    const exp = parseInt(s.split(/e/i)[1], 10);
    return exp < 0 ? -exp : 0;
  }
  if (!s.includes('.')) return 0;
  return s.split('.')[1].length;
}

function inferStep(qtyOrdered) {
  const q = Number(qtyOrdered);
  if (!Number.isFinite(q) || q <= 0) return 1;
  // Integer-like => step 1
  if (Math.abs(q - Math.round(q)) < 1e-9) return 1;

  const s = String(q);
  if (s.includes('e') || s.includes('E')) {
    const exp = parseInt(s.split(/e/i)[1], 10);
    if (Number.isFinite(exp) && exp < 0) return Math.pow(10, exp);
    return 1;
  }
  if (!s.includes('.')) return 1;
  const decimals = s.split('.')[1].length;
  return Math.pow(10, -decimals);
}

function clampAndRound(value, max, step) {
  const v = Math.max(0, Math.min(Number(value), Number(max)));
  const dec = stepDecimals(step);
  // Round to step decimals to avoid 0.30000000004
  const rounded = Number(v.toFixed(dec));
  // Avoid -0
  return rounded === 0 ? 0 : rounded;
}

function isLineDone(line) {
  const ordered = Number(line.qty_ordered || 0);
  const collected = Number(line.qty_collected || 0);
  if (ordered <= EPS) return true;
  return collected >= ordered - 1e-6;
}

function isOrderDone(order) {
  const total = Number(order.total_qty || 0);
  const collected = Number(order.collected_qty || 0);
  if (total <= EPS) return false;
  return collected >= total - 1e-6;
}

// ==== API ====
async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.token) headers.set('Authorization', `Bearer ${state.token}`);
  const opts = { ...options, headers };
  const res = await fetch(path, opts);
  if (res.status === 401) {
    // Token invalid/expired
    logout();
    throw new Error('Unauthorized');
  }
  return res;
}

async function loadConfig() {
  const res = await apiFetch('/api/config');
  state.config = await res.json();
}

async function login(password) {
  const res = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || 'Login failed');
  }

  const data = await res.json();
  state.token = data.token;
}

async function syncNow() {
  const res = await apiFetch('/api/sync-now', { method: 'POST' });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function loadOrders() {
  const res = await apiFetch('/api/orders');
  if (!res.ok) throw new Error(await res.text());
  state.orders = await res.json();
  renderBoard();
}

async function loadOrderDetail(orderId) {
  const res = await apiFetch(`/api/orders/${orderId}`);
  if (!res.ok) throw new Error(await res.text());
  state.orderDetail = await res.json();
  state.activeOrderId = orderId;
  renderOrderDetail();
}

async function patchLine(lineId, qty) {
  const res = await apiFetch(`/api/lines/${lineId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ qty_collected: qty }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || 'Failed to update');
  }

  const data = await res.json();

  // Update local state
  if (state.orderDetail) {
    state.orderDetail.order = data.order;
    const idx = state.orderDetail.lines.findIndex((l) => l.id === data.line.id);
    if (idx >= 0) {
      state.orderDetail.lines[idx] = data.line;
    }
  }

  // Also update the board cache for progress previews
  const wIdx = state.orders.findIndex((w) => w.order && w.order.id === data.order.id);
  if (wIdx >= 0) {
    state.orders[wIdx].order = data.order;
    // Update corresponding preview line if it exists
    const lIdx = state.orders[wIdx].lines.findIndex((l) => l.id === data.line.id);
    if (lIdx >= 0) state.orders[wIdx].lines[lIdx] = data.line;
  }

  // Update UI incrementally
  updateOrderDetailHeader(data.order);
  updateOrderCardOnBoard(data.order);
  updateLineUI(data.line);

  if (data.order_done) {
    showComplete();
    // Close order view shortly after
    setTimeout(async () => {
      try {
        closeOrderView();
        await loadOrders();
      } catch (_) {
        // ignore
      }
    }, 700);
  }

  return data;
}

// ==== polling ====
function startPolling() {
  stopPolling();
  state.pollTimer = setInterval(async () => {
    if (!state.activeOrderId) {
      try {
        await loadOrders();
      } catch (_) {
        // ignore periodic errors
      }
    }
  }, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
}

// ==== board ====
function getOrderColumn(order) {
  const cfg = state.config || {};
  const s = norm(order.onec_status);

  if (s && norm(cfg.onec_status_picked) && s === norm(cfg.onec_status_picked)) return 'picked';
  if (s && norm(cfg.onec_status_picking) && s === norm(cfg.onec_status_picking)) return 'picking';

  // Fallback by progress
  if (isOrderDone(order)) return 'picked';
  if (Number(order.collected_qty || 0) > EPS) return 'picking';
  return 'not_started';
}

function renderBoard() {
  boardEl.innerHTML = '';

  const columns = [
    { key: 'not_started', title: 'Новые' },
    { key: 'picking', title: 'В сборке' },
    { key: 'picked', title: 'Собраны' },
  ];

  const colEls = {};

  for (const col of columns) {
    const colEl = document.createElement('section');
    colEl.className = 'column';
    colEl.dataset.column = col.key;

    const header = document.createElement('div');
    header.className = 'column-header';
    header.innerHTML = `<div class="column-title">${col.title}</div><div class="column-count" id="count_${col.key}"></div>`;

    const list = document.createElement('div');
    list.className = 'column-list';

    colEl.appendChild(header);
    colEl.appendChild(list);
    boardEl.appendChild(colEl);

    colEls[col.key] = list;
  }

  const counts = { not_started: 0, picking: 0, picked: 0 };

  for (const w of state.orders) {
    const col = getOrderColumn(w.order);
    counts[col] += 1;
    colEls[col].appendChild(renderOrderCard(w));
  }

  for (const k of Object.keys(counts)) {
    const el = document.getElementById(`count_${k}`);
    if (el) el.textContent = counts[k] ? String(counts[k]) : '';
  }
}

function renderOrderCard(w) {
  const o = w.order;
  const card = document.createElement('div');
  card.className = 'card';
  card.dataset.orderId = o.id;

  const title = document.createElement('div');
  title.className = 'card-title';
  title.textContent = `Заказ ${o.number || o.id}`;

  const customer = document.createElement('div');
  customer.className = 'card-sub';
  customer.textContent = o.customer_name || '';

  const meta = document.createElement('div');
  meta.className = 'card-meta';

  const tags = [];
  if (o.urgency_text) tags.push(o.urgency_text);
  if (o.ship_deadline) tags.push(`Отгрузка до ${fmtDateOnly(o.ship_deadline)}`);

  meta.textContent = tags.join(' · ');

  const progressRow = document.createElement('div');
  progressRow.className = 'progress-row';

  const progressText = document.createElement('div');
  progressText.className = 'progress-text';
  progressText.textContent = `${fmtQty(o.collected_qty)} / ${fmtQty(o.total_qty)} (${Math.round(o.progress_pct || 0)}%)`;

  const bar = document.createElement('div');
  bar.className = 'progress-bar';
  const fill = document.createElement('div');
  fill.className = 'progress-fill';
  fill.style.width = `${Math.max(0, Math.min(100, o.progress_pct || 0))}%`;
  bar.appendChild(fill);

  progressRow.appendChild(progressText);
  progressRow.appendChild(bar);

  const linesPreview = document.createElement('div');
  linesPreview.className = 'lines-preview';

  const previewLines = (w.lines || []).slice(0, 5);
  for (const l of previewLines) {
    const row = document.createElement('div');
    row.className = 'line-preview-row';
    row.textContent = `${l.item_name} — ${fmtQty(l.qty_collected)} / ${fmtQty(l.qty_ordered)}${l.unit ? ' ' + l.unit : ''}`;
    linesPreview.appendChild(row);
  }

  card.appendChild(title);
  card.appendChild(customer);
  if (tags.length) card.appendChild(meta);
  card.appendChild(progressRow);
  if (previewLines.length) card.appendChild(linesPreview);

  card.addEventListener('click', async () => {
    await openOrder(o.id);
  });

  return card;
}

function updateOrderCardOnBoard(order) {
  const card = document.querySelector(`.card[data-order-id="${order.id}"]`);
  if (!card) return;

  // Update progress text and bar
  const progressText = card.querySelector('.progress-text');
  const fill = card.querySelector('.progress-fill');
  if (progressText) {
    progressText.textContent = `${fmtQty(order.collected_qty)} / ${fmtQty(order.total_qty)} (${Math.round(order.progress_pct || 0)}%)`;
  }
  if (fill) {
    fill.style.width = `${Math.max(0, Math.min(100, order.progress_pct || 0))}%`;
  }

  // Update meta tags
  const meta = card.querySelector('.card-meta');
  const tags = [];
  if (order.urgency_text) tags.push(order.urgency_text);
  if (order.ship_deadline) tags.push(`Отгрузка до ${fmtDateOnly(order.ship_deadline)}`);
  if (meta) {
    meta.textContent = tags.join(' · ');
    if (!tags.length) meta.remove();
  } else if (tags.length) {
    // Insert after customer
    const customer = card.querySelector('.card-sub');
    if (customer) {
      const m = document.createElement('div');
      m.className = 'card-meta';
      m.textContent = tags.join(' · ');
      customer.after(m);
    }
  }
}

// ==== order view ====
async function openOrder(orderId) {
  stopPolling();

  // Mark as "picking" (async) – does not block the UI.
  apiFetch(`/api/orders/${orderId}/open`, { method: 'POST' }).catch(() => {});

  await loadOrderDetail(orderId);

  hide(appView);
  show(orderView);
  orderView.setAttribute('aria-hidden', 'false');
}

function closeOrderView() {
  state.activeOrderId = null;
  state.orderDetail = null;
  hide(orderView);
  orderView.setAttribute('aria-hidden', 'true');
  show(appView);
  hideComplete();
  startPolling();
}

function showComplete() {
  show(completeOverlay);
  completeOverlay.setAttribute('aria-hidden', 'false');
}
function hideComplete() {
  hide(completeOverlay);
  completeOverlay.setAttribute('aria-hidden', 'true');
}

function updateOrderDetailHeader(order) {
  const titleBits = [];
  titleBits.push(`Заказ ${order.number || order.id}`);
  if (order.urgency_text) titleBits.push(order.urgency_text);
  orderTopInfo.textContent = titleBits.join(' · ');

  const infoLines = [];
  if (order.customer_name) infoLines.push(`<div class="od-customer">${escapeHtml(order.customer_name)}</div>`);

  infoLines.push(
    `<div class="od-progress"><div class="od-progress-text">${fmtQty(order.collected_qty)} / ${fmtQty(order.total_qty)} (${Math.round(order.progress_pct || 0)}%)</div>` +
      `<div class="progress-bar"><div class="progress-fill" style="width:${Math.max(0, Math.min(100, order.progress_pct || 0))}%"></div></div></div>`
  );

  if (order.ship_deadline) {
    infoLines.push(`<div class="od-deadline">Отгрузка до ${escapeHtml(fmtDateOnly(order.ship_deadline))}</div>`);
  }

  orderCardInfo.innerHTML = infoLines.join('');

  if (order.comment) {
    orderComment.textContent = order.comment;
    show(orderComment);
  } else {
    hide(orderComment);
    orderComment.textContent = '';
  }
}

function renderOrderDetail() {
  const detail = state.orderDetail;
  if (!detail) return;

  const order = detail.order;
  const lines = detail.lines || [];

  updateOrderDetailHeader(order);

  // Render lines:
  // 1) incomplete (not removed)
  // 2) complete (not removed)
  // 3) removed (history)
  const incomplete = [];
  const complete = [];
  const removed = [];

  for (const l of lines) {
    if (l.is_removed) {
      removed.push(l);
    } else if (isLineDone(l)) {
      complete.push(l);
    } else {
      incomplete.push(l);
    }
  }

  orderLinesEl.innerHTML = '';

  for (const l of [...incomplete, ...complete, ...removed]) {
    orderLinesEl.appendChild(renderLine(l));
  }

  hideComplete();
}

function renderLine(line) {
  const done = isLineDone(line);

  const el = document.createElement('div');
  el.className = 'line';
  el.dataset.lineId = line.id;
  el.dataset.done = done ? '1' : '0';

  const delta = computeDelta(line);
  const qtyChanged = delta !== null;

  if (line.is_removed) el.classList.add('line--removed');
  if (line.is_added && !done && !line.is_removed) el.classList.add('line--added');
  if (qtyChanged && !done && !line.is_removed) el.classList.add('line--qty-changed');

  const header = document.createElement('div');
  header.className = 'line-header';

  const title = document.createElement('div');
  title.className = 'line-title';
  title.textContent = line.item_name;

  const meta = document.createElement('div');
  meta.className = 'line-meta';

  const unit = line.unit ? ` ${line.unit}` : '';

  let needHtml = `<span class="qty-ordered">${escapeHtml(fmtQty(line.qty_ordered))}</span>${escapeHtml(unit)}`;
  if (qtyChanged) {
    const sign = delta > 0 ? '+' : '';
    needHtml += ` <span class="line-delta">(${sign}${escapeHtml(fmtQty(delta))})</span>`;
  }

  meta.innerHTML = `Нужно: ${needHtml}`;

  header.appendChild(title);
  header.appendChild(meta);

  const progress = document.createElement('div');
  progress.className = 'line-progress';
  progress.textContent = `Собрано: ${fmtQty(line.qty_collected)} / ${fmtQty(line.qty_ordered)}`;

  el.appendChild(header);
  el.appendChild(progress);

  if (!line.is_removed) {
    const controls = document.createElement('div');
    controls.className = 'line-controls';

    const step = inferStep(line.qty_ordered);

    const minus = document.createElement('button');
    minus.className = 'btn btn-ghost';
    minus.textContent = '−';

    const value = document.createElement('div');
    value.className = 'qty-value';
    value.textContent = fmtQty(line.qty_collected);

    const plus = document.createElement('button');
    plus.className = 'btn btn-ghost';
    plus.textContent = '+';

    minus.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      try {
        const cur = Number(getLineQtyFromState(line.id));
        const next = clampAndRound(cur - step, line.qty_ordered, step);
        await patchLine(line.id, next);
      } catch (e) {
        // ignore
      }
    });

    plus.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      try {
        const cur = Number(getLineQtyFromState(line.id));
        const next = clampAndRound(cur + step, line.qty_ordered, step);
        await patchLine(line.id, next);
      } catch (e) {
        // ignore
      }
    });

    controls.appendChild(minus);
    controls.appendChild(value);
    controls.appendChild(plus);

    el.appendChild(controls);
  }

  return el;
}

function computeDelta(line) {
  // Returns qty_ordered - baseline_qty_ordered (or null if no baseline / no change)
  if (line.baseline_qty_ordered === null || line.baseline_qty_ordered === undefined) return null;
  if (line.is_added) return Number(line.qty_ordered || 0);

  const base = Number(line.baseline_qty_ordered);
  const cur = Number(line.qty_ordered || 0);
  if (!Number.isFinite(base) || !Number.isFinite(cur)) return null;
  if (Math.abs(base - cur) < 1e-6) return null;
  return cur - base;
}

function getLineQtyFromState(lineId) {
  if (!state.orderDetail) return 0;
  const l = state.orderDetail.lines.find((x) => x.id === lineId);
  return l ? Number(l.qty_collected || 0) : 0;
}

function updateLineUI(line) {
  const el = document.querySelector(`.line[data-line-id="${line.id}"]`);
  if (!el) return;

  // Update qty display
  const qtyValEl = el.querySelector('.qty-value');
  if (qtyValEl) qtyValEl.textContent = fmtQty(line.qty_collected);

  const progressEl = el.querySelector('.line-progress');
  if (progressEl) {
    progressEl.textContent = `Собрано: ${fmtQty(line.qty_collected)} / ${fmtQty(line.qty_ordered)}`;
  }

  const wasDone = el.dataset.done === '1';
  const nowDone = isLineDone(line);
  el.dataset.done = nowDone ? '1' : '0';

  // Update highlight classes
  el.classList.toggle('line--removed', !!line.is_removed);
  el.classList.toggle('line--added', !!line.is_added && !nowDone && !line.is_removed);

  const delta = computeDelta(line);
  const qtyChanged = delta !== null;
  el.classList.toggle('line--qty-changed', qtyChanged && !nowDone && !line.is_removed);

  const meta = el.querySelector('.line-meta');
  if (meta) {
    const unit = line.unit ? ` ${line.unit}` : '';
    let needHtml = `<span class="qty-ordered">${escapeHtml(fmtQty(line.qty_ordered))}</span>${escapeHtml(unit)}`;
    if (qtyChanged) {
      const sign = delta > 0 ? '+' : '';
      needHtml += ` <span class="line-delta">(${sign}${escapeHtml(fmtQty(delta))})</span>`;
    }
    meta.innerHTML = `Нужно: ${needHtml}`;
  }

  // Reorder only on first transition to DONE
  if (!wasDone && nowDone) {
    moveLineToBottom(el);
  }
}

function moveLineToBottom(lineEl) {
  if (!orderLinesEl) return;

  // Insert before the first removed line, if any, else append to end.
  const removedEls = orderLinesEl.querySelectorAll('.line--removed');
  if (removedEls.length > 0) {
    orderLinesEl.insertBefore(lineEl, removedEls[0]);
  } else {
    orderLinesEl.appendChild(lineEl);
  }
}

function escapeHtml(str) {
  return String(str)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

// ==== auth / lifecycle ====
function logout() {
  state.token = null;
  state.config = null;
  state.orders = [];
  state.orderDetail = null;
  state.activeOrderId = null;
  stopPolling();

  hide(appView);
  hide(orderView);
  show(loginView);

  passwordInput.value = '';
  loginError.textContent = '';
  hide(loginError);
}

async function bootApp() {
  try {
    await loadConfig();
    show(appView);
    hide(loginView);

    await loadOrders();
    startPolling();

    // Fire-and-forget background sync from 1C (no UI blocking)
    syncNow()
      .then(loadOrders)
      .catch(() => {});
  } catch (e) {
    // If config failed (token invalid), force logout
    logout();
  }
}

// ==== event bindings ====
loginButton.addEventListener('click', async () => {
  const pwd = passwordInput.value;
  loginButton.disabled = true;
  try {
    await login(pwd);
    hide(loginError);
    await bootApp();
  } catch (e) {
    loginError.textContent = 'Неверный пароль';
    show(loginError);
  } finally {
    loginButton.disabled = false;
  }
});

passwordInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') loginButton.click();
});

logoutButton.addEventListener('click', () => {
  logout();
});

refreshButton.addEventListener('click', async () => {
  refreshButton.disabled = true;
  try {
    await syncNow();
    await loadOrders();
  } catch (_) {
    // ignore
  } finally {
    refreshButton.disabled = false;
  }
});

backButton.addEventListener('click', () => {
  closeOrderView();
});

// start in login state
logout();
