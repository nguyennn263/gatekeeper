(() => {
  'use strict';

  const state = {
    adminToken: '',
    testing: false,
    models: [],
    modelsLoaded: false,
    collapsedProviders: new Set(),
  };
  const $ = (id) => document.getElementById(id);
  const number = new Intl.NumberFormat('vi-VN');

  function setHidden(element, hidden) {
    element.hidden = hidden;
  }

  function showError(id, message) {
    const element = $(id);
    element.textContent = message;
    setHidden(element, false);
  }

  function clearError(id) {
    const element = $(id);
    element.textContent = '';
    setHidden(element, true);
  }

  function authHeaders() {
    const token = state.adminToken.trim();
    if (!token) throw new Error('Nhập admin token trước khi tiếp tục.');
    return { Authorization: `Bearer ${token}` };
  }

  function nextPaint() {
    return new Promise((resolve) => window.requestAnimationFrame(() => resolve()));
  }

  async function appendStreamFrame(text, eventCount) {
    $('test-output').append(document.createTextNode(text));
    $('test-output').scrollTop = $('test-output').scrollHeight;
    $('test-meta').textContent = `Đang nhận stream · ${number.format(eventCount)} event(s)`;
    await nextPaint();
  }

  async function apiJson(path) {
    let response;
    try {
      response = await fetch(path, { headers: authHeaders() });
    } catch (_error) {
      throw new Error('Không thể kết nối tới Gatekeeper.');
    }
    const text = await response.text();
    let payload = null;
    try { payload = text ? JSON.parse(text) : null; } catch (_error) { payload = null; }
    if (!response.ok) {
      throw new Error(payload?.error?.message || `Request failed (${response.status}).`);
    }
    return payload;
  }

  function totalTokens(item) {
    return (item.input_tokens || 0) + (item.output_tokens || 0) + (item.cache_tokens || 0);
  }

  function setMetric(id, value) {
    const element = $(id);
    const formatted = number.format(value || 0);
    element.textContent = formatted;
    element.title = formatted;
    element.setAttribute('aria-label', formatted);
  }

  function renderSummary(summary = {}) {
    setMetric('metric-requests', summary.requests);
    setMetric('metric-failed', summary.failed_requests);
    setMetric('metric-input', summary.input_tokens);
    setMetric('metric-output', summary.output_tokens);
    setMetric('metric-cache', summary.cache_tokens);
  }

  function formatBucket(seconds) {
    return new Intl.DateTimeFormat('vi-VN', {
      day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit',
    }).format(new Date(seconds * 1000));
  }

  function renderChart(series = []) {
    const chart = $('usage-chart');
    chart.replaceChildren();
    setHidden($('chart-empty'), series.length > 0);
    if (!series.length) {
      $('chart-empty').textContent = 'Chưa có token usage do provider báo cáo trong khoảng này.';
      return;
    }
    const maximum = Math.max(...series.map(totalTokens), 1);
    series.forEach((point) => {
      const column = document.createElement('div');
      column.className = 'chart-column';
      column.tabIndex = 0;
      const total = totalTokens(point);
      const stack = document.createElement('div');
      stack.className = 'chart-stack';
      stack.style.height = `${Math.max((total / maximum) * 100, total ? 2 : 0)}%`;
      for (const [key, className] of [
        ['cache_tokens', 'cache'], ['output_tokens', 'output'], ['input_tokens', 'input'],
      ]) {
        const segment = document.createElement('span');
        segment.className = `chart-segment ${className}`;
        segment.style.flexGrow = String(point[key] || 0);
        if (!(point[key] || 0)) segment.style.display = 'none';
        stack.append(segment);
      }
      const label = `${formatBucket(point.bucket_start)} · ${number.format(point.requests || 0)} requests · ${number.format(point.failed_requests || 0)} provider failures · ${number.format(point.input_tokens || 0)} input · ${number.format(point.output_tokens || 0)} output · ${number.format(point.cache_tokens || 0)} cache`;
      column.title = label;
      column.setAttribute('aria-label', label);
      column.append(stack);
      chart.append(column);
    });
  }

  function actionButton(label, model, stream) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = stream ? 'secondary-button compact-button' : 'text-button compact-button';
    button.textContent = label;
    button.addEventListener('click', () => testModel(model, stream, button));
    return button;
  }

  function deleteUsageButton(model) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'danger-button compact-button';
    button.textContent = 'Xóa usage';
    button.addEventListener('click', () => deleteArchivedModel(model, button));
    return button;
  }

  async function deleteArchivedModel(model, button) {
    const confirmed = window.confirm(
      `Xóa toàn bộ thống kê đã lưu của ${model}? Hành động này không thể hoàn tác.`,
    );
    if (!confirmed) return;
    clearError('model-action-error');
    state.adminToken = $('admin-token').value;
    button.disabled = true;
    button.textContent = 'Đang xóa…';
    try {
      const response = await fetch(`/admin/api/stats/models?model=${encodeURIComponent(model)}`, {
        method: 'DELETE',
        headers: authHeaders(),
      });
      if (!response.ok) {
        const text = await response.text();
        let message = text;
        try { message = JSON.parse(text)?.error?.message || text; } catch (_error) { /* raw body */ }
        throw new Error(message || `Delete failed (${response.status}).`);
      }
      await loadDashboard();
    } catch (error) {
      button.disabled = false;
      button.textContent = 'Xóa usage';
      showError('model-action-error', error.message);
    }
  }

  function updateProviderFilter() {
    const select = $('provider-filter');
    const selected = select.value;
    const providers = [...new Set(state.models.map((model) => model.provider).filter(Boolean))]
      .sort((left, right) => left.localeCompare(right));
    select.replaceChildren();
    const all = document.createElement('option');
    all.value = '';
    all.textContent = 'Tất cả providers';
    select.append(all);
    providers.forEach((provider) => {
      const option = document.createElement('option');
      option.value = provider;
      option.textContent = provider;
      select.append(option);
    });
    if (providers.includes(selected)) select.value = selected;
  }

  function setModels(models) {
    state.models = Array.isArray(models) ? models : [];
    state.modelsLoaded = true;
    updateProviderFilter();
    renderModels();
  }

  function filteredModels() {
    const query = $('model-search').value.trim().toLocaleLowerCase();
    const provider = $('provider-filter').value;
    return state.models.filter((model) => {
      if (provider && model.provider !== provider) return false;
      if (!query) return true;
      return `${model.model || ''} ${model.provider || ''}`.toLocaleLowerCase().includes(query);
    });
  }

  function renderModels() {
    const body = $('model-stats-body');
    body.replaceChildren();
    const models = filteredModels();
    $('model-filter-count').textContent = state.modelsLoaded
      ? `${number.format(models.length)} / ${number.format(state.models.length)} models`
      : '';
    setHidden($('models-empty'), models.length > 0);
    if (!models.length) {
      $('models-empty').textContent = !state.modelsLoaded
        ? 'Load dashboard để xem models.'
        : state.models.length
          ? 'Không tìm thấy model phù hợp với bộ lọc.'
          : 'Chưa có model nào được lưu.';
      return;
    }
    const groups = new Map();
    models.forEach((model) => {
      const provider = model.provider || 'Unknown provider';
      if (!groups.has(provider)) groups.set(provider, []);
      groups.get(provider).push(model);
    });
    [...groups.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .forEach(([providerName, providerModels]) => {
        const searchActive = Boolean($('model-search').value.trim());
        const collapsed = state.collapsedProviders.has(providerName) && !searchActive;
        const heading = document.createElement('tr');
        heading.className = 'provider-group-row';
        const headingCell = document.createElement('td');
        headingCell.colSpan = 7;
        const toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.className = 'provider-group-toggle';
        toggle.setAttribute('aria-expanded', String(!collapsed));
        toggle.addEventListener('click', () => {
          if (state.collapsedProviders.has(providerName)) {
            state.collapsedProviders.delete(providerName);
          } else {
            state.collapsedProviders.add(providerName);
          }
          renderModels();
        });
        const chevron = document.createElement('span');
        chevron.className = 'provider-chevron';
        chevron.textContent = collapsed ? '›' : '⌄';
        chevron.setAttribute('aria-hidden', 'true');
        const providerLabel = document.createElement('strong');
        providerLabel.textContent = providerName;
        const modelCount = document.createElement('span');
        const removedCount = providerModels.filter((model) => model.active === false).length;
        const disabledCount = providerModels.filter((model) => model.active !== false && model.enabled === false).length;
        modelCount.textContent = removedCount
          ? `${number.format(providerModels.length)} model(s) · ${number.format(removedCount)} đã xóa khỏi registry`
          : disabledCount
            ? `${number.format(providerModels.length)} model(s) · provider disabled`
          : `${number.format(providerModels.length)} model(s)`;
        toggle.append(chevron, providerLabel, modelCount);
        headingCell.append(toggle);
        heading.append(headingCell);
        body.append(heading);

        if (collapsed) return;

        providerModels.forEach((model) => {
          const row = document.createElement('tr');
          const modelCell = document.createElement('td');
          const id = document.createElement('strong');
          id.className = 'model-id';
          id.textContent = model.model;
          const provider = document.createElement('span');
          provider.className = 'model-name';
          provider.textContent = model.provider;
          modelCell.append(id, provider);
          if (model.active === false) {
            const archived = document.createElement('span');
            archived.className = 'archived-badge';
            archived.textContent = 'Đã xóa khỏi registry';
            modelCell.append(archived);
          } else if (model.enabled === false) {
            const disabled = document.createElement('span');
            disabled.className = 'disabled-badge';
            disabled.textContent = 'Provider disabled';
            modelCell.append(disabled);
          }
          const values = ['requests', 'failed_requests', 'input_tokens', 'output_tokens', 'cache_tokens'].map((key) => {
            const cell = document.createElement('td');
            cell.className = 'numeric';
            cell.textContent = number.format(model[key] || 0);
            return cell;
          });
          const actions = document.createElement('td');
          actions.className = 'test-actions';
          if (model.active === false) {
            actions.append(deleteUsageButton(model.model));
          } else if (model.enabled === false) {
            const unavailable = document.createElement('span');
            unavailable.className = 'muted';
            unavailable.textContent = 'Disabled';
            actions.append(unavailable);
          } else {
            actions.append(actionButton('Test', model.model, false), actionButton('Stream', model.model, true));
          }
          row.append(modelCell, ...values, actions);
          body.append(row);
        });
      });
  }

  async function loadDashboard() {
    const button = $('load-dashboard');
    clearError('dashboard-error');
    state.adminToken = $('admin-token').value;
    button.disabled = true;
    button.textContent = 'Loading…';
    try {
      const range = encodeURIComponent($('stats-range').value);
      const payload = await apiJson(`/admin/api/stats?range=${range}`);
      renderSummary(payload.summary);
      renderChart(payload.series);
      setModels(payload.models);
      $('updated-at').textContent = `Cập nhật ${new Date(payload.generated_at * 1000).toLocaleString('vi-VN')}`;
    } catch (error) {
      showError('dashboard-error', error.message);
    } finally {
      button.disabled = false;
      button.textContent = 'Load dashboard';
    }
  }

  async function testModel(model, stream, button) {
    if (state.testing) return;
    clearError('test-error');
    state.adminToken = $('admin-token').value;
    setHidden($('test-panel'), false);
    $('test-title').textContent = `${stream ? 'Stream test' : 'Model test'} · ${model}`;
    $('test-meta').textContent = 'Đang kết nối…';
    $('test-output').textContent = '';
    $('test-panel').scrollIntoView({ behavior: 'smooth', block: 'start' });
    state.testing = true;
    const oldLabel = button.textContent;
    button.disabled = true;
    button.textContent = 'Testing…';
    const started = performance.now();
    try {
      const response = await fetch('/admin/api/models/test', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ model, stream }),
      });
      const firstByte = response.headers.get('x-northgate-test-latency-ms');
      if (!response.ok) {
        const text = await response.text();
        let message = text;
        try { message = JSON.parse(text)?.error?.message || text; } catch (_error) { /* raw body */ }
        throw new Error(message || `Test failed (${response.status}).`);
      }
      if (stream && response.body) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let pending = '';
        let eventCount = 0;
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          pending += decoder.decode(value, { stream: true });
          while (true) {
            const delimiter = pending.match(/\r?\n\r?\n/);
            if (!delimiter || delimiter.index === undefined) break;
            const end = delimiter.index + delimiter[0].length;
            const frame = pending.slice(0, end);
            pending = pending.slice(end);
            eventCount += 1;
            await appendStreamFrame(frame, eventCount);
          }
        }
        pending += decoder.decode();
        if (pending) await appendStreamFrame(pending, eventCount + 1);
      } else {
        const text = await response.text();
        try { $('test-output').textContent = JSON.stringify(JSON.parse(text), null, 2); }
        catch (_error) { $('test-output').textContent = text; }
      }
      const total = Math.round(performance.now() - started);
      $('test-meta').textContent = `Hoạt động · upstream headers ${firstByte || '—'} ms · hoàn tất ${total} ms`;
      await loadDashboard();
    } catch (error) {
      $('test-meta').textContent = 'Test thất bại';
      showError('test-error', error.message);
    } finally {
      state.testing = false;
      button.disabled = false;
      button.textContent = oldLabel;
    }
  }

  $('admin-token').addEventListener('input', () => { state.adminToken = $('admin-token').value; });
  $('toggle-admin-token').addEventListener('click', () => {
    const input = $('admin-token');
    const visible = input.type === 'text';
    input.type = visible ? 'password' : 'text';
    $('toggle-admin-token').textContent = visible ? 'Show' : 'Hide';
    $('toggle-admin-token').setAttribute('aria-pressed', String(!visible));
  });
  $('load-dashboard').addEventListener('click', loadDashboard);
  $('stats-range').addEventListener('change', () => {
    if (state.adminToken.trim()) loadDashboard();
  });
  $('model-search').addEventListener('input', renderModels);
  $('provider-filter').addEventListener('change', renderModels);
  $('clear-test').addEventListener('click', () => {
    $('test-output').textContent = '';
    clearError('test-error');
    setHidden($('test-panel'), true);
  });
  window.addEventListener('pagehide', () => {
    state.adminToken = '';
    $('admin-token').value = '';
    $('test-output').textContent = '';
  });
})();
