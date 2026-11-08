(() => {
  'use strict';

  const state = {
    adminToken: '',
    providerKey: '',
    discoveredModels: [],
    discoveredConfig: null,
    selectedIds: new Set(),
    enrichGeneration: 0,
    enrichTimer: null,
    addingProvider: null,
    addableModels: [],
    addSelectedIds: new Set(),
  };

  const $ = (id) => document.getElementById(id);
  const discoverForm = $('discover-form');
  const saveForm = $('save-form');
  const adminTokenInput = $('admin-token');
  const providerKeyInput = $('provider-key');
  const baseUrlInput = $('base-url');
  const modelsPanel = $('models-panel');
  const savePanel = $('save-panel');
  const modelsBody = $('models-body');
  const selectAll = $('select-all');
  const selectionCount = $('selection-count');
  const inventoryList = $('inventory-list');
  const addModelsPanel = $('add-models-panel');
  const addModelsBody = $('add-models-body');

  function setHidden(element, hidden) {
    element.hidden = hidden;
  }

  function setStatus(id, message) {
    $(id).textContent = message;
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

  function setBusy(button, busy, label, busyLabel) {
    button.disabled = busy;
    button.textContent = busy ? busyLabel : label;
  }

  function authHeaders() {
    const token = state.adminToken.trim();
    if (!token) {
      throw new Error('Nhập admin token trước khi tiếp tục.');
    }
    return { Authorization: `Bearer ${token}` };
  }

  async function apiRequest(path, options = {}) {
    const headers = new Headers(options.headers || {});
    Object.entries(authHeaders()).forEach(([name, value]) => headers.set(name, value));
    if (options.body !== undefined) headers.set('Content-Type', 'application/json');

    let response;
    try {
      response = await fetch(path, { ...options, headers });
    } catch (error) {
      throw new Error('Không thể kết nối tới Gatekeeper. Kiểm tra URL và thử lại.');
    }

    const text = await response.text();
    let payload = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (_error) {
        payload = text;
      }
    }

    if (!response.ok) {
      const serverMessage = payload && typeof payload === 'object'
        ? (payload.message || payload.error?.message || payload.error || payload.detail)
        : payload;
      throw new Error(serverMessage ? String(serverMessage) : `Request failed (${response.status}).`);
    }
    return payload;
  }

  function protocolLabel(protocol) {
    const labels = {
      openai: 'OpenAI-compatible',
      anthropic: 'Anthropic-compatible',
      responses: 'Responses · Codex-compatible tool calling',
    };
    return labels[protocol] || protocol;
  }

  function normalizeModels(payload) {
    const rawModels = Array.isArray(payload)
      ? payload
      : payload && Array.isArray(payload.models)
        ? payload.models
        : payload && Array.isArray(payload.data)
          ? payload.data
          : [];

    return rawModels
      .filter((model) => model && typeof model === 'object' && typeof model.id === 'string' && model.id.trim())
      .map((model) => ({
        id: model.id,
        display_name: typeof model.display_name === 'string' ? model.display_name : '',
        context_window: numberOrNull(model.context_window),
        max_output_tokens: numberOrNull(model.max_output_tokens),
        capabilities: model.capabilities && typeof model.capabilities === 'object' ? model.capabilities : {},
      }));
  }

  function numberOrNull(value) {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
  }

  function formatNumber(value) {
    if (value === null || value === undefined) return 'Unknown';
    return new Intl.NumberFormat().format(value);
  }

  function capabilityNames(capabilities) {
    return Object.entries(capabilities)
      .filter(([, value]) => value === true || (typeof value === 'string' && value.trim()))
      .map(([key, value]) => value === true ? key : `${key}: ${value}`);
  }

  function createCapabilityList(capabilities) {
    const wrapper = document.createElement('div');
    wrapper.className = 'capabilities';
    const names = capabilityNames(capabilities);
    if (!names.length) {
      const unknown = document.createElement('span');
      unknown.className = 'muted';
      unknown.textContent = 'Unknown';
      wrapper.append(unknown);
      return wrapper;
    }
    names.forEach((name) => {
      const badge = document.createElement('span');
      badge.className = 'capability';
      badge.textContent = name;
      wrapper.append(badge);
    });
    return wrapper;
  }

  function renderModels() {
    modelsBody.replaceChildren();
    state.discoveredModels.forEach((model) => {
      const row = document.createElement('tr');
      const checkCell = document.createElement('td');
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.className = 'model-checkbox';
      checkbox.value = model.id;
      checkbox.checked = state.selectedIds.has(model.id);
      checkbox.setAttribute('aria-label', `Select ${model.id}`);
      checkbox.addEventListener('change', () => {
        if (checkbox.checked) state.selectedIds.add(model.id);
        else state.selectedIds.delete(model.id);
        updateSelectionUi();
        scheduleEnrich();
      });
      checkCell.append(checkbox);

      const idCell = document.createElement('td');
      const id = document.createElement('span');
      id.className = 'model-id';
      id.textContent = model.id;
      idCell.append(id);
      if (model.display_name && model.display_name !== model.id) {
        const displayName = document.createElement('span');
        displayName.className = 'model-name';
        displayName.textContent = model.display_name;
        idCell.append(displayName);
      }

      const contextCell = document.createElement('td');
      contextCell.textContent = formatNumber(model.context_window);
      const outputCell = document.createElement('td');
      outputCell.textContent = formatNumber(model.max_output_tokens);
      const capabilitiesCell = document.createElement('td');
      capabilitiesCell.append(createCapabilityList(model.capabilities));

      row.append(checkCell, idCell, contextCell, outputCell, capabilitiesCell);
      modelsBody.append(row);
    });
    setHidden($('models-empty'), state.discoveredModels.length > 0);
    updateSelectionUi();
  }

  function updateSelectionUi() {
    const total = state.discoveredModels.length;
    const selected = state.selectedIds.size;
    selectionCount.textContent = `${selected} selected`;
    selectAll.checked = total > 0 && selected === total;
    selectAll.indeterminate = selected > 0 && selected < total;
    setHidden(savePanel, selected === 0);
    updateSteps(selected > 0 ? 3 : 2);
  }

  function updateSteps(activeStep) {
    document.querySelectorAll('[data-step-indicator]').forEach((item) => {
      const step = Number(item.dataset.stepIndicator);
      item.classList.toggle('active', step === activeStep);
      item.classList.toggle('complete', step < activeStep);
    });
  }

  function selectedModels() {
    return state.discoveredModels.filter((model) => state.selectedIds.has(model.id));
  }

  function clearDiscoveryState() {
    state.enrichGeneration += 1;
    if (state.enrichTimer !== null) window.clearTimeout(state.enrichTimer);
    state.enrichTimer = null;
    state.providerKey = '';
    state.discoveredConfig = null;
    state.discoveredModels = [];
    state.selectedIds = new Set();
    modelsBody.replaceChildren();
    setHidden(modelsPanel, true);
    setHidden(savePanel, true);
  }

  function scheduleEnrich() {
    state.enrichGeneration += 1;
    const generation = state.enrichGeneration;
    if (state.enrichTimer !== null) window.clearTimeout(state.enrichTimer);
    if (!state.selectedIds.size || !state.discoveredConfig || !state.providerKey) return;
    state.enrichTimer = window.setTimeout(() => enrichSelected(generation), 250);
  }

  async function enrichSelected(generation) {
    state.enrichTimer = null;
    setStatus('models-summary', 'Looking up selected model metadata…');
    clearError('model-error');
    try {
      const payload = await apiRequest('/admin/api/models/enrich', {
        method: 'POST',
        body: JSON.stringify({
          base_url: state.discoveredConfig.baseUrl,
          api_key: state.providerKey,
          protocol: state.discoveredConfig.protocol,
          model_ids: [...state.selectedIds],
        }),
      });
      if (generation !== state.enrichGeneration) return;
      const enriched = new Map(normalizeModels(payload).map((model) => [model.id, model]));
      state.discoveredModels = state.discoveredModels.map((model) => enriched.get(model.id) || model);
      renderModels();
      setStatus('models-summary', `${state.discoveredModels.length} model(s) found · selected metadata refreshed`);
    } catch (error) {
      if (generation !== state.enrichGeneration) return;
      showError('model-error', error.message);
      setStatus('models-summary', 'Metadata lookup failed; saving will validate again.');
    }
  }

  function validateUrl() {
    try {
      const url = new URL(baseUrlInput.value.trim());
      if (!['http:', 'https:'].includes(url.protocol)) throw new Error();
      return url.toString().replace(/\/$/, '');
    } catch (_error) {
      throw new Error('Nhập một Provider URL hợp lệ bắt đầu bằng http:// hoặc https://.');
    }
  }

  async function discover(event) {
    event.preventDefault();
    clearError('discover-error');
    clearError('model-error');
    setStatus('discover-status', '');
    let baseUrl;
    const providerKey = providerKeyInput.value.trim();
    if (!providerKey) {
      showError('discover-error', 'Nhập provider API key trước khi discover.');
      providerKeyInput.focus();
      return;
    }
    try {
      baseUrl = validateUrl();
      state.adminToken = adminTokenInput.value;
      clearDiscoveryState();
    } catch (error) {
      showError('discover-error', error.message);
      baseUrlInput.focus();
      return;
    }

    const button = $('discover-button');
    setBusy(button, true, 'Discover models', 'Discovering…');
    setStatus('discover-status', 'Đang tải model metadata…');
    try {
      const protocol = document.querySelector('input[name="protocol"]:checked').value;
      const payload = await apiRequest('/admin/api/discover', {
        method: 'POST',
        body: JSON.stringify({ base_url: baseUrl, api_key: providerKey, protocol }),
      });
      state.providerKey = providerKey;
      state.discoveredConfig = { baseUrl, protocol };
      state.discoveredModels = normalizeModels(payload);
      state.selectedIds = new Set();
      renderModels();
      setHidden(modelsPanel, false);
      setStatus('discover-status', `${state.discoveredModels.length} model(s) found.`);
      $('models-summary').textContent = `${state.discoveredModels.length} model(s) found · ${protocolLabel(protocol)}`;
      $('status-dot')?.classList.add('ready');
      modelsPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
      updateSteps(2);
    } catch (error) {
      setStatus('discover-status', '');
      showError('discover-error', error.message);
    } finally {
      setBusy(button, false, 'Discover models', 'Discovering…');
    }
  }

  async function saveProvider(event) {
    event.preventDefault();
    clearError('save-error');
    setStatus('save-status', '');
    const name = $('provider-name').value.trim();
    if (!name) {
      showError('save-error', 'Nhập provider name trước khi lưu.');
      $('provider-name').focus();
      return;
    }
    if (!state.discoveredConfig || !state.providerKey) {
      showError('save-error', 'Phiên discover đã hết dữ liệu trong memory. Hãy discover lại.');
      return;
    }
    const button = $('save-button');
    const fallbackApiKeys = [$('fallback-key-1').value.trim(), $('fallback-key-2').value.trim()]
      .filter(Boolean);
    setBusy(button, true, 'Save provider', 'Saving…');
    setStatus('save-status', 'Đang lưu provider…');
    try {
      await apiRequest('/admin/api/providers', {
        method: 'POST',
        body: JSON.stringify({
          name,
          base_url: state.discoveredConfig.baseUrl,
          api_key: state.providerKey,
          fallback_api_keys: fallbackApiKeys,
          protocol: state.discoveredConfig.protocol,
          models: selectedModels(),
        }),
      });
      state.providerKey = '';
      providerKeyInput.value = '';
      $('provider-name').value = '';
      $('fallback-key-1').value = '';
      $('fallback-key-2').value = '';
      setStatus('save-status', 'Provider saved.');
      await loadInventory();
    } catch (error) {
      setStatus('save-status', '');
      showError('save-error', error.message);
    } finally {
      setBusy(button, false, 'Save provider', 'Saving…');
    }
  }

  function normalizeProviders(payload) {
    if (Array.isArray(payload)) return payload;
    if (payload && Array.isArray(payload.providers)) return payload.providers;
    if (payload && Array.isArray(payload.data)) return payload.data;
    return [];
  }

  function providerModelIds(provider) {
    if (!provider || !Array.isArray(provider.models)) return [];
    return provider.models
      .map((model) => typeof model === 'string' ? model : model && (model.public_id || model.id))
      .filter(Boolean);
  }

  function providerModels(provider) {
    if (!provider || !Array.isArray(provider.models)) return [];
    return provider.models
      .map((model) => typeof model === 'string'
        ? { id: model.replace(`${provider.slug}/`, ''), publicId: model }
        : model && { id: model.id, publicId: model.public_id || model.id })
      .filter((model) => model && typeof model.id === 'string' && model.id);
  }

  function closeAddModels() {
    state.addingProvider = null;
    state.addableModels = [];
    state.addSelectedIds = new Set();
    addModelsBody.replaceChildren();
    clearError('add-models-error');
    setStatus('add-models-status', '');
    setHidden(addModelsPanel, true);
  }

  function renderAddModels() {
    addModelsBody.replaceChildren();
    state.addableModels.forEach((model) => {
      const row = document.createElement('tr');
      const checkCell = document.createElement('td');
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.className = 'model-checkbox';
      checkbox.checked = state.addSelectedIds.has(model.id);
      checkbox.setAttribute('aria-label', `Select ${model.id}`);
      checkbox.addEventListener('change', () => {
        if (checkbox.checked) state.addSelectedIds.add(model.id);
        else state.addSelectedIds.delete(model.id);
        updateAddModelsSelection();
      });
      checkCell.append(checkbox);
      const idCell = document.createElement('td');
      const id = document.createElement('span');
      id.className = 'model-id';
      id.textContent = model.id;
      idCell.append(id);
      if (model.display_name && model.display_name !== model.id) {
        const displayName = document.createElement('span');
        displayName.className = 'model-name';
        displayName.textContent = model.display_name;
        idCell.append(displayName);
      }
      const contextCell = document.createElement('td');
      contextCell.textContent = formatNumber(model.context_window);
      const outputCell = document.createElement('td');
      outputCell.textContent = formatNumber(model.max_output_tokens);
      const capabilitiesCell = document.createElement('td');
      capabilitiesCell.append(createCapabilityList(model.capabilities));
      row.append(checkCell, idCell, contextCell, outputCell, capabilitiesCell);
      addModelsBody.append(row);
    });
    setHidden($('add-models-empty'), state.addableModels.length > 0);
    updateAddModelsSelection();
  }

  function updateAddModelsSelection() {
    const total = state.addableModels.length;
    const selected = state.addSelectedIds.size;
    $('add-models-selection-count').textContent = `${selected} selected`;
    const selectAllNew = $('add-models-select-all');
    selectAllNew.checked = total > 0 && selected === total;
    selectAllNew.indeterminate = selected > 0 && selected < total;
    $('add-models-save').disabled = selected === 0;
  }

  async function discoverProviderModels(provider, button) {
    if (!provider?.slug) return;
    clearError('inventory-error');
    setBusy(button, true, 'Add models', 'Discovering…');
    try {
      state.adminToken = adminTokenInput.value;
      const payload = await apiRequest(`/admin/api/providers/${encodeURIComponent(provider.slug)}/discover`, {
        method: 'POST', body: JSON.stringify({}),
      });
      const existing = new Set(providerModelIds(provider).map((id) => id.replace(`${provider.slug}/`, '')));
      state.addingProvider = provider;
      state.addableModels = normalizeModels(payload).filter((model) => !existing.has(model.id));
      state.addSelectedIds = new Set();
      $('add-models-title').textContent = `Add models · ${provider.name || provider.slug}`;
      $('add-models-summary').textContent = state.addableModels.length
        ? `${state.addableModels.length} model(s) not yet in this provider.`
        : 'All discovered models are already registered.';
      renderAddModels();
      setHidden(addModelsPanel, false);
      addModelsPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    } catch (error) {
      showError('inventory-error', error.message);
    } finally {
      setBusy(button, false, 'Add models', 'Discovering…');
    }
  }

  async function addSelectedModels() {
    const provider = state.addingProvider;
    if (!provider?.slug || !state.addSelectedIds.size) return;
    clearError('add-models-error');
    const button = $('add-models-save');
    setBusy(button, true, 'Add selected models', 'Adding…');
    setStatus('add-models-status', 'Validating selected models with provider…');
    try {
      await apiRequest(`/admin/api/providers/${encodeURIComponent(provider.slug)}/models`, {
        method: 'POST',
        body: JSON.stringify({ models: [...state.addSelectedIds].map((id) => ({ id })) }),
      });
      closeAddModels();
      await loadInventory();
    } catch (error) {
      showError('add-models-error', error.message);
    } finally {
      if (state.addingProvider) setBusy(button, false, 'Add selected models', 'Adding…');
    }
  }

  async function deleteProviderModel(provider, model, button) {
    if (!provider?.slug || !model?.id) return;
    if (!window.confirm(`Xóa ${model.publicId} khỏi registry? Model này sẽ không còn được harness gọi qua Gatekeeper.`)) return;
    clearError('inventory-error');
    button.disabled = true;
    button.textContent = '…';
    try {
      await apiRequest(
        `/admin/api/providers/${encodeURIComponent(provider.slug)}/models?model=${encodeURIComponent(model.id)}`,
        { method: 'DELETE' },
      );
      if (state.addingProvider?.slug === provider.slug) closeAddModels();
      await loadInventory();
    } catch (error) {
      button.disabled = false;
      button.textContent = '×';
      showError('inventory-error', error.message);
    }
  }

  function renderInventory(providers) {
    inventoryList.replaceChildren();
    setHidden($('inventory-empty'), providers.length > 0);
    if (!providers.length) {
      $('inventory-empty').textContent = state.adminToken.trim()
        ? 'No providers saved yet.'
        : 'Enter an admin token to load saved providers.';
      return;
    }
    providers.forEach((provider) => {
      const card = document.createElement('article');
      card.className = 'provider-card';
      card.classList.toggle('provider-disabled', provider.enabled === false);
      const details = document.createElement('div');
      const title = document.createElement('h3');
      title.textContent = provider.name || provider.slug || 'Unnamed provider';
      const meta = document.createElement('p');
      meta.className = 'provider-meta';
      meta.textContent = `${provider.slug || '—'} · ${protocolLabel(provider.protocol || '')}`;
      const status = document.createElement('span');
      status.className = provider.enabled === false ? 'provider-status disabled' : 'provider-status enabled';
      status.textContent = provider.enabled === false ? 'Disabled' : 'Enabled';
      const keyHint = document.createElement('p');
      keyHint.className = 'provider-key-hint';
      keyHint.textContent = `Primary API key · ${provider.api_key_hint || '••••'}`;
      details.append(title, status, meta, keyHint);
      const fallbackHints = Array.isArray(provider.fallback_api_key_hints)
        ? provider.fallback_api_key_hints
        : [];
      const fallbackSummary = document.createElement('p');
      fallbackSummary.className = 'provider-key-hint';
      fallbackSummary.textContent = fallbackHints.length
        ? `Fallback · ${fallbackHints.map((hint, index) => `${index + 1}: ${hint}`).join(' · ')}`
        : 'Fallback · chưa cấu hình';
      details.append(fallbackSummary);
      const modelList = document.createElement('div');
      modelList.className = 'provider-models';
      providerModels(provider).forEach((model) => {
        const tag = document.createElement('span');
        tag.className = 'provider-model';
        const modelId = document.createElement('span');
        modelId.textContent = model.publicId;
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'provider-model-remove';
        remove.textContent = '×';
        remove.setAttribute('aria-label', `Remove ${model.publicId}`);
        remove.title = `Remove ${model.publicId}`;
        remove.addEventListener('click', () => deleteProviderModel(provider, model, remove));
        tag.append(modelId, remove);
        modelList.append(tag);
      });
      details.append(modelList);
      const actions = document.createElement('div');
      actions.className = 'provider-actions';
      const fallbackEditor = document.createElement('div');
      fallbackEditor.className = 'provider-fallback-editor';
      for (let position = 1; position <= 2; position += 1) {
        const row = document.createElement('div');
        row.className = 'fallback-key-row';
        const input = document.createElement('input');
        input.type = 'password';
        input.autocomplete = 'new-password';
        input.spellcheck = false;
        input.placeholder = fallbackHints[position - 1]
          ? `Thay fallback ${position} · ${fallbackHints[position - 1]}`
          : `Thêm fallback ${position}`;
        input.setAttribute('aria-label', `Fallback API key ${position} for ${provider.name || provider.slug}`);
        if (position === 2 && !fallbackHints[0]) input.disabled = true;
        const saveFallback = document.createElement('button');
        saveFallback.type = 'button';
        saveFallback.className = 'text-button compact-button';
        saveFallback.textContent = fallbackHints[position - 1] ? 'Replace' : 'Add';
        saveFallback.disabled = input.disabled;
        saveFallback.addEventListener('click', () => setFallbackKey(
          provider.slug, position, input, saveFallback,
        ));
        row.append(input, saveFallback);
        if (fallbackHints[position - 1]) {
          const removeFallback = document.createElement('button');
          removeFallback.type = 'button';
          removeFallback.className = 'text-button compact-button danger-text';
          removeFallback.textContent = 'Remove';
          removeFallback.addEventListener('click', () => deleteFallbackKey(
            provider.slug, position, removeFallback,
          ));
          row.append(removeFallback);
        }
        fallbackEditor.append(row);
      }
      actions.append(fallbackEditor);
      const addModelsButton = document.createElement('button');
      addModelsButton.type = 'button';
      addModelsButton.className = 'secondary-button compact-button';
      addModelsButton.textContent = 'Add models';
      addModelsButton.addEventListener('click', () => discoverProviderModels(provider, addModelsButton));
      actions.append(addModelsButton);
      const toggleProvider = document.createElement('button');
      toggleProvider.type = 'button';
      toggleProvider.className = 'secondary-button compact-button';
      toggleProvider.textContent = provider.enabled === false ? 'Enable provider' : 'Disable provider';
      toggleProvider.addEventListener('click', () => setProviderEnabled(
        provider.slug, provider.enabled === false, toggleProvider,
      ));
      actions.append(toggleProvider);
      const deleteButton = document.createElement('button');
      deleteButton.type = 'button';
      deleteButton.className = 'danger-button';
      deleteButton.textContent = 'Delete';
      deleteButton.dataset.slug = provider.slug || '';
      deleteButton.addEventListener('click', () => deleteProvider(provider.slug, deleteButton));
      actions.append(deleteButton);
      card.append(details, actions);
      inventoryList.append(card);
    });
  }

  async function loadInventory() {
    clearError('inventory-error');
    setStatus('inventory-status', 'Loading saved providers…');
    try {
      state.adminToken = adminTokenInput.value;
      const payload = await apiRequest('/admin/api/providers');
      renderInventory(normalizeProviders(payload));
      setStatus('inventory-status', '');
    } catch (error) {
      setStatus('inventory-status', '');
      showError('inventory-error', error.message);
      renderInventory([]);
    }
  }

  async function deleteProvider(slug, button) {
    if (!slug) return;
    clearError('inventory-error');
    button.disabled = true;
    button.textContent = 'Deleting…';
    try {
      await apiRequest(`/admin/api/providers/${encodeURIComponent(slug)}`, { method: 'DELETE' });
      await loadInventory();
    } catch (error) {
      button.disabled = false;
      button.textContent = 'Delete';
      showError('inventory-error', error.message);
    }
  }

  async function setProviderEnabled(slug, enabled, button) {
    if (!slug) return;
    if (!enabled && !window.confirm(
      `Disable ${slug}? Harnesses sẽ không nhìn thấy hoặc gọi được models của provider này.`,
    )) return;
    clearError('inventory-error');
    button.disabled = true;
    button.textContent = enabled ? 'Enabling…' : 'Disabling…';
    try {
      await apiRequest(`/admin/api/providers/${encodeURIComponent(slug)}/enabled`, {
        method: 'PUT',
        body: JSON.stringify({ enabled }),
      });
      await loadInventory();
    } catch (error) {
      button.disabled = false;
      button.textContent = enabled ? 'Enable provider' : 'Disable provider';
      showError('inventory-error', error.message);
    }
  }

  async function setFallbackKey(slug, position, input, button) {
    const apiKey = input.value.trim();
    if (!apiKey) {
      showError('inventory-error', `Nhập fallback API key ${position}.`);
      input.focus();
      return;
    }
    clearError('inventory-error');
    button.disabled = true;
    button.textContent = 'Saving…';
    try {
      await apiRequest(
        `/admin/api/providers/${encodeURIComponent(slug)}/fallback-keys/${position}`,
        { method: 'PUT', body: JSON.stringify({ api_key: apiKey }) },
      );
      input.value = '';
      await loadInventory();
    } catch (error) {
      input.value = '';
      button.disabled = false;
      button.textContent = 'Retry';
      showError('inventory-error', error.message);
    }
  }

  async function deleteFallbackKey(slug, position, button) {
    clearError('inventory-error');
    button.disabled = true;
    button.textContent = 'Removing…';
    try {
      await apiRequest(
        `/admin/api/providers/${encodeURIComponent(slug)}/fallback-keys/${position}`,
        { method: 'DELETE' },
      );
      await loadInventory();
    } catch (error) {
      button.disabled = false;
      button.textContent = 'Remove';
      showError('inventory-error', error.message);
    }
  }

  function bindPasswordToggle(buttonId, inputId) {
    $(buttonId).addEventListener('click', () => {
      const input = $(inputId);
      const visible = input.type === 'text';
      input.type = visible ? 'password' : 'text';
      $(buttonId).textContent = visible ? 'Show' : 'Hide';
      $(buttonId).setAttribute('aria-pressed', String(!visible));
    });
  }

  adminTokenInput.addEventListener('input', () => {
    state.adminToken = adminTokenInput.value;
  });
  discoverForm.addEventListener('submit', discover);
  saveForm.addEventListener('submit', saveProvider);
  $('discover-again').addEventListener('click', () => {
    modelsPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    baseUrlInput.focus();
  });
  $('refresh-inventory').addEventListener('click', loadInventory);
  $('cancel-add-models').addEventListener('click', closeAddModels);
  $('add-models-save').addEventListener('click', addSelectedModels);
  $('add-models-select-all').addEventListener('change', (event) => {
    state.addSelectedIds = event.target.checked
      ? new Set(state.addableModels.map((model) => model.id))
      : new Set();
    renderAddModels();
  });
  selectAll.addEventListener('change', () => {
    state.discoveredModels.forEach((model) => {
      if (selectAll.checked) state.selectedIds.add(model.id);
      else state.selectedIds.delete(model.id);
    });
    renderModels();
    scheduleEnrich();
  });
  bindPasswordToggle('toggle-admin-token', 'admin-token');
  bindPasswordToggle('toggle-provider-key', 'provider-key');
  window.addEventListener('pagehide', () => {
    state.adminToken = '';
    state.providerKey = '';
    adminTokenInput.value = '';
    providerKeyInput.value = '';
    $('fallback-key-1').value = '';
    $('fallback-key-2').value = '';
  });
})();
