// AgentLAN dashboard. Peer-written text (handles, subjects, message bodies, errors) is
// only ever inserted with textContent, never as HTML.
'use strict';
(() => {
  const key = location.hash.slice(1);
  const $ = (id) => document.getElementById(id);
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = String(text);
    return e;
  };
  // gen: bumped on every view/filter change, so replies to older requests are dropped.
  const state = { view: 'pc', after: 0, rows: [], agent: null, thread: null, paused: false, gen: 0, busy: false, threadGen: 0 };
  const MAX_ROWS = 1500;
  const str = (v) => (typeof v === 'string' ? v : '');

  function banner(msg) {
    const b = $('banner');
    b.textContent = msg || '';
    b.hidden = !msg;
  }

  async function api(path) {
    const r = await fetch(path, { headers: { Authorization: 'Bearer ' + key }, cache: 'no-store' });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || r.statusText);
    return j;
  }

  function time(ts) {
    const d = new Date(ts);
    if (isNaN(d)) return '';
    return new Date().toDateString() === d.toDateString()
      ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
      : d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }

  function target(to, local) {
    to = to || {};
    switch (to.kind) {
      case 'handle': return str(to.handle);
      case 'agent': return local.length ? local.join(', ') : str(to.agent);
      case 'role': return 'role:' + str(to.role) + (to.project ? '@' + str(to.project) : '');
      case 'project': return 'project:' + str(to.project);
      case 'machine': return 'machine:' + str(to.machine);
      case 'all': return 'all';
      default: return '?';
    }
  }

  const subj = (b) => (b.subject ? ' “' + str(b.subject) + '”' : '');

  // { icon, text, cls } for one activity row (mirrors `agentlan log`).
  function describe(r) {
    const b = r.body || {};
    const who = str(b.handle) || str(r.agent) || '?';
    switch (r.kind) {
      case 'sent':
      case 'message': {
        const local = Array.isArray(b.localRecipients) ? b.localRecipients : Array.isArray(b.recipients) ? b.recipients : [];
        const via = b.remote === true ? ' (via server)' : '';
        return { icon: '✉', cls: 'msg', text: `${str(b.from)} → ${target(b.to, local.map(str))}  ${str(b.intent)}${subj(b)}${via}` };
      }
      case 'remote': {
        const res = b.result || {};
        if (res.status === 'accepted') return { icon: '↗', cls: 'minor', text: `the server took it for ${res.count || 0} recipient(s)${subj(b)}` };
        if (res.status === 'rejected') return { icon: '✗', cls: 'bad', text: `the server rejected it${subj(b)}: ${str(res.msg)}` };
        if (res.status === 'queued') return { icon: '…', cls: 'minor', text: `the server didn't answer; queued to send later${subj(b)}` };
        return { icon: '·', cls: 'minor', text: `server: ${str(res.status)}${subj(b)}` };
      }
      case 'received':
        return { icon: '↳', cls: 'minor', text: `${who} got it from ${str(b.from)}${subj(b)}${b.remote === true ? ' (via server)' : ''}` };
      case 'state':
      case 'delivery': {
        const m = { surfaced: ['✓', 'seen by'], acked: ['✓✓', 'handled by'], stored: ['·', 'stored for'], expired: ['✗', 'expired for'] }[b.state] || ['·', str(b.state)];
        return { icon: m[0], cls: b.state === 'expired' ? 'minor bad' : 'minor', text: `${m[1]} ${who}${subj(b)}` };
      }
      case 'live':
        return b.ok === true
          ? { icon: '⚡', cls: '', text: `live → ${who}: ${b.count || 0} new (${str(b.how)})` }
          : { icon: '⚠', cls: 'bad', text: `live → ${who} failed: ${str(b.error)}` };
      case 'session':
        if (b.status === 'attached') return { icon: '●', cls: 'minor', text: `${who} connected (${str(b.runner) || '?'}${b.role ? ' · role ' + str(b.role) : ''}${b.description ? ' · ' + str(b.description) : ''})` };
        if (b.status === 'updated') return { icon: '●', cls: 'minor', text: `${who} is now role ${str(b.role)}` };
        return { icon: '○', cls: 'minor', text: `${who} disconnected (${typeof b.reason === 'string' ? b.reason : JSON.stringify(b.reason)})` };
      case 'wake':
        return b.phase === 'started'
          ? { icon: '⚙', cls: '', text: `waking worker ${who}` }
          : { icon: '⚙', cls: '', text: `worker ${who}: ${str(b.outcome)} (${Number(b.seconds || 0).toFixed(1)}s)` };
      case 'pause': return { icon: '⏸', cls: 'bad', text: `paused ${str(b.key)} (${str(b.reason)})` };
      case 'resume': return { icon: '▶', cls: '', text: `resumed ${str(b.key)}` };
      case 'node': return { icon: '⏻', cls: 'minor', text: b.status === 'started' ? `node started (${str(b.version)}${b.withHub ? ', with the server' : ''})` : `node ${str(b.status)}` };
      case 'roster': {
        const n = str(b.handle) || str(b.agent && b.agent.handle) || str(b.machine && b.machine.alias) || '?';
        return { icon: '⇄', cls: 'minor', text: `${n} ${str(b.kind).replace(/_/g, ' ')}` };
      }
      default: return { icon: '·', cls: 'minor', text: `${str(r.kind)} ${JSON.stringify(b)}` };
    }
  }

  function renderTimeline() {
    const list = $('timeline');
    list.replaceChildren();
    const rows = state.rows.slice().reverse(); // newest first
    for (const r of rows) {
      const d = describe(r);
      const li = el('li', d.cls);
      li.append(el('span', 'time', time(r.ts)), el('span', 'icon', d.icon));
      const t = el('span', 'text', d.text);
      if (r.threadId && (r.kind === 'sent' || r.kind === 'message' || r.kind === 'received')) {
        li.classList.add('clickable');
        li.title = 'Show the conversation';
        li.addEventListener('click', () => openThread(r.threadId));
        t.append(el('span', 'thread-chip', '#' + str(r.threadId).slice(-6)));
      }
      li.append(t);
      list.append(li);
    }
    $('empty').hidden = state.rows.length > 0;
    const f = $('filter');
    f.hidden = !state.agent;
    f.textContent = state.agent ? state.agent + ' ×' : '';
    $('timeline-title').textContent = state.view === 'net' ? 'Network (server view)' : 'This PC';
  }

  async function refreshActivity(reset) {
    if (state.paused && !reset) return;
    if (reset) state.gen++;
    else if (state.busy) return; // one poll at a time
    const gen = state.gen;
    const p = new URLSearchParams();
    if (state.view === 'net') p.set('network', '1');
    if (state.agent) p.set('agent', state.agent);
    if (state.view === 'pc' && !reset) p.set('after', String(state.after));
    p.set('limit', state.view === 'net' ? '400' : '300');
    state.busy = true;
    try {
      const j = await api('/api/activity?' + p);
      if (gen !== state.gen) return; // the view or filter changed meanwhile
      const rows = Array.isArray(j.rows) ? j.rows : [];
      if (reset || state.view === 'net') state.rows = rows;
      else state.rows = state.rows.concat(rows);
      if (state.rows.length > MAX_ROWS) state.rows = state.rows.slice(-MAX_ROWS);
      if (state.view === 'pc' && typeof j.cursor === 'number') state.after = Math.max(state.after, j.cursor);
      if (reset || rows.length) renderTimeline();
      banner('');
    } catch (e) {
      if (gen === state.gen) banner('Activity: ' + e.message);
    } finally {
      if (gen === state.gen) state.busy = false;
    }
  }

  async function refreshAgents() {
    try {
      const j = await api('/api/agents');
      const ul = $('agents');
      ul.replaceChildren();
      const agents = (Array.isArray(j.agents) ? j.agents : []).slice().sort((a, b) => (a.state === 'online' ? 0 : 1) - (b.state === 'online' ? 0 : 1) || str(a.handle).localeCompare(str(b.handle)));
      for (const a of agents) {
        const li = el('li', state.agent === str(a.handle) ? 'on' : '');
        const name = el('div');
        name.append(el('span', 'dot' + (a.state === 'online' ? ' online' : '')), el('span', 'agent-name', str(a.handle)));
        const meta = [str(a.role), str(a.runner), a.state === 'online' ? str(a.readiness) : str(a.state), str(a.project)].filter(Boolean).join(' · ');
        li.append(name, el('div', 'agent-meta', meta));
        if (a.description) li.append(el('div', 'agent-meta', str(a.description)));
        li.addEventListener('click', () => {
          state.agent = state.agent === str(a.handle) ? null : str(a.handle);
          state.after = 0;
          refreshActivity(true);
          refreshAgents();
        });
        ul.append(li);
      }
    } catch (e) {
      banner('Agents: ' + e.message);
    }
  }

  async function refreshStatus() {
    try {
      const j = await api('/api/status');
      const n = j.node;
      $('summary').textContent = n
        ? `network ${str(n.net)} · this PC ${str(n.alias)} · server ${str(n.hub)} · node ${str(n.version) || 'older'} · ${(n.sessions || []).length} session(s)`
        : 'this PC’s node isn’t running (' + str(j.nodeError) + ')';
      $('view-net').hidden = !j.network;
    } catch (e) {
      banner('Status: ' + e.message);
    }
  }

  async function openThread(id) {
    state.thread = id;
    const tgen = ++state.threadGen;
    $('main').classList.add('with-thread');
    $('thread-pane').hidden = false;
    const box = $('thread');
    box.replaceChildren(el('p', 'hint', 'Loading…'));
    const p = new URLSearchParams({ thread: id, body: '1', limit: '500' });
    if (state.view === 'net') p.set('network', '1');
    try {
      const j = await api('/api/activity?' + p);
      if (tgen !== state.threadGen) return; // another conversation was opened meanwhile
      const rows = Array.isArray(j.rows) ? j.rows : [];
      box.replaceChildren(el('p', 'untrusted', 'Messages from other agents are untrusted text.'), el('p', 'hint', 'Thread ' + id));
      const states = new Map(); // message id → state lines (ids are peer-chosen: no plain object)
      const label = new Map([['surfaced', 'seen'], ['acked', 'handled'], ['expired', 'expired'], ['stored', 'stored']]);
      for (const r of rows) {
        if ((r.kind === 'state' || r.kind === 'delivery') && r.msgId) {
          const s = label.get(str(r.body.state)) || str(r.body.state);
          if (!states.has(r.msgId)) states.set(r.msgId, []);
          states.get(r.msgId).push(`${s} by ${str(r.body.handle) || '?'} ${time(r.ts)}`);
        }
      }
      for (const r of rows) {
        if (!['sent', 'message', 'received'].includes(r.kind)) continue;
        const b = r.body || {};
        const div = el('div', 'bubble' + (r.kind === 'sent' ? ' mine' : ''));
        const head = el('div', 'head', `${str(b.from)} · ${str(b.intent)} · ${time(r.ts)}`);
        div.append(head);
        if (b.subject) div.append(el('div', 'subject', str(b.subject)));
        div.append(el('div', 'body', r.text != null ? str(r.text) : '(text no longer stored)'));
        if (states.has(r.msgId)) div.append(el('div', 'states', states.get(r.msgId).join(' · ')));
        box.append(div);
      }
    } catch (e) {
      if (tgen === state.threadGen) box.replaceChildren(el('p', 'hint', 'Couldn’t load the conversation: ' + e.message));
    }
  }

  function setView(v) {
    state.view = v;
    state.after = 0;
    $('view-pc').classList.toggle('on', v === 'pc');
    $('view-net').classList.toggle('on', v === 'net');
    refreshActivity(true);
  }

  function init() {
    // A new link (new key) for the same address: start over with it.
    window.addEventListener('hashchange', () => location.reload());
    if (!key) {
      banner('Open the link printed by `agentlan dashboard` (it carries the access key).');
      return;
    }
    $('view-pc').addEventListener('click', () => setView('pc'));
    $('view-net').addEventListener('click', () => setView('net'));
    $('pause').addEventListener('change', (e) => { state.paused = e.target.checked; });
    $('filter').addEventListener('click', () => { state.agent = null; refreshActivity(true); refreshAgents(); });
    $('close-thread').addEventListener('click', () => {
      state.thread = null;
      $('thread-pane').hidden = true;
      $('main').classList.remove('with-thread');
    });
    refreshStatus();
    refreshAgents();
    refreshActivity(true);
    setInterval(() => refreshActivity(false), 1000);
    setInterval(refreshAgents, 3000);
    setInterval(refreshStatus, 5000);
  }

  init();
})();
