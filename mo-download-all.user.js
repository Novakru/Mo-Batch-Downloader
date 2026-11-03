// ==UserScript==
// @name         Mo 浙大工作台：一键下载全部文件
// @namespace    local.mo.download
// @version      1.3.0
// @description  递归下载项目和挂载数据集，流式写入 ZIP64，保留目录结构与进度显示。
// @match        https://mo.zju.edu.cn/workspace/*
// @grant        unsafeWindow
// @run-at       document-idle
// ==/UserScript==

(() => {
  'use strict';
  const page = unsafeWindow;
  const LIMIT = 512 * 1024 * 1024;
  let controller;
  const host = document.createElement('div');
  host.style.cssText = 'position:fixed;right:20px;bottom:24px;z-index:2147483647';
  const ui = host.attachShadow({ mode: 'closed' });
  ui.innerHTML = `<style>
    section{width:min(310px,calc(100vw - 68px));padding:14px;background:#fff;color:#222;border:1px solid #ddd;
      border-radius:10px;box-shadow:0 4px 18px #0002;font:13px/1.6 system-ui}
    button{border:0;border-radius:6px;padding:8px 12px;cursor:pointer;background:#2861fc;color:#fff}
    button:disabled{opacity:.5;cursor:default} #stop{background:#666;margin-left:8px}
    p{margin:10px 0 0;overflow-wrap:anywhere;white-space:pre-wrap;max-height:180px;overflow:auto}
    a{display:block;margin-top:8px;color:#2861fc}
    [hidden]{display:none!important} progress{display:block;width:100%;height:12px;margin-top:12px;accent-color:#2861fc}
    small{display:block;color:#444;margin-top:6px;font-variant-numeric:tabular-nums}
    button:focus-visible,a:focus-visible{outline:2px solid #222;outline-offset:3px}
  </style><section><button id="start">下载全部文件（ZIP）</button><button id="stop" disabled>取消</button>
    <progress id="progress" max="100" value="0" aria-label="当前阶段进度"></progress>
    <small id="detail">等待开始</small>
    <p id="status" role="status">请先打开项目，等待文件列表加载。保存的是服务器上的文件，请先保存编辑内容。</p>
    <a id="save" hidden>再次保存 ZIP</a></section>`;
  document.body.appendChild(host);
  const start = ui.querySelector('#start');
  const stop = ui.querySelector('#stop');
  const status = ui.querySelector('#status');
  const save = ui.querySelector('#save');
  const progress = ui.querySelector('#progress');
  const detail = ui.querySelector('#detail');
  let previousUrl;
  const tell = text => { status.textContent = text; };
  const check = () => controller.signal.throwIfAborted();
  const encodePath = path => path.split('/').map(encodeURIComponent).join('/');
  const pause = () => new Promise(resolve => setTimeout(resolve, 0));
  const sizeText = bytes => bytes >= 1073741824 ? `${(bytes / 1073741824).toFixed(2)} GiB` :
    bytes < 1048576 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1048576).toFixed(1)} MiB`;
  function showProgress(percent, text) {
    if (percent === null) progress.removeAttribute('value');
    else progress.value = Math.max(0, Math.min(100, percent));
    detail.textContent = text;
  }

  // Streaming ZIP64 STORE writer. Only directory records remain in memory.
  class StreamZip {
    constructor(sink) {
      this.sink = sink;
      this.offset = 0n;
      this.directory = [];
      this.table = new Uint32Array(256);
      for (let i = 0; i < 256; i++) {
        let crc = i;
        for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
        this.table[i] = crc;
      }
      const now = new Date();
      this.time = (now.getHours() << 11) | (now.getMinutes() << 5) | (now.getSeconds() >> 1);
      this.date = ((Math.max(1980, now.getFullYear()) - 1980) << 9) | ((now.getMonth() + 1) << 5) | now.getDate();
    }
    header(length) {
      const bytes = new Uint8Array(length);
      return { bytes, view: new DataView(bytes.buffer) };
    }
    async write(bytes) {
      check();
      await this.sink.write(bytes);
      this.offset += BigInt(bytes.byteLength);
    }
    async entry(path, chunks, onChunk = () => {}) {
      check();
      const name = new TextEncoder().encode(path);
      if (name.length > 65535) throw new Error('ZIP path exceeds 65535 UTF-8 bytes');
      const offset = this.offset;
      const local = this.header(30 + name.length + 20);
      local.view.setUint32(0, 0x04034b50, true);
      local.view.setUint16(4, 45, true);
      local.view.setUint16(6, 0x808, true);
      local.view.setUint16(10, this.time, true);
      local.view.setUint16(12, this.date, true);
      local.view.setUint32(18, 0xffffffff, true);
      local.view.setUint32(22, 0xffffffff, true);
      local.view.setUint16(26, name.length, true);
      local.view.setUint16(28, 20, true);
      local.bytes.set(name, 30);
      local.view.setUint16(30 + name.length, 1, true);
      local.view.setUint16(32 + name.length, 16, true);
      await this.write(local.bytes);
      let crc = 0xffffffff;
      let size = 0n;
      let lastYield = Date.now();
      for await (const chunk of chunks) {
        for (let start = 0; start < chunk.byteLength; start += 262144) {
          check();
          const part = chunk.subarray(start, Math.min(start + 262144, chunk.byteLength));
          for (let i = 0; i < part.length; i++) crc = (crc >>> 8) ^ this.table[(crc ^ part[i]) & 255];
          await this.write(part);
          size += BigInt(part.byteLength);
          onChunk(part.byteLength, Number(size));
          if (Date.now() - lastYield >= 12) { await pause(); check(); lastYield = Date.now(); }
        }
      }
      crc = (crc ^ 0xffffffff) >>> 0;
      const descriptor = this.header(24);
      descriptor.view.setUint32(0, 0x08074b50, true);
      descriptor.view.setUint32(4, crc, true);
      descriptor.view.setBigUint64(8, size, true);
      descriptor.view.setBigUint64(16, size, true);
      await this.write(descriptor.bytes);
      const central = this.header(46 + name.length + 28);
      central.view.setUint32(0, 0x02014b50, true);
      central.view.setUint16(4, 45, true);
      central.view.setUint16(6, 45, true);
      central.view.setUint16(8, 0x808, true);
      central.view.setUint16(12, this.time, true);
      central.view.setUint16(14, this.date, true);
      central.view.setUint32(16, crc, true);
      central.view.setUint32(20, 0xffffffff, true);
      central.view.setUint32(24, 0xffffffff, true);
      central.view.setUint16(28, name.length, true);
      central.view.setUint16(30, 28, true);
      central.view.setUint32(38, path.endsWith('/') ? 0x10 : 0, true);
      central.view.setUint32(42, 0xffffffff, true);
      central.bytes.set(name, 46);
      const extra = 46 + name.length;
      central.view.setUint16(extra, 1, true);
      central.view.setUint16(extra + 2, 24, true);
      central.view.setBigUint64(extra + 4, size, true);
      central.view.setBigUint64(extra + 12, size, true);
      central.view.setBigUint64(extra + 20, offset, true);
      this.directory.push(central.bytes);
    }
    async finish(onUpdate) {
      const start = this.offset;
      for (let i = 0; i < this.directory.length; i++) {
        await this.write(this.directory[i]);
        onUpdate((i + 1) / this.directory.length * 100);
        if (i % 100 === 0) await pause();
      }
      const size = this.offset - start;
      const endOffset = this.offset;
      const end64 = this.header(56);
      end64.view.setUint32(0, 0x06064b50, true);
      end64.view.setBigUint64(4, 44n, true);
      end64.view.setUint16(12, 45, true);
      end64.view.setUint16(14, 45, true);
      end64.view.setBigUint64(24, BigInt(this.directory.length), true);
      end64.view.setBigUint64(32, BigInt(this.directory.length), true);
      end64.view.setBigUint64(40, size, true);
      end64.view.setBigUint64(48, start, true);
      await this.write(end64.bytes);
      const locator = this.header(20);
      locator.view.setUint32(0, 0x07064b50, true);
      locator.view.setBigUint64(8, endOffset, true);
      locator.view.setUint32(16, 1, true);
      await this.write(locator.bytes);
      const end = this.header(22);
      end.view.setUint32(0, 0x06054b50, true);
      end.view.setUint16(8, 0xffff, true);
      end.view.setUint16(10, 0xffff, true);
      end.view.setUint32(12, 0xffffffff, true);
      end.view.setUint32(16, 0xffffffff, true);
      await this.write(end.bytes);
    }
  }

  function safePath(path) {
    if (typeof path !== 'string' || !path || path.startsWith('/') ||
        path.includes('\\') || path.includes('\0') ||
        path.split('/').some(part => !part || part === '..' || part === '.')) {
      throw new Error('接口返回了不安全或无法识别的文件路径');
    }
    return path;
  }

  async function request(url, headers = {}) {
    check();
    const target = new URL(url, location.origin);
    if (target.origin !== location.origin) throw new Error('拒绝向其他域名发送项目凭证');
    const response = await fetch(target.href, {
      credentials: 'same-origin', headers, signal: controller.signal,
    });
    if (!response.ok) throw new Error(`请求失败：HTTP ${response.status}`);
    if (new URL(response.url).origin !== location.origin || /\/user\/login/.test(response.url)) {
      throw new Error('登录已失效，请重新登录并刷新页面');
    }
    return response;
  }

  async function json(url, headers) {
    const response = await request(url, headers);
    if (/text\/html/i.test(response.headers.get('content-type') || '')) {
      throw new Error('接口返回了网页，请确认已登录且项目已启动');
    }
    const result = await response.json();
    if (typeof result.status === 'number' && result.status !== 200) {
      throw new Error(result.message || `接口错误：${result.status}`);
    }
    return result;
  }

  async function connection(projectId) {
    // 与站点现有单文件下载代码使用相同的 Jupyter baseUrl 和 token。
    const node = document.getElementById('jupyter-config-data');
    let config = {};
    try { config = JSON.parse(node?.textContent || '{}'); } catch (_) { /* 继续读取接口 */ }
    let base = config.baseUrl;
    let token = config.token || page.hubToken;
    if (!base) {
      const entry = performance.getEntriesByType('resource').map(item => item.name)
        .reverse().find(url => /\/hub_api\/user\/[^/]+\/(?:api\/|files\/|lab)/.test(url));
      if (entry) base = new URL(entry).pathname.match(/^(.*?\/hub_api\/user\/[^/]+\/)/)?.[1];
    }
    if (!base || !token) {
      const bearer = localStorage.getItem('token');
      const metadata = await json(`/pyapi/apps/${projectId}?yml=true&collaborators=true`,
        bearer ? { Authorization: `Bearer ${bearer}` } : {});
      const project = metadata.data || metadata.res || metadata;
      if (!base && project.hub_name) base = `/hub_api/user/${encodeURIComponent(project.hub_name)}/`;
      token ||= project.hub_token;
    }
    if (!base) throw new Error('尚未找到项目下载地址。请先启动工作台并等待文件列表加载，再重试。');
    const target = new URL(base, location.origin);
    if (target.origin !== location.origin || !/^\/hub_api\/user\/[^/]+\/$/.test(target.pathname)) {
      throw new Error('无法识别当前 Jupyter 下载地址，请反馈此提示以适配页面');
    }
    return { base: target.pathname, token };
  }

  async function downloadAll() {
    const match = location.pathname.match(/^\/workspace\/([a-f\d]{24})\/app(?:\/|$)/i);
    if (!match) return tell('请在 /workspace/项目ID/app 页面使用。');
    const projectId = match[1];
    const filename = `mo-${projectId}-${new Date().toISOString().slice(0, 10)}.zip`;
    let sink;
    let diskHandle;
    let committed = false;
    const memoryParts = [];
    controller = new AbortController();
    start.disabled = true;
    stop.disabled = false;
    save.hidden = true;
    let stage = '准备';
    const started = Date.now();
    const clockText = () => `耗时 ${Math.floor((Date.now() - started) / 1000)} 秒`;
    const ticker = setInterval(() => { detail.textContent = detail.textContent.replace(/ · 耗时 .*$/, '') + ` · ${clockText()}`; }, 1000);
    showProgress(null, '准备下载');
    try {
      // 必须在点击事件中、首次异步操作之前打开保存对话框。
      if (typeof page.showSaveFilePicker === 'function') {
        tell('请选择 ZIP 保存位置，随后开始扫描和下载。');
        diskHandle = await page.showSaveFilePicker({
          suggestedName: filename,
          types: [{ description: 'ZIP 文件', accept: { 'application/zip': ['.zip'] } }],
        });
        check();
      }
      tell('正在读取项目下载配置……');
      const { base, token } = await connection(projectId);
      const runtimeHeaders = token ? { Authorization: `token ${token}` } : {};
      const files = [];
      const emptyDatasetDirs = [];
      const dirs = ['.'];
      const visited = new Set();
      const paths = new Set();
      const folders = [];
      stage = '扫描';
      while (dirs.length) {
        check();
        const dir = dirs.shift();
        if (visited.has(dir)) continue;
        visited.add(dir);
        tell(`正在扫描：${dir}\n已找到 ${files.length} 个文件`);
        // 挂载数据集可能不会被 /pyapi/file/contents 列出。
        // 使用与单文件下载相同的运行环境，读取它实际可访问的目录。
        const listingUrl = new URL(`${base}api/contents${dir === '.' ? '' : `/${encodePath(dir)}`}`, location.origin);
        listingUrl.searchParams.set('content', '1');
        if (token) listingUrl.searchParams.set('token', token);
        const result = await json(listingUrl, runtimeHeaders);
        const entries = result.content || result.data?.content || result.res?.content;
        if (!Array.isArray(entries)) throw new Error(`无法识别文件列表：${dir}`);
        if (!entries.length && (dir === 'datasets' || dir.startsWith('datasets/'))) emptyDatasetDirs.push(dir);
        for (const item of entries) {
          const path = safePath(item.path);
          if (dir !== '.' && !path.startsWith(`${dir}/`)) {
            throw new Error(`目录返回了范围外的路径：${path}`);
          }
          if (paths.has(path)) continue;
          paths.add(path);
          if (item.type === 'directory') { folders.push(path); dirs.push(path); }
          else if (item.type === 'file' || item.type === 'notebook') files.push({ path, size: Number(item.size) || 0 });
          else throw new Error(`无法识别的文件类型：${path} (${item.type})`);
          if (paths.size > 20000) throw new Error('文件及目录超过 20000 项，请缩小下载范围');
        }
      }
      if (!files.length) throw new Error('项目中没有可下载的文件');
      const estimatedTotal = files.reduce((sum, file) => sum + file.size, 0);
      if (!diskHandle && estimatedTotal > LIMIT) {
        throw new Error('当前浏览器不支持直接写入磁盘，内存模式限制为 512 MiB。请用 Chrome 或 Edge 打开此页面后重试');
      }
      let memoryBytes = 0;
      sink = diskHandle ? await diskHandle.createWritable() : {
        async write(chunk) {
          memoryBytes += chunk.byteLength;
          if (memoryBytes > LIMIT) throw new Error('内存模式超过 512 MiB。请用 Chrome 或 Edge 的直接保存模式重试');
          memoryParts.push(new Uint8Array(chunk));
        },
        async abort() { memoryParts.length = 0; },
      };
      check();
      const archive = new StreamZip(sink);
      for (const folder of folders) await archive.entry(`${folder}/`, []);
      let bytes = 0;
      const downloadStarted = Date.now();
      stage = '下载';
      for (let index = 0; index < files.length; index++) {
        check();
        const file = files[index];
        tell(`正在下载并写入 ZIP ${index + 1}/${files.length}\n${file.path}${diskHandle ? '\n直接写入磁盘' : '\n内存模式（最大 512 MiB）'}`);
        showProgress(index / files.length * 100, `下载 ${index}/${files.length} 个 · ${sizeText(bytes)}`);
        const url = new URL(`${base}files/${encodePath(file.path)}`, location.origin);
        if (token) url.searchParams.set('token', token);
        url.searchParams.set('download', '1');
        const response = await request(url, token ? { Authorization: `token ${token}` } : {});
        if (!response.body) throw new Error('当前浏览器无法读取下载流，请更新浏览器');
        let currentBytes = 0;
        const expectedBytes = Number(response.headers.get('content-length')) || file.size || 0;
        const chunks = async function* () {
          const reader = response.body.getReader();
          const prefix = new Uint8Array(512);
          let prefixLength = 0;
          let checkedPrefix = false;
          let ended = false;
          const validatePrefix = () => {
            if (checkedPrefix) return;
            checkedPrefix = true;
            const head = new TextDecoder().decode(prefix.subarray(0, prefixLength));
            if (/^\s*(?:<!doctype html|<html)/i.test(head) && !/\.html?$/i.test(file.path)) {
              throw new Error(`下载 ${file.path} 时返回了 HTML 页面，请确认项目已启动且登录有效`);
            }
          };
          try {
            while (true) {
              check();
              const { done, value } = await reader.read();
              if (done) { ended = true; validatePrefix(); break; }
              if (prefixLength < prefix.length) {
                const count = Math.min(value.byteLength, prefix.length - prefixLength);
                prefix.set(value.subarray(0, count), prefixLength);
                prefixLength += count;
                if (prefixLength === prefix.length) validatePrefix();
              }
              yield value;
            }
          } finally {
            if (!ended) { try { await reader.cancel(); } catch (_) { /* 请求可能已中断 */ } }
            reader.releaseLock();
          }
        };
        await archive.entry(file.path, chunks(), (count, written) => {
          bytes += count;
          currentBytes = written;
          const fraction = expectedBytes ? Math.min(currentBytes / expectedBytes, 0.99) : 0;
          const speed = bytes / Math.max(1, (Date.now() - downloadStarted) / 1000);
          showProgress((index + fraction) / files.length * 100,
            `下载 ${index}/${files.length} 个 · 已写入 ${sizeText(bytes)} · ${sizeText(speed)}/秒\n当前文件 ${sizeText(currentBytes)}${expectedBytes ? ` / ${sizeText(expectedBytes)}` : ''}`);
        });
        showProgress((index + 1) / files.length * 100, `下载 ${index + 1}/${files.length} 个 · ${sizeText(bytes)}`);
      }
      stage = '保存';
      tell('文件已写入，正在保存 ZIP 目录记录……');
      await archive.finish(percent => {
        showProgress(percent, `保存目录记录 ${Math.floor(percent)}%`);
      });
      check();
      if (diskHandle) {
        tell('正在完成磁盘保存，请保持页面打开……');
        showProgress(null, '正在完成磁盘保存');
        await sink.close();
        committed = true;
      } else {
        const blob = new Blob(memoryParts, { type: 'application/zip' });
        memoryParts.length = 0;
        if (previousUrl) URL.revokeObjectURL(previousUrl);
        previousUrl = URL.createObjectURL(blob);
        save.href = previousUrl;
        save.download = filename;
        save.hidden = false;
        save.click();
        committed = true;
      }
      const emptyNotice = emptyDatasetDirs.length ? `\n注意：运行环境仍将以下数据集目录列为空：\n${emptyDatasetDirs.slice(0, 3).join('\n')}\n这些目录中的数据未包含在 ZIP 中，请在网页检查挂载状态。` : '';
      tell(`已保存 ${files.length} 个文件，保留子目录。${diskHandle ? `\n文件：${diskHandle.name || filename}` : '\n若未弹出保存窗口，点击下方“再次保存 ZIP”。'}${emptyNotice}`);
      showProgress(100, `完成 · ${files.length} 个文件 · ZIP ${sizeText(Number(archive.offset))}`);
    } catch (error) {
      if (sink && !committed) { try { await sink.abort(); } catch (_) { /* 不覆盖原始错误 */ } }
      const cancelled = controller.signal.aborted || error.name === 'AbortError';
      tell(cancelled ? '已取消，未提交本次 ZIP 写入。' : `下载未完成：${error.message}\n本次 ZIP 写入未提交。`);
      detail.textContent = cancelled ? `已取消 · ${stage}阶段` : `未完成 · ${stage}阶段`;
    } finally {
      clearInterval(ticker);
      detail.textContent += ` · ${clockText()}`;
      start.disabled = false;
      stop.disabled = true;
    }
  }
  start.addEventListener('click', downloadAll);
  stop.addEventListener('click', () => controller?.abort());
})();
