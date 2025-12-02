async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  const ct = r.headers.get('content-type') || '';
  if (ct.includes('application/json')) return await r.json();
  const text = await r.text(); throw new Error(text.slice(0, 400));
}

async function getJSON(url) {
  const r = await fetch(url);
  const ct = r.headers.get('content-type') || '';
  if (ct.includes('application/json')) return await r.json();
  const text = await r.text(); throw new Error(text.slice(0, 400));
}

function fillSelect(sel, arr, addAll=false) {
  sel.innerHTML = '';
  if (addAll) {
    const optAll = document.createElement('option');
    optAll.value = 'all'; optAll.textContent = '全部';
    sel.appendChild(optAll);
  }
  arr.forEach(v=>{
    const opt = document.createElement('option');
    opt.value = v; opt.textContent = v; sel.appendChild(opt);
  });
}

window.addEventListener('DOMContentLoaded', async () => {
  const ak = document.getElementById('ak');
  const btnLoadRegions = document.getElementById('btnLoadRegions');
  const regionStatus = document.getElementById('regionStatus');
  const province = document.getElementById('province');
  const city = document.getElementById('city');
  const district = document.getElementById('district');
  const queries = document.getElementById('queries');
  const qps = document.getElementById('qps');
  const cityLimit = document.getElementById('city_limit');
  const status = document.getElementById('status');
  const btnCrawl = document.getElementById('btnCrawl');
  const btnExportMapDb = document.getElementById('btnExportMapDb');
  const catsBox = document.getElementById('catsBox');
  const btnRefreshCats = document.getElementById('btnRefreshCats');

  const csvPicker   = document.getElementById('csvPicker');
  const btnAddFiles = document.getElementById('btnAddFiles');
  const dropZone    = document.getElementById('dropZone');
  const fileListUl  = document.getElementById('fileList');
  const btnMerge    = document.getElementById('btnMerge');
  const btnExport   = document.getElementById('btnExportMerge');
  const csvStatus   = document.getElementById('csvStatus');

  let REG = {};
  let regionsLoaded = false;
  let selectedFiles = [];
  let currentGeoJSON = null;

  function lockRegionSelects(lock=true){
    province.disabled = lock; city.disabled = lock; district.disabled = lock;
  }
  function fmtSize(bytes){
    if (bytes == null) return '';
    const units = ['B','KB','MB','GB'];
    let i = 0, n = bytes;
    while (n >= 1024 && i < units.length-1) { n /= 1024; i++; }
    return n.toFixed(n>=10 ? 0 : 1) + ' ' + units[i];
  }
  function fileKey(f){ return `${f.name}__${f.size}__${f.lastModified}`; }
  function renderFileList(){
    fileListUl.innerHTML = '';
    if (selectedFiles.length === 0){
      fileListUl.innerHTML = '<li style="color:#666;">（暂无文件）</li>';
      btnMerge.disabled = true;
      return;
    }
    btnMerge.disabled = false;
    selectedFiles.forEach((f, idx)=>{
      const li = document.createElement('li');
      li.style.cssText = 'display:flex; align-items:center; justify-content:space-between; padding:6px 8px; border:1px solid #eee; border-radius:6px; margin-bottom:4px;';
      li.innerHTML = `
        <div style="overflow:hidden; white-space:nowrap; text-overflow:ellipsis; margin-right:10px;">
          <b>${f.name}</b> <span style="color:#666;">（${fmtSize(f.size)}）</span>
        </div>
        <div>
          <button class="btn" data-idx="${idx}" style="padding:4px 8px;">删除</button>
        </div>`;
      li.querySelector('button').addEventListener('click',(ev)=>{
        const i = parseInt(ev.currentTarget.getAttribute('data-idx'),10);
        selectedFiles.splice(i,1); renderFileList();
      });
      fileListUl.appendChild(li);
    });
  }
  function appendFiles(fileList){
    const exist = new Set(selectedFiles.map(fileKey));
    for (const f of fileList){
      const key = fileKey(f);
      if (!exist.has(key)){
        selectedFiles.push(f);
        exist.add(key);
      }
    }
    renderFileList();
  }

  async function loadRegions({forceRefresh=false} = {}) {
    const params = new URLSearchParams();
    if (ak.value.trim()) params.set('ak', ak.value.trim());
    if (forceRefresh) params.set('refresh', '1');
    regionStatus.textContent = '正在加载全国行政区…';
    lockRegionSelects(true);
    try {
      const res = await getJSON('/regions' + (params.toString()?('?' + params.toString()):''));
      if (res.__error) {
        regionStatus.textContent = '行政区加载失败：' + res.__error;
        regionsLoaded = false; return false;
      }
      const provs = Object.keys(res || {});
      if (provs.length === 0) {
        regionStatus.textContent = '行政区加载失败：返回为空，请检查 AK 权限或配额。';
        regionsLoaded = false; return false;
      }
      REG = res;
      fillSelect(province, provs, false);
      province.dispatchEvent(new Event('change'));
      lockRegionSelects(false);
      regionStatus.textContent = '行政区已加载完成。';
      regionsLoaded = true; return true;
    } catch (e) {
      regionStatus.textContent = '行政区加载失败：' + e.message;
      regionsLoaded = false; return false;
    }
  }

  province.addEventListener('change', ()=>{
    const prov = province.value;
    const cities = Object.keys((REG[prov] || {}));
    fillSelect(city, cities, true);
    fillSelect(district, [], true);
  });
  city.addEventListener('change', ()=>{
    const prov = province.value;
    const c = city.value;
    if (c === 'all') fillSelect(district, [], true);
    else fillSelect(district, REG[prov]?.[c] || [], true);
  });
  btnLoadRegions.addEventListener('click', async ()=>{
    if (!ak.value.trim()) { regionStatus.textContent = '请先输入 AK。'; return; }
    await loadRegions({forceRefresh:true});
  });
  try { await loadRegions({forceRefresh:false}); } catch(e){}

  btnCrawl.addEventListener('click', async ()=>{
    if (!regionsLoaded) { status.textContent = '失败：还未加载行政区'; return; }
    if (!province.value) { status.textContent = '失败：省份为空'; return; }
    status.textContent = '正在抓取…';
    try {
      const body = {
        ak: ak.value.trim(),
        province: province.value,
        city: city.value || 'all',
        district: district.value || 'all',
        queries: queries.value.trim(),
        qps: qps.value.trim(),
        city_limit: (cityLimit.value === 'true')
      };
      const res = await postJSON('/crawl', body);
      if (res.ok) {
        const lines = (res.per_region || []).map(x => `【${x.region}】→ ${x.inserted_or_updated}`).join('；');
        const errs  = (res.errors || []).map(e => `【${e.region}】${e.error}`).join('；');
        status.textContent = `完成：入库 ${res.inserted_or_updated} 条。${lines}${errs ? '；错误：'+errs : ''}`;
      } else {
        status.textContent = `失败：${res.error || 'unknown'}`;
      }
    } catch (e) { status.textContent = '请求失败：' + e.message; }
  });

  btnRefreshCats.addEventListener('click', async () => {
    const res = await getJSON('/categories');
    catsBox.textContent = JSON.stringify(res, null, 2);
  });
  btnExportMapDb.addEventListener('click', ()=>{ window.open('/export_map_db', '_blank'); });

  btnAddFiles.addEventListener('click', ()=>{ csvPicker.value=''; csvPicker.click(); });
  csvPicker.addEventListener('change', ()=>{ if (csvPicker.files && csvPicker.files.length){ appendFiles(csvPicker.files); } });
  ['dragenter','dragover'].forEach(evtName=>{
    dropZone.addEventListener(evtName, (e)=>{ e.preventDefault(); e.stopPropagation(); dropZone.style.background='#eef3ff'; });
  });
  ['dragleave','drop'].forEach(evtName=>{
    dropZone.addEventListener(evtName, (e)=>{ e.preventDefault(); e.stopPropagation(); dropZone.style.background='#fafafa'; });
  });
  dropZone.addEventListener('drop', (e)=>{ const files = e.dataTransfer.files; if (files && files.length){ appendFiles(files); } });

  btnMerge.addEventListener('click', async ()=>{
    if (selectedFiles.length === 0){ csvStatus.textContent='请先添加 CSV 文件。'; return; }
    csvStatus.textContent='正在上传并合并…'; btnMerge.disabled=true;
    const fd = new FormData(); selectedFiles.forEach(f=>fd.append('files',f));
    try{
      const res = await fetch('/upload_csv',{method:'POST',body:fd});
      const data = await res.json();
      if (!data.ok){ csvStatus.textContent='合并失败：'+(data.msg||'未知错误'); btnMerge.disabled=false; return; }
      currentGeoJSON=data.geojson;
      csvStatus.textContent=`合并成功：${data.total_points} 个点\n`+JSON.stringify(data.files||[],null,2);
      btnExport.disabled=false;
    }catch(e){ csvStatus.textContent='请求失败：'+e.message; }
    finally{ btnMerge.disabled=false; }
  });

  btnExport.addEventListener('click', async ()=>{
    if (!currentGeoJSON){ csvStatus.textContent='没有可导出的数据'; return; }
    csvStatus.textContent='正在导出 HTML 地图…';
    try{
      const payload={title:'合并POI地图',zoom:8,center:[34.0,108.0],geojson:currentGeoJSON};
      const res=await fetch('/export_map',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
      if(!res.ok){ const data=await res.json().catch(()=>({})); csvStatus.textContent='导出失败：'+(data.msg||res.statusText); return;}
      const blob=await res.blob();
      const a=document.createElement('a');
      a.href=URL.createObjectURL(blob);
      a.download=res.headers.get('Content-Disposition')?.match(/filename="?(.+?)"?$/)?.[1]||'merged_map.html';
      a.click();
      csvStatus.textContent='导出完成（已下载 HTML）。';
    }catch(e){ csvStatus.textContent='导出失败：'+e.message; }
  });

  renderFileList();
});
