#!/usr/bin/env node
/**
 * Calcula tamanho total dos videos com tipo_de_aula="Fórum de Dúvidas"
 * em determinados anos. Soma:
 *  - digital_master.size (master original)
 *  - sources MP4 progressive
 *
 * Imprime resumo agregado por ano.
 */

const BC_CLIENT_ID = process.env.BC_CLIENT_ID;
const BC_CLIENT_SECRET = process.env.BC_CLIENT_SECRET;
const BC_ACCOUNT_ID = process.env.BC_ACCOUNT_ID || '1126051577001';
const YEARS = (process.env.YEARS || '2020,2021,2022,2023,2024').split(',');
const PARALLEL = 10;

if (!BC_CLIENT_ID || !BC_CLIENT_SECRET) {
  console.error('Faltam BC_CLIENT_ID / BC_CLIENT_SECRET');
  process.exit(1);
}

async function bcToken() {
  const auth = Buffer.from(`${BC_CLIENT_ID}:${BC_CLIENT_SECRET}`).toString('base64');
  const res = await fetch('https://oauth.brightcove.com/v4/access_token', {
    method: 'POST',
    headers: { 'Authorization': `Basic ${auth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'grant_type=client_credentials',
  });
  if (!res.ok) throw new Error(`OAuth ${res.status}`);
  return (await res.json()).access_token;
}

async function listVideos(token, q, offset, limit = 100) {
  const url = new URL(`https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos`);
  url.searchParams.set('q', q);
  url.searchParams.set('limit', limit);
  url.searchParams.set('offset', offset);
  url.searchParams.set('sort', '-created_at');
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) throw new Error(`list ${res.status}: ${await res.text()}`);
  return res.json();
}

async function getDigitalMaster(token, videoId) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos/${videoId}/digital_master`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (res.status === 404) return null;
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`digital_master ${videoId} ${res.status}: ${txt.substring(0, 100)}`);
  }
  return res.json();
}

async function getSources(token, videoId) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos/${videoId}/sources`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) return [];
  return res.json();
}

async function processInBatches(items, batchSize, worker) {
  const out = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const results = await Promise.all(batch.map(worker));
    out.push(...results);
  }
  return out;
}

function fmtBytes(b) {
  if (!b) return '0 B';
  if (b < 1024) return b + ' B';
  if (b < 1024**2) return (b/1024).toFixed(1) + ' KB';
  if (b < 1024**3) return (b/1024**2).toFixed(1) + ' MB';
  if (b < 1024**4) return (b/1024**3).toFixed(2) + ' GB';
  return (b/1024**4).toFixed(2) + ' TB';
}

async function main() {
  console.log(`Anos: ${YEARS.join(', ')}`);
  console.log('Auth Brightcove...');
  const token = await bcToken();

  // Coleta todos os videos de forum nos anos solicitados
  const allVideos = [];
  for (const year of YEARS) {
    const q = `+custom_fields:"Fórum de Dúvidas" +custom_fields:${year}`;
    let offset = 0;
    while (true) {
      const batch = await listVideos(token, q, offset, 100);
      if (batch.length === 0) break;
      for (const v of batch) {
        allVideos.push({ id: v.id, name: v.name, year, duration: v.duration });
      }
      if (batch.length < 100) break;
      offset += 100;
    }
    console.log(`  ${year}: encontrados ${allVideos.filter(v => v.year === year).length}`);
  }
  console.log(`\nTotal videos: ${allVideos.length}\n`);

  // Pega digital master + sources de cada
  let processed = 0;
  const sizesByYear = {};
  let totalMaster = 0;
  let totalProgressive = 0;
  let countWithMaster = 0;
  let countWithoutMaster = 0;
  let totalDuration = 0;

  await processInBatches(allVideos, PARALLEL, async (v) => {
    try {
      const [master, sources] = await Promise.all([
        getDigitalMaster(token, v.id),
        getSources(token, v.id),
      ]);

      const masterSize = master && master.size ? master.size : 0;
      // MP4 progressive size (geralmente o maior MP4 entre os sources)
      let progSize = 0;
      for (const s of sources) {
        if (s.container === 'MP4' && s.size && s.size > progSize) progSize = s.size;
      }

      if (masterSize > 0) countWithMaster++; else countWithoutMaster++;
      totalMaster += masterSize;
      totalProgressive += progSize;
      totalDuration += v.duration || 0;

      sizesByYear[v.year] = sizesByYear[v.year] || { count: 0, master: 0, progressive: 0, duration: 0 };
      sizesByYear[v.year].count++;
      sizesByYear[v.year].master += masterSize;
      sizesByYear[v.year].progressive += progSize;
      sizesByYear[v.year].duration += v.duration || 0;
    } catch (e) {
      console.warn(`  ERRO ${v.id}: ${e.message}`);
    }
    processed++;
    if (processed % 50 === 0) console.log(`  progresso: ${processed}/${allVideos.length}`);
  });

  console.log('\n=== RESUMO POR ANO ===');
  for (const year of YEARS) {
    const s = sizesByYear[year];
    if (!s) continue;
    console.log(`${year}: ${s.count} videos | master=${fmtBytes(s.master)} | progressive=${fmtBytes(s.progressive)} | ${(s.duration/1000/3600).toFixed(1)}h`);
  }

  console.log('\n=== TOTAL ===');
  console.log(`Videos: ${allVideos.length}`);
  console.log(`Com digital master: ${countWithMaster}`);
  console.log(`Sem digital master: ${countWithoutMaster}`);
  console.log(`Duração total: ${(totalDuration/1000/3600).toFixed(1)} horas`);
  console.log(`Digital master total: ${fmtBytes(totalMaster)}`);
  console.log(`MP4 progressive total: ${fmtBytes(totalProgressive)}`);
  console.log(`Estimativa storage Brightcove (master + ~2x renditions): ${fmtBytes(totalMaster * 3)}`);
}

main().catch(e => { console.error('ERRO:', e); process.exit(1); });
