#!/usr/bin/env node
/**
 * Popula custom_field "ano_de_grava_o" em todos os videos da conta.
 *
 * Estrategia:
 *   1. Tag "gravado-em-XXXX" tem prioridade
 *   2. Tag que e so um ano (4 digitos) entre 2010-2030
 *   3. Fallback: ano do created_at
 *
 * Variaveis:
 *   BC_CLIENT_ID, BC_CLIENT_SECRET, BC_ACCOUNT_ID
 *   DRY_RUN=1 para simular sem patch
 *   ONLY_YEAR=2026 para processar so um ano
 */

const BC_CLIENT_ID = process.env.BC_CLIENT_ID;
const BC_CLIENT_SECRET = process.env.BC_CLIENT_SECRET;
const BC_ACCOUNT_ID = process.env.BC_ACCOUNT_ID || '1126051577001';
const DRY_RUN = process.env.DRY_RUN === '1';
const ONLY_YEAR = process.env.ONLY_YEAR || '';
const VALID_YEARS = ['2018','2019','2020','2021','2022','2023','2024','2025','2026'];
const PARALLEL = 12;

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
  if (!res.ok) throw new Error(`OAuth ${res.status}: ${await res.text()}`);
  return (await res.json()).access_token;
}

async function listVideos(token, offset, limit = 100) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos?limit=${limit}&offset=${offset}&sort=-created_at`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) throw new Error(`list ${res.status}: ${await res.text()}`);
  return res.json();
}

async function patchVideo(token, id, year) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos/${id}`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ custom_fields: { ano_de_grava_o: year } }),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`patch ${id} ${res.status}: ${txt.substring(0, 200)}`);
  }
  return true;
}

function detectYear(video) {
  const tags = video.tags || [];
  // 1. tag gravado-em-YYYY
  for (const t of tags) {
    const m = t.match(/^gravado[-_]em[-_]?(\d{4})$/i);
    if (m && VALID_YEARS.includes(m[1])) return { year: m[1], source: 'tag-gravado' };
  }
  // 2. tag que e so um ano de 4 digitos
  for (const t of tags) {
    if (/^\d{4}$/.test(t) && VALID_YEARS.includes(t)) return { year: t, source: 'tag-year' };
  }
  // 3. created_at
  if (video.created_at) {
    const yr = video.created_at.slice(0, 4);
    if (VALID_YEARS.includes(yr)) return { year: yr, source: 'created_at' };
  }
  return { year: null, source: 'none' };
}

async function main() {
  console.log(`Auth Brightcove (DRY_RUN=${DRY_RUN}, ONLY_YEAR=${ONLY_YEAR || 'all'})...`);
  const token = await bcToken();

  let offset = 0;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  let totalAlreadyOk = 0;
  let totalErrors = 0;
  const sourceStats = { 'tag-gravado': 0, 'tag-year': 0, 'created_at': 0, 'none': 0 };
  const yearStats = {};

  while (true) {
    const batch = await listVideos(token, offset, 100);
    if (batch.length === 0) break;

    // Processa em paralelo (PARALLEL por vez)
    const tasks = batch.map(v => async () => {
      const det = detectYear(v);
      sourceStats[det.source]++;
      if (!det.year) { totalSkipped++; return; }
      yearStats[det.year] = (yearStats[det.year] || 0) + 1;

      // Filtro ONLY_YEAR
      if (ONLY_YEAR && det.year !== ONLY_YEAR) { totalSkipped++; return; }

      // Ja tem o valor correto?
      if (v.custom_fields && v.custom_fields.ano_de_grava_o === det.year) {
        totalAlreadyOk++;
        return;
      }

      if (DRY_RUN) {
        totalUpdated++;
        return;
      }

      try {
        await patchVideo(token, v.id, det.year);
        totalUpdated++;
      } catch (e) {
        totalErrors++;
        console.warn(`  ERRO ${v.id}: ${e.message}`);
      }
    });

    // Run with concurrency limit
    for (let i = 0; i < tasks.length; i += PARALLEL) {
      const slice = tasks.slice(i, i + PARALLEL);
      await Promise.all(slice.map(t => t()));
    }

    totalProcessed += batch.length;
    offset += batch.length;
    console.log(`offset=${offset} | proc=${totalProcessed} updated=${totalUpdated} ok=${totalAlreadyOk} skip=${totalSkipped} err=${totalErrors}`);

    if (batch.length < 100) break;
    if (offset > 50000) break; // safety
  }

  console.log('\n=== RESUMO ===');
  console.log(`Total processados: ${totalProcessed}`);
  console.log(`Atualizados: ${totalUpdated}`);
  console.log(`Ja estavam corretos: ${totalAlreadyOk}`);
  console.log(`Pulados (sem ano detectavel ou fora do filtro): ${totalSkipped}`);
  console.log(`Erros: ${totalErrors}`);
  console.log(`\nFontes de deteccao:`, sourceStats);
  console.log(`Distribuicao por ano:`, yearStats);
}

main().catch(e => { console.error('ERRO:', e); process.exit(1); });
