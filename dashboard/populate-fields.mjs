#!/usr/bin/env node
/**
 * Popula multiplos custom fields em todos os videos:
 *   - ano_de_grava_o: tag gravado-em-XXXX, tag YYYY, ou created_at
 *   - tipo_de_aula: Masterclass | Fórum de Dúvidas | Aula
 *   - tipo_de_curso: Pós Graduação (se tag pos-*) | Curso Online
 *
 * Idempotente: so faz PATCH se algum valor mudou.
 */

const BC_CLIENT_ID = process.env.BC_CLIENT_ID;
const BC_CLIENT_SECRET = process.env.BC_CLIENT_SECRET;
const BC_ACCOUNT_ID = process.env.BC_ACCOUNT_ID || '1126051577001';
const DRY_RUN = process.env.DRY_RUN === '1';
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

async function patchVideo(token, id, customFields) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos/${id}`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ custom_fields: customFields }),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`patch ${id} ${res.status}: ${txt.substring(0, 200)}`);
  }
  return true;
}

function detectYear(video) {
  const tags = video.tags || [];
  for (const t of tags) {
    const m = t.match(/^gravado[-_]em[-_]?(\d{4})$/i);
    if (m && VALID_YEARS.includes(m[1])) return m[1];
  }
  for (const t of tags) {
    if (/^\d{4}$/.test(t) && VALID_YEARS.includes(t)) return t;
  }
  if (video.created_at) {
    const yr = video.created_at.slice(0, 4);
    if (VALID_YEARS.includes(yr)) return yr;
  }
  return null;
}

function detectTipoAula(video) {
  const name = (video.name || '').toLowerCase();
  if (name.includes('masterclass')) return 'Masterclass';
  if (name.includes('fórum') || name.includes('forum')) return 'Fórum de Dúvidas';
  return 'Aula';
}

function detectTipoCurso(video) {
  const tags = video.tags || [];
  // Tag começando com "pos-" indica pós graduação
  for (const t of tags) {
    if (/^pos[-_]/i.test(t)) return 'Pós Graduação';
  }
  // Nome com sigla de pós
  const name = (video.name || '');
  if (/\bP[óo]s\b|^Pós\s|\sPós\s|Pós-Graduação|Pos CMF|Pos CMPA|Pós CMF|Pós CMPA/i.test(name)) {
    return 'Pós Graduação';
  }
  return 'Curso Online';
}

async function processInBatches(items, batchSize, worker) {
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    await Promise.all(batch.map(worker));
  }
}

async function main() {
  console.log(`Auth Brightcove (DRY_RUN=${DRY_RUN})...`);
  const token = await bcToken();

  let offset = 0;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalNoChange = 0;
  let totalErrors = 0;
  const stats = {
    tipo_de_aula: { 'Masterclass': 0, 'Fórum de Dúvidas': 0, 'Aula': 0 },
    tipo_de_curso: { 'Pós Graduação': 0, 'Curso Online': 0 },
    ano_year: {},
    no_year: 0,
  };

  while (true) {
    const batch = await listVideos(token, offset, 100);
    if (batch.length === 0) break;

    await processInBatches(batch, PARALLEL, async (v) => {
      const ano = detectYear(v);
      const tipoAula = detectTipoAula(v);
      const tipoCurso = detectTipoCurso(v);

      stats.tipo_de_aula[tipoAula]++;
      stats.tipo_de_curso[tipoCurso]++;
      if (ano) stats.ano_year[ano] = (stats.ano_year[ano] || 0) + 1;
      else stats.no_year++;

      const cf = v.custom_fields || {};
      const newFields = {};
      let changed = false;

      if (ano && cf.ano_de_grava_o !== ano) { newFields.ano_de_grava_o = ano; changed = true; }
      if (cf.tipo_de_aula !== tipoAula) { newFields.tipo_de_aula = tipoAula; changed = true; }
      if (cf.tipo_de_curso !== tipoCurso) { newFields.tipo_de_curso = tipoCurso; changed = true; }

      if (!changed) { totalNoChange++; return; }

      if (DRY_RUN) { totalUpdated++; return; }

      try {
        await patchVideo(token, v.id, newFields);
        totalUpdated++;
      } catch (e) {
        totalErrors++;
        console.warn(`  ERRO ${v.id}: ${e.message}`);
      }
    });

    totalProcessed += batch.length;
    offset += batch.length;
    console.log(`offset=${offset} | proc=${totalProcessed} updated=${totalUpdated} same=${totalNoChange} err=${totalErrors}`);

    if (batch.length < 100) break;
    if (offset > 50000) break;
  }

  console.log('\n=== RESUMO ===');
  console.log(`Total processados: ${totalProcessed}`);
  console.log(`Atualizados: ${totalUpdated}`);
  console.log(`Sem mudança: ${totalNoChange}`);
  console.log(`Erros: ${totalErrors}`);
  console.log(`\nDistribuição tipo_de_aula:`, stats.tipo_de_aula);
  console.log(`Distribuição tipo_de_curso:`, stats.tipo_de_curso);
  console.log(`Distribuição ano_de_grava_o:`, stats.ano_year);
  console.log(`Sem ano detectável: ${stats.no_year}`);
}

main().catch(e => { console.error('ERRO:', e); process.exit(1); });
