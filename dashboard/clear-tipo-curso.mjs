#!/usr/bin/env node
/**
 * Limpa o valor "Curso Online" do custom field tipo_de_curso
 * em todos os videos. Mantém ano_de_grava_o e tipo_de_aula intactos.
 */

const BC_CLIENT_ID = process.env.BC_CLIENT_ID;
const BC_CLIENT_SECRET = process.env.BC_CLIENT_SECRET;
const BC_ACCOUNT_ID = process.env.BC_ACCOUNT_ID || '1126051577001';
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
  if (!res.ok) throw new Error(`OAuth ${res.status}`);
  return (await res.json()).access_token;
}

async function listVideos(token, offset, limit = 100) {
  const url = new URL(`https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos`);
  url.searchParams.set('limit', limit);
  url.searchParams.set('offset', offset);
  url.searchParams.set('sort', '-created_at');
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) throw new Error(`list ${res.status}`);
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
    throw new Error(`patch ${id} ${res.status}: ${txt.substring(0, 100)}`);
  }
  return true;
}

async function processInBatches(items, batchSize, worker) {
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    await Promise.all(batch.map(worker));
  }
}

async function main() {
  console.log('Auth Brightcove...');
  const token = await bcToken();

  let offset = 0;
  let totalProcessed = 0;
  let totalCleared = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  while (true) {
    const batch = await listVideos(token, offset, 100);
    if (batch.length === 0) break;

    await processInBatches(batch, PARALLEL, async (v) => {
      const cf = v.custom_fields || {};
      // Só limpa se tipo_de_curso == "Curso Online"
      if (cf.tipo_de_curso !== 'Curso Online') {
        totalSkipped++;
        return;
      }
      try {
        // PATCH com tipo_de_curso vazio
        await patchVideo(token, v.id, { tipo_de_curso: '' });
        totalCleared++;
      } catch (e) {
        totalErrors++;
        console.warn(`  ERRO ${v.id}: ${e.message}`);
      }
    });

    totalProcessed += batch.length;
    offset += batch.length;
    console.log(`offset=${offset} | proc=${totalProcessed} cleared=${totalCleared} skip=${totalSkipped} err=${totalErrors}`);

    if (batch.length < 100) break;
    if (offset > 50000) break;
  }

  console.log('\n=== RESUMO ===');
  console.log(`Total processados: ${totalProcessed}`);
  console.log(`Limpos (tipo_de_curso removido): ${totalCleared}`);
  console.log(`Pulados (não tinham 'Curso Online'): ${totalSkipped}`);
  console.log(`Erros: ${totalErrors}`);
}

main().catch(e => { console.error('ERRO:', e); process.exit(1); });
