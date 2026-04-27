#!/usr/bin/env node
/**
 * Coletor diário de engajamento Brightcove + Moodle.
 *
 * Lê:
 *   - Brightcove Analytics API (dimensão video + viewer)
 *   - Brightcove CMS API (metadados de vídeos)
 *   - Moodle Web Services (usuários, cursos, livros)
 *
 * Escreve:
 *   - dashboard/data.json com a estrutura consumida pelo index.html
 *
 * Variáveis de ambiente necessárias:
 *   BC_CLIENT_ID         - OAuth client_id Brightcove
 *   BC_CLIENT_SECRET     - OAuth client_secret Brightcove
 *   BC_ACCOUNT_ID        - Account ID Brightcove (padrão: 1126051577001)
 *   MOODLE_URL           - URL base do Moodle (padrão: https://ead.equalis.com.br)
 *   MOODLE_TOKEN         - Token de Web Service do Moodle (read-only)
 *   LOOKBACK_DAYS        - Quantos dias para trás puxar (padrão: 90)
 */

import { writeFile, readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';

const BC_CLIENT_ID = process.env.BC_CLIENT_ID;
const BC_CLIENT_SECRET = process.env.BC_CLIENT_SECRET;
const BC_ACCOUNT_ID = process.env.BC_ACCOUNT_ID || '1126051577001';
const MOODLE_URL = (process.env.MOODLE_URL || 'https://ead.equalis.com.br').replace(/\/$/, '');
const MOODLE_TOKEN = process.env.MOODLE_TOKEN;
const LOOKBACK_DAYS = parseInt(process.env.LOOKBACK_DAYS || '90', 10);
const OUTPUT = process.env.OUTPUT || path.join(process.cwd(), 'dashboard', 'data.json');

if (!BC_CLIENT_ID || !BC_CLIENT_SECRET) {
  console.error('Faltam BC_CLIENT_ID / BC_CLIENT_SECRET no env.');
  process.exit(1);
}
if (!MOODLE_TOKEN) {
  console.error('Falta MOODLE_TOKEN no env.');
  process.exit(1);
}

const today = new Date();
const fromDate = new Date(today.getTime() - LOOKBACK_DAYS * 86400000);
const fromStr = fromDate.toISOString().slice(0, 10);
const toStr = today.toISOString().slice(0, 10);

console.log(`Coletando dados de ${fromStr} até ${toStr}`);

// ---------- Brightcove ----------

async function bcToken() {
  const auth = Buffer.from(`${BC_CLIENT_ID}:${BC_CLIENT_SECRET}`).toString('base64');
  const res = await fetch('https://oauth.brightcove.com/v4/access_token', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });
  if (!res.ok) throw new Error(`OAuth Brightcove ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.access_token;
}

async function bcAnalytics(token) {
  const all = [];
  let offset = 0;
  const limit = 10000;
  while (true) {
    const url = new URL('https://analytics.api.brightcove.com/v1/data');
    url.searchParams.set('accounts', BC_ACCOUNT_ID);
    url.searchParams.set('dimensions', 'video,viewer');
    url.searchParams.set('fields', [
      'video_view',
      'video_seconds_viewed',
      'video_percent_viewed',
      'video_engagement_25',
      'video_engagement_50',
      'video_engagement_75',
      'video_engagement_100',
      'play_request',
      'play_rate',
    ].join(','));
    url.searchParams.set('from', fromStr);
    url.searchParams.set('to', toStr);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(offset));
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`Analytics ${res.status}: ${await res.text()}`);
    const data = await res.json();
    const items = data.items || [];
    all.push(...items);
    console.log(`  analytics offset=${offset} got=${items.length}`);
    if (items.length < limit) break;
    offset += limit;
    if (offset > 100000) break; // safety
  }
  return all;
}

async function bcVideos(token) {
  const all = [];
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://cms.api.brightcove.com/v1/accounts/${BC_ACCOUNT_ID}/videos?limit=${limit}&offset=${offset}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`CMS ${res.status}: ${await res.text()}`);
    const batch = await res.json();
    all.push(...batch);
    console.log(`  videos offset=${offset} got=${batch.length}`);
    if (batch.length < limit) break;
    offset += limit;
    if (offset > 50000) break; // safety
  }
  return all;
}

// ---------- Moodle ----------

async function moodle(method, params = {}) {
  const url = new URL(`${MOODLE_URL}/webservice/rest/server.php`);
  url.searchParams.set('wstoken', MOODLE_TOKEN);
  url.searchParams.set('wsfunction', method);
  url.searchParams.set('moodlewsrestformat', 'json');
  // Flatten nested params for Moodle
  const flatten = (obj, prefix = '') => {
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix ? `${prefix}[${k}]` : k;
      if (Array.isArray(v)) {
        v.forEach((item, i) => {
          if (typeof item === 'object' && item !== null) flatten(item, `${key}[${i}]`);
          else url.searchParams.append(`${key}[${i}]`, String(item));
        });
      } else if (typeof v === 'object' && v !== null) {
        flatten(v, key);
      } else {
        url.searchParams.append(key, String(v));
      }
    }
  };
  flatten(params);
  const res = await fetch(url);
  const data = await res.json();
  if (data && data.exception) throw new Error(`Moodle ${method}: ${data.message}`);
  return data;
}

async function moodleUsersByUsernames(usernames) {
  if (usernames.length === 0) return [];
  const out = [];
  // chunk
  for (let i = 0; i < usernames.length; i += 100) {
    const chunk = usernames.slice(i, i + 100);
    const data = await moodle('core_user_get_users_by_field', {
      field: 'username',
      values: chunk,
    });
    out.push(...(Array.isArray(data) ? data : []));
  }
  return out;
}

async function moodleCourses() {
  return await moodle('core_course_get_courses');
}

async function moodleCourseContents(courseId) {
  return await moodle('core_course_get_contents', { courseid: courseId });
}

// ---------- Mapeamento videoId -> atividade Moodle ----------

async function buildVideoMap(courses) {
  const map = {};
  let processed = 0;
  for (const course of courses) {
    if (!course.visible) continue;
    try {
      const sections = await moodleCourseContents(course.id);
      for (const section of sections) {
        for (const mod of (section.modules || [])) {
          if (mod.modname !== 'book') continue;
          for (const content of (mod.contents || [])) {
            if (content.content && typeof content.content === 'string') {
              const matches = content.content.matchAll(/videoId=(\d+)/g);
              for (const m of matches) {
                const vid = m[1];
                if (!map[vid]) {
                  map[vid] = {
                    courseId: course.id,
                    courseName: course.fullname,
                    courseShort: course.shortname,
                    bookId: mod.id,
                    bookName: mod.name,
                  };
                }
              }
            }
          }
        }
      }
      processed++;
      if (processed % 10 === 0) console.log(`  cursos processados: ${processed}/${courses.length}`);
    } catch (e) {
      console.warn(`  falha curso ${course.id}: ${e.message}`);
    }
  }
  return map;
}

// ---------- Main ----------

async function main() {
  console.log('Autenticando Brightcove...');
  const token = await bcToken();

  console.log('Coletando analytics (video x viewer)...');
  const analytics = await bcAnalytics(token);
  console.log(`  total rows: ${analytics.length}`);

  console.log('Coletando metadados de vídeos...');
  const videos = await bcVideos(token);
  console.log(`  total videos: ${videos.length}`);
  const videoIndex = {};
  for (const v of videos) {
    videoIndex[v.id] = {
      id: v.id,
      name: v.name,
      duration: v.duration,
      created_at: v.created_at,
      tags: v.tags || [],
      reference_id: v.reference_id,
    };
  }

  console.log('Coletando cursos Moodle...');
  const courses = await moodleCourses();
  console.log(`  total cursos: ${courses.length}`);

  console.log('Mapeando videoId -> atividade Moodle (pode demorar)...');
  const videoMap = await buildVideoMap(courses);
  console.log(`  videos mapeados: ${Object.keys(videoMap).length}`);

  console.log('Coletando dados de alunos Moodle...');
  const usernames = [...new Set(analytics.map(r => r.viewer).filter(u => u && u !== '(unknown)'))];
  console.log(`  viewers unicos: ${usernames.length}`);
  const users = await moodleUsersByUsernames(usernames);
  const userIndex = {};
  for (const u of users) {
    userIndex[u.username] = {
      id: u.id,
      username: u.username,
      fullname: u.fullname,
      email: u.email,
      lastaccess: u.lastaccess,
    };
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    fromDate: fromStr,
    toDate: toStr,
    accountId: BC_ACCOUNT_ID,
    summary: {
      totalRows: analytics.length,
      uniqueViewers: usernames.length,
      uniqueVideos: Object.keys(videoIndex).length,
      mappedVideos: Object.keys(videoMap).length,
      identifiedStudents: Object.keys(userIndex).length,
    },
    videos: videoIndex,
    videoMap,
    users: userIndex,
    rows: analytics.map(r => ({
      videoId: r.video,
      viewer: r.viewer,
      views: r.video_view,
      seconds: r.video_seconds_viewed,
      percent: r.video_percent_viewed,
      eng25: r.video_engagement_25,
      eng50: r.video_engagement_50,
      eng75: r.video_engagement_75,
      eng100: r.video_engagement_100,
      plays: r.play_request,
      playRate: r.play_rate,
    })),
  };

  await mkdir(path.dirname(OUTPUT), { recursive: true });
  await writeFile(OUTPUT, JSON.stringify(payload, null, 2));
  console.log(`OK - escrito ${OUTPUT} (${(JSON.stringify(payload).length / 1024).toFixed(1)} KB)`);
}

main().catch(e => {
  console.error('ERRO:', e);
  process.exit(1);
});
