#!/usr/bin/env node
/**
 * Coletor diário de engajamento Brightcove + Moodle.
 */

import { writeFile, mkdir } from 'node:fs/promises';
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

console.log(`Coletando dados de ${fromStr} ate ${toStr}`);

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

async function bcAnalyticsByVideoViewer(token) {
  // A dimensao viewer aceita apenas um subset de metricas.
  // Usamos as mais essenciais que sao seguras.
  const fields = ['video_view', 'video_seconds_viewed'].join(',');
  const all = [];
  let offset = 0;
  const limit = 10000;
  while (true) {
    const url = new URL('https://analytics.api.brightcove.com/v1/data');
    url.searchParams.set('accounts', BC_ACCOUNT_ID);
    url.searchParams.set('dimensions', 'video,viewer');
    url.searchParams.set('fields', fields);
    url.searchParams.set('from', fromStr);
    url.searchParams.set('to', toStr);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(offset));
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`Analytics video,viewer ${res.status}: ${await res.text()}`);
    const data = await res.json();
    const items = data.items || [];
    all.push(...items);
    console.log(`  analytics video,viewer offset=${offset} got=${items.length}`);
    if (items.length < limit) break;
    offset += limit;
    if (offset > 100000) break;
  }
  return all;
}

async function bcAnalyticsByVideo(token) {
  // Para os campos de engagement (que nao funcionam com viewer), fazemos um agregado por video apenas.
  const fields = [
    'video_view',
    'video_seconds_viewed',
    'video_percent_viewed',
    'video_engagement_25',
    'video_engagement_50',
    'video_engagement_75',
    'video_engagement_100',
    'play_request',
  ].join(',');
  const all = [];
  let offset = 0;
  const limit = 10000;
  while (true) {
    const url = new URL('https://analytics.api.brightcove.com/v1/data');
    url.searchParams.set('accounts', BC_ACCOUNT_ID);
    url.searchParams.set('dimensions', 'video');
    url.searchParams.set('fields', fields);
    url.searchParams.set('from', fromStr);
    url.searchParams.set('to', toStr);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(offset));
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!res.ok) throw new Error(`Analytics video ${res.status}: ${await res.text()}`);
    const data = await res.json();
    const items = data.items || [];
    all.push(...items);
    console.log(`  analytics video offset=${offset} got=${items.length}`);
    if (items.length < limit) break;
    offset += limit;
    if (offset > 100000) break;
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
    if (offset > 50000) break;
  }
  return all;
}

async function moodle(method, params = {}) {
  const url = new URL(`${MOODLE_URL}/webservice/rest/server.php`);
  url.searchParams.set('wstoken', MOODLE_TOKEN);
  url.searchParams.set('wsfunction', method);
  url.searchParams.set('moodlewsrestformat', 'json');
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

async function main() {
  console.log('Autenticando Brightcove...');
  const token = await bcToken();

  console.log('Coletando analytics por video...');
  const byVideo = await bcAnalyticsByVideo(token);
  console.log(`  total videos com trafego: ${byVideo.length}`);
  const videoStats = {};
  for (const v of byVideo) {
    videoStats[v.video] = {
      views: v.video_view,
      seconds: v.video_seconds_viewed,
      percent: v.video_percent_viewed,
      eng25: v.video_engagement_25,
      eng50: v.video_engagement_50,
      eng75: v.video_engagement_75,
      eng100: v.video_engagement_100,
      plays: v.play_request,
    };
  }

  console.log('Coletando analytics por video x viewer...');
  const byViewer = await bcAnalyticsByVideoViewer(token);
  console.log(`  total rows video x viewer: ${byViewer.length}`);

  console.log('Coletando metadados de videos...');
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
  const usernames = [...new Set(byViewer.map(r => r.viewer).filter(u => u && u !== '(unknown)'))];
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
      totalRows: byViewer.length,
      uniqueViewers: usernames.length,
      uniqueVideos: Object.keys(videoStats).length,
      mappedVideos: Object.keys(videoMap).length,
      identifiedStudents: Object.keys(userIndex).length,
    },
    videos: videoIndex,
    videoStats,
    videoMap,
    users: userIndex,
    rows: byViewer.map(r => ({
      videoId: r.video,
      viewer: r.viewer,
      views: r.video_view,
      seconds: r.video_seconds_viewed,
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
