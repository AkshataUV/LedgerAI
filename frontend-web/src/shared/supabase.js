import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

function isValidHttpUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

const isPlaceholderUrl =
  typeof supabaseUrl === 'string' &&
  (supabaseUrl.includes('your_supabase_url') || supabaseUrl.includes('<your_supabase_url>'));

const isPlaceholderKey =
  typeof supabaseAnonKey === 'string' &&
  (supabaseAnonKey.includes('your_supabase_anon_key') || supabaseAnonKey.includes('<your_supabase_anon_key>'));

export const supabaseConfigError =
  !supabaseUrl || !supabaseAnonKey
    ? 'Missing Supabase env vars. Set VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY in frontend-web/.env.'
    : isPlaceholderUrl || isPlaceholderKey
      ? 'Supabase env vars still use placeholder values. Update VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY in frontend-web/.env.'
      : !isValidHttpUrl(supabaseUrl)
        ? 'Invalid VITE_SUPABASE_URL. It must be a valid HTTP/HTTPS URL (for example: https://<project-ref>.supabase.co).'
        : null;

export const supabase = supabaseConfigError
  ? null
  : createClient(supabaseUrl, supabaseAnonKey, {
      auth: {
        storage: typeof window !== 'undefined' ? window.localStorage : undefined,
        autoRefreshToken: true,
        persistSession: true,
        detectSessionInUrl: false,
      },
    });