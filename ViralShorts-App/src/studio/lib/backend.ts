import {
  isLocalDevHost,
  API,
  FLY_DIRECT_API_PREFIXES,
  STUDIO_SITE_URL,
  STUDIO_AGENT_API,
} from '../shared';

export function resolveStudioBackendUrl(path: string): string {
  const normalized = path.startsWith('/') ? path : `/${path}`;
  if (isLocalDevHost) return `${API}${normalized}`;
  if (FLY_DIRECT_API_PREFIXES.some((prefix) => normalized.startsWith(prefix))) {
    return `${STUDIO_AGENT_API}${normalized}`;
  }
  return `${API}${normalized}`;
}

export const studioAgentOAuthReturnUrl = (): string => {
  if (typeof window === 'undefined') {
    return `${STUDIO_SITE_URL}?page=dashboard&tab=agent`;
  }
  const u = new URL(window.location.href);
  u.searchParams.set('page', 'dashboard');
  u.searchParams.set('tab', 'agent');
  u.searchParams.delete('youtube');
  u.searchParams.delete('youtube_message');
  return u.toString();
};
