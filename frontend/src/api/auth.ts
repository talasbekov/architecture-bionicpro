// Фронтенд не знает ничего о токенах Keycloak: он ходит только в bionicpro-auth
// и полагается на сессионную cookie, которую браузер хранит сам.
const AUTH_URL = process.env.REACT_APP_AUTH_URL || 'http://localhost:8000';

export interface SessionInfo {
  authenticated: boolean;
  username: string;
  name?: string;
  email?: string;
  roles: string[];
  identityProvider?: string;
  consentGranted: boolean;
}

async function request(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${AUTH_URL}${path}`, {
    ...init,
    // cookie обязательна для каждого запроса, иначе бэкенд не найдёт сессию
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(init.headers || {}),
    },
  });
}

export async function getSession(): Promise<SessionInfo | null> {
  const response = await request('/auth/session');
  if (response.status === 401) {
    return null;
  }
  if (!response.ok) {
    throw new Error('Не удалось получить данные сессии');
  }
  return response.json();
}

export function login(): void {
  window.location.href = `${AUTH_URL}/auth/login`;
}

export async function logout(): Promise<void> {
  await request('/auth/logout', { method: 'POST' });
  window.location.href = '/';
}

export async function submitConsent(granted: boolean): Promise<void> {
  const response = await request('/auth/consent', {
    method: 'POST',
    body: JSON.stringify({ granted }),
  });
  if (!response.ok) {
    throw new Error('Не удалось сохранить решение');
  }
}

export interface ReportResponse {
  userId: string;
  period: { from: string; to: string };
  dataProcessedUntil: string;
  cached: boolean;
  reportUrl: string;
}

export async function requestReport(from: string, to: string): Promise<ReportResponse> {
  const query = new URLSearchParams({ from, to }).toString();
  const response = await request(`/api/reports?${query}`);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Ошибка ${response.status}`);
  }
  return response.json();
}
