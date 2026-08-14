import React, { useCallback, useEffect, useState } from 'react';
import {
  getSession,
  login,
  logout,
  requestReport,
  ReportResponse,
  SessionInfo,
} from '../api/auth';
import ConsentDialog from './ConsentDialog';

function isoDaysAgo(days: number): string {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date.toISOString().slice(0, 10);
}

const ReportPage: React.FC = () => {
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [checking, setChecking] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportResponse | null>(null);
  const [from, setFrom] = useState(isoDaysAgo(30));
  const [to, setTo] = useState(isoDaysAgo(1));

  const loadSession = useCallback(async () => {
    setChecking(true);
    try {
      setSession(await getSession());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка');
    } finally {
      setChecking(false);
    }
  }, []);

  useEffect(() => {
    loadSession();
  }, [loadSession]);

  const downloadReport = async () => {
    setLoading(true);
    setError(null);
    setReport(null);
    try {
      const result = await requestReport(from, to);
      setReport(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось получить отчёт');
    } finally {
      setLoading(false);
    }
  };

  if (checking) {
    return <div className="p-8">Загрузка...</div>;
  }

  if (!session) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md text-center">
          <h1 className="text-2xl font-bold mb-4">Отчёты BionicPRO</h1>
          <p className="mb-6 text-sm text-gray-600">
            Для входа используется единая учётная запись BionicPRO.
          </p>
          <button
            onClick={login}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Войти
          </button>
        </div>
      </div>
    );
  }

  if (!session.consentGranted) {
    return <ConsentDialog provider={session.identityProvider} onDecided={loadSession} />;
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-lg">
        <div className="flex justify-between items-start mb-6">
          <div>
            <h1 className="text-2xl font-bold">Отчёт о работе протеза</h1>
            <p className="text-sm text-gray-600">{session.name || session.username}</p>
          </div>
          <button onClick={logout} className="text-sm text-blue-600 hover:underline">
            Выйти
          </button>
        </div>

        <div className="flex gap-4 mb-6">
          <label className="flex flex-col text-sm">
            С
            <input
              type="date"
              value={from}
              onChange={(e) => setFrom(e.target.value)}
              className="border rounded px-2 py-1"
            />
          </label>
          <label className="flex flex-col text-sm">
            По
            <input
              type="date"
              value={to}
              onChange={(e) => setTo(e.target.value)}
              className="border rounded px-2 py-1"
            />
          </label>
        </div>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Готовим отчёт...' : 'Получить отчёт'}
        </button>

        {report && (
          <div className="mt-6 p-4 bg-green-50 rounded text-sm">
            <p className="mb-1">
              Период: {report.period.from} — {report.period.to}
            </p>
            <p className="mb-1 text-gray-600">
              Данные обработаны по {report.dataProcessedUntil}
              {report.cached ? ', отчёт взят из хранилища' : ', отчёт сформирован заново'}
            </p>
            <a
              href={report.reportUrl}
              target="_blank"
              rel="noreferrer"
              className="text-blue-600 hover:underline"
            >
              Скачать отчёт
            </a>
          </div>
        )}

        {error && <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">{error}</div>}
      </div>
    </div>
  );
};

export default ReportPage;
