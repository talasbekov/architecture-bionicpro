import React, { useState } from 'react';
import { submitConsent } from '../api/auth';

interface Props {
  provider?: string;
  onDecided: () => void;
}

const ConsentDialog: React.FC<Props> = ({ provider, onDecided }) => {
  const [busy, setBusy] = useState(false);

  const decide = async (granted: boolean) => {
    setBusy(true);
    try {
      await submitConsent(granted);
      onDecided();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md max-w-md">
        <h2 className="text-xl font-bold mb-4">Разрешение на использование данных</h2>
        <p className="mb-4 text-sm text-gray-700">
          Вы вошли через {provider && provider !== 'keycloak' ? provider : 'учётную запись BionicPRO'}.
          Чтобы формировать отчёты по вашему протезу, нам нужно сохранить данные профиля:
          имя, адрес электронной почты и идентификатор учётной записи.
        </p>
        <p className="mb-6 text-sm text-gray-700">
          Без согласия отчёты будут недоступны, вы сможете дать его позже при следующем входе.
        </p>
        <div className="flex gap-3">
          <button
            onClick={() => decide(true)}
            disabled={busy}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:opacity-50"
          >
            Разрешить
          </button>
          <button
            onClick={() => decide(false)}
            disabled={busy}
            className="px-4 py-2 bg-gray-200 text-gray-800 rounded hover:bg-gray-300 disabled:opacity-50"
          >
            Отказаться
          </button>
        </div>
      </div>
    </div>
  );
};

export default ConsentDialog;
