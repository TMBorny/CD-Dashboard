/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_APP_BUILD?: string;
  readonly VITE_DATA_MODE?: 'live' | 'static';
  readonly VITE_INTERNAL_API_KEY?: string;
  readonly VITE_SITE_BASE?: string;
  readonly VITE_COURSEDOG_PRD_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
