/// <reference types="vite/client" />

declare module "*.vue" {
  import type { DefineComponent } from "vue";
  const component: DefineComponent<Record<string, never>, Record<string, never>, unknown>;
  export default component;
}

interface TelegramBackButton {
  show: () => void;
  hide: () => void;
  onClick?: (callback: () => void) => void;
  offClick?: (callback: () => void) => void;
}

interface TelegramThemeParams {
  bg_color?: string;
  secondary_bg_color?: string;
}

interface TelegramWebApp {
  ready: () => void;
  expand: () => void;
  viewportHeight?: number;
  viewportStableHeight?: number;
  themeParams?: TelegramThemeParams;
  setHeaderColor?: (color: string) => void;
  setBackgroundColor?: (color: string) => void;
  onEvent?: (eventType: string, callback: () => void) => void;
  BackButton?: TelegramBackButton;
}

interface Window {
  Telegram?: {
    WebApp?: TelegramWebApp;
  };
}
