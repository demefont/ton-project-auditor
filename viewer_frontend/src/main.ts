import { createApp } from "vue";
import { createPinia } from "pinia";
import { router } from "./router";
import App from "./App.vue";
import "./styles.css";

function initTelegramWebApp(): void {
  const webApp = window.Telegram?.WebApp;
  if (!webApp) {
    return;
  }
  const root = document.documentElement;
  const applyViewport = () => {
    const height = Number(webApp.viewportHeight || window.innerHeight || 0);
    const stableHeight = Number(webApp.viewportStableHeight || height || 0);
    if (height > 0) {
      root.style.setProperty("--tg-viewport-height", `${height}px`);
    }
    if (stableHeight > 0) {
      root.style.setProperty("--tg-stable-viewport-height", `${stableHeight}px`);
    }
  };
  const applyTheme = () => {
    const background = String(webApp.themeParams?.bg_color || "#0b1020");
    const secondary = String(webApp.themeParams?.secondary_bg_color || "#0d1324");
    root.style.setProperty("--tg-bg-color", background);
    root.style.setProperty("--tg-panel-color", secondary);
  };
  try {
    webApp.ready();
    webApp.expand();
    webApp.setHeaderColor?.("#0b1020");
    webApp.setBackgroundColor?.("#0b1020");
  } catch {
    return;
  }
  document.body.classList.add("telegram-webapp");
  applyViewport();
  applyTheme();
  webApp.onEvent?.("viewportChanged", applyViewport);
  webApp.onEvent?.("themeChanged", applyTheme);
}

initTelegramWebApp();
createApp(App).use(createPinia()).use(router).mount("#app");
