import { createRouter, createWebHistory } from "vue-router";
import ViewerPage from "../pages/ViewerPage.vue";

export const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      name: "viewer-root",
      component: ViewerPage,
    },
    {
      path: "/runs/:runId",
      name: "viewer-run",
      component: ViewerPage,
    },
    {
      path: "/:pathMatch(.*)*",
      redirect: "/",
    },
  ],
});
