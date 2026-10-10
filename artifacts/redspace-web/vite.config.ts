import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";
import runtimeErrorOverlay from "@replit/vite-plugin-runtime-error-modal";

const isBuild = process.argv.includes("build");

const rawPort = process.env.PORT;

// PORT only required for dev server, not for production build
const port = rawPort ? Number(rawPort) : 3000;

const API_SERVER_PORT = 8080;
const API_TARGET = `http://localhost:${API_SERVER_PORT}`;

const apiPaths = [
  "/register",
  "/login",
  "/chats",
  "/messages",
  "/user",
  "/users",
  "/stories",
  "/groups",
  "/friends",
  "/update-profile",
  "/update-password",
  "/uploads",
  "/avatars",
  "/api",
  "/cinema",
  "/stickers",
  "/block",
  "/unblock",
  "/blocks",
  "/report",
  "/ban-status",
  "/appeal",
  "/admin",
];

const proxyEntries: Record<string, any> = {
  "/socket.io": {
    target: API_TARGET,
    ws: true,
    changeOrigin: true,
  },
};

for (const p of apiPaths) {
  proxyEntries[p] = {
    target: API_TARGET,
    changeOrigin: true,
  };
}

export default defineConfig({
  base: "/",
  plugins: [
    react(),
    tailwindcss(),
    runtimeErrorOverlay(),
    ...(process.env.NODE_ENV !== "production" &&
    process.env.REPL_ID !== undefined
      ? [
          await import("@replit/vite-plugin-cartographer").then((m) =>
            m.cartographer({
              root: path.resolve(import.meta.dirname, ".."),
            }),
          ),
          await import("@replit/vite-plugin-dev-banner").then((m) =>
            m.devBanner(),
          ),
        ]
      : []),
  ],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "src"),
      "@assets": path.resolve(import.meta.dirname, "..", "..", "attached_assets"),
    },
    dedupe: ["react", "react-dom"],
  },
  root: path.resolve(import.meta.dirname),
  build: {
    outDir: path.resolve(import.meta.dirname, "dist/public"),
    emptyOutDir: true,
  },
  server: {
    port,
    strictPort: true,
    host: "0.0.0.0",
    allowedHosts: true,
    headers: {
      "Permissions-Policy": "display-capture=*",
    },
    proxy: isBuild ? undefined : proxyEntries,
    fs: {
      strict: true,
    },
  },
  preview: {
    port,
    host: "0.0.0.0",
    allowedHosts: true,
    headers: {
      "Permissions-Policy": "display-capture=*",
    },
  },
});
