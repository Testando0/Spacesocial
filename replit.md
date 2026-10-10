# REDSPACE 2026

Aplicativo de mensagens em tempo real em português, estilo WhatsApp — com chat individual, grupos, stories, sistema de amizades e chamadas.

## Run & Operate

- `pnpm --filter @workspace/api-server run dev` — API + Socket.IO (porta 8080)
- `pnpm --filter @workspace/redspace-web run dev` — Frontend Vite (porta dinâmica)
- `pnpm run typecheck` — typecheck completo
- `pnpm run build` — build todos os pacotes

## Stack

- pnpm workspaces, Node.js 24, TypeScript 5.9
- **API**: Express 5 + Socket.IO 4 + PostgreSQL (Replit built-in) + bcryptjs
- **Frontend**: HTML/CSS/JS puro servido pelo Vite (sem framework React)
- **Build**: esbuild (CJS bundle), socket.io externalizado

## Where things live

- `artifacts/api-server/src/server.ts` — servidor completo: rotas, Socket.IO e acesso ao PostgreSQL
- `artifacts/api-server/src/index.ts` — entry point; aguarda o PostgreSQL e chama `server.listen(PORT)`
- `artifacts/api-server/data/` — arquivos locais auxiliares e fila de recuperação de mídia
- `artifacts/api-server/public/uploads/` — mídias enviadas
- `artifacts/api-server/public/avatars/` — fotos de perfil
- `artifacts/redspace-web/index.html` — frontend completo (2593 linhas)
- `artifacts/redspace-web/vite.config.ts` — proxy de todas as rotas de API para porta 8080

## Architecture decisions

- **PostgreSQL** (Replit built-in) via `DATABASE_URL` — banco externo que persiste entre deploys permanentemente
- O servidor falha explicitamente sem `DATABASE_URL` e só aceita requisições depois de concluir o schema e carregar os usuários
- `socket.io` externalizado no esbuild para preservar o serving do `socket.io.js` estático
- Frontend HTML puro (não React) — todo o app em um único `index.html`
- Vite faz proxy interno de `/socket.io`, `/register`, `/login`, etc. → api-server porta 8080
- Autenticação via header `x-username` (stateless, sem JWT/sessão)
- Wrapper `db` em `server.ts` mantém compatibilidade com as rotas legadas e converte placeholders SQLite para PostgreSQL

## Product

- Cadastro e login de usuários
- Chat em tempo real (Socket.IO) com status de entrega
- Grupos com admin, moderadores e membros
- Stories com reações, visualizações e respostas
- Sistema de amizades com solicitação e aceite
- Suporte a mensagens de texto, imagem, áudio e vídeo
- Chamadas de voz/vídeo (WebRTC via Socket.IO)
- Perfil editável com avatar e bio

## User preferences

_Populate as you build — explicit user instructions worth remembering across sessions._

## Gotchas

- `socket.io` DEVE estar na lista `external` do `build.mjs` — se bundleado pelo esbuild, o servidor não consegue servir `socket.io.js` ao cliente
- O schema PostgreSQL é criado de forma idempotente no primeiro start
- O frontend usa o header `x-username` (não Bearer token) para autenticação nas rotas GET
- Rotas do frontend: `/chats/:me`, `/messages/:u1/:u2`, `/friends/:username` — sempre com parâmetros na URL

## Pointers

- See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details
