#!/bin/bash
# ============================================================
#  GIT AUTO-PUSH — Faz tudo: login, repo, commit e push
#  Uso: ./gitpush.sh
# ============================================================

set -e

# ---------- CORES ----------
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; B='\033[0;34m'
C='\033[0;36m'; M='\033[0;35m'; N='\033[0m'

# ---------- BANNER ----------
clear
echo -e "${M}"
echo "  ╔══════════════════════════════════════════╗"
echo "  ║        GIT AUTO-PUSH  🚀                 ║"
echo "  ║   Login + Repo + Commit + Push           ║"
echo "  ╚══════════════════════════════════════════╝"
echo -e "${N}"

# ============================================================
# 1) CHECAR DEPENDÊNCIAS
# ============================================================
command -v git >/dev/null 2>&1 || { echo -e "${R}✗ Git não instalado.${N}"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo -e "${R}✗ Curl não instalado.${N}"; exit 1; }

# ============================================================
# 2) LOGIN — TOKEN DO GITHUB
# ============================================================
echo -e "${C}┌─── LOGIN GITHUB ──────────────────────────┐${N}"
echo -e "${Y}  Crie um token em: https://github.com/settings/tokens${N}"
echo -e "${Y}  Marque a permissão: [✓] repo${N}"
echo -e "${C}└───────────────────────────────────────────┘${N}"
echo ""

read -p "👤 Usuário do GitHub: " GH_USER
[ -z "$GH_USER" ] && { echo -e "${R}✗ Usuário obrigatório.${N}"; exit 1; }

read -sp "🔑 Token (não aparece na tela): " GH_TOKEN
echo ""
[ -z "$GH_TOKEN" ] && { echo -e "${R}✗ Token obrigatório.${N}"; exit 1; }

# Valida token
echo -e "\n${B}→ Validando credenciais...${N}"
HTTP=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: token $GH_TOKEN" https://api.github.com/user)
if [ "$HTTP" != "200" ]; then
    echo -e "${R}✗ Token inválido ou sem permissão.${N}"
    exit 1
fi
echo -e "${G}✓ Login OK — bem-vindo, $GH_USER!${N}\n"

# Salva credencial pra não pedir de novo
git config --global credential.helper store
echo "https://$GH_USER:$GH_TOKEN@github.com" > ~/.git-credentials
chmod 600 ~/.git-credentials

# ============================================================
# 3) DADOS DO REPOSITÓRIO
# ============================================================
echo -e "${C}┌─── REPOSITÓRIO ───────────────────────────┐${N}"
read -p "📦 Nome do repositório: " REPO_NAME
[ -z "$REPO_NAME" ] && { echo -e "${R}✗ Nome obrigatório.${N}"; exit 1; }

read -p "📝 Descrição (opcional): " REPO_DESC
read -p "🔒 Privado? (s/N): " PRIV
[[ "$PRIV" =~ ^[Ss]$ ]] && PRIVATE="true" || PRIVATE="false"
echo -e "${C}└───────────────────────────────────────────┘${N}\n"

# ============================================================
# 4) VERIFICA / CRIA REPOSITÓRIO NO GITHUB
# ============================================================
echo -e "${B}→ Verificando se o repositório já existe...${N}"
CHECK=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: token $GH_TOKEN" \
    "https://api.github.com/repos/$GH_USER/$REPO_NAME")

if [ "$CHECK" = "200" ]; then
    echo -e "${Y}⚠ Repositório já existe. Usando o existente.${N}"
else
    echo -e "${B}→ Criando repositório no GitHub...${N}"
    RESP=$(curl -s -X POST \
        -H "Authorization: token $GH_TOKEN" \
        -H "Accept: application/vnd.github+json" \
        https://api.github.com/user/repos \
        -d "{\"name\":\"$REPO_NAME\",\"description\":\"$REPO_DESC\",\"private\":$PRIVATE}")

    if echo "$RESP" | grep -q '"full_name"'; then
        echo -e "${G}✓ Repositório criado: https://github.com/$GH_USER/$REPO_NAME${N}"
    else
        echo -e "${R}✗ Erro ao criar repositório:${N}"
        echo "$RESP" | grep '"message"'
        exit 1
    fi
fi
echo ""

# ============================================================
# 5) CONFIGURA GIT LOCAL
# ============================================================
if [ ! -d ".git" ]; then
    echo -e "${B}→ Inicializando Git...${N}"
    git init -q
fi

git branch -M main 2>/dev/null || true

# Remote (remove se já existir pra evitar conflito)
git remote remove origin 2>/dev/null || true
git remote add origin "https://github.com/$GH_USER/$REPO_NAME.git"

# Identidade (se não tiver configurada globalmente)
git config user.name  >/dev/null 2>&1 || git config user.name  "$GH_USER"
git config user.email >/dev/null 2>&1 || git config user.email "$GH_USER@users.noreply.github.com"

echo -e "${G}✓ Git configurado${N}\n"

# ============================================================
# 6) COMMIT
# ============================================================
if [ -n "$(git status --porcelain)" ]; then
    echo -e "${B}→ Arquivos detectados:${N}"
    git status -s
    echo ""
    read -p "💬 Mensagem do commit (Enter = 'update'): " MSG
    [ -z "$MSG" ] && MSG="update"

    git add -A
    git commit -q -m "$MSG"
    echo -e "${G}✓ Commit criado: $MSG${N}\n"
else
    echo -e "${Y}⚠ Nada novo para commitar.${N}\n"
fi

# ============================================================
# 7) PUSH
# ============================================================
echo -e "${B}→ Enviando para o GitHub...${N}"
if git push -u origin main 2>&1 | grep -v "^remote:"; then
    echo ""
    echo -e "${G}╔══════════════════════════════════════════╗${N}"
    echo -e "${G}║  ✓  TUDO PRONTO! Arquivos no ar! 🎉      ║${N}"
    echo -e "${G}╚══════════════════════════════════════════╝${N}"
    echo -e "${C}  🔗 https://github.com/$GH_USER/$REPO_NAME${N}"
    echo ""
else
    echo -e "${R}✗ Erro no push.${N}"
    exit 1
fi