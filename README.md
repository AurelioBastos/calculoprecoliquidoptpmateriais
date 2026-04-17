# NF-e · Cálculo de Preço Líquido
**Deploy no Render — Guia completo**

---

## Estrutura do projeto

```
nfe_render/
├── main.py          ← Backend FastAPI (toda a lógica de cálculo)
├── nfe_app.html     ← Frontend (servido pelo próprio backend)
├── requirements.txt ← Dependências Python
├── render.yaml      ← Configuração de deploy automático
└── README.md        ← Este arquivo
```

---

## Passo 1 — Criar conta no GitHub

1. Acesse **https://github.com** e crie uma conta gratuita (se ainda não tiver).
2. Clique em **"New repository"** (botão verde no canto superior esquerdo).
3. Nome: `nfe-preco-liquido`
4. Deixe como **Private** (privado — só você acessa o código).
5. Clique em **"Create repository"**.

---

## Passo 2 — Subir os arquivos no GitHub

Após criar o repositório, o GitHub mostrará uma página com instruções.
Clique em **"uploading an existing file"** e arraste os 4 arquivos:

- `main.py`
- `nfe_app.html`
- `requirements.txt`
- `render.yaml`

Clique em **"Commit changes"** (botão verde).

---

## Passo 3 — Criar conta no Render

1. Acesse **https://render.com**
2. Clique em **"Get Started for Free"**
3. Faça login com sua conta do **GitHub** (opção mais simples — clique em "Continue with GitHub")
4. Autorize o Render a acessar seu GitHub.

---

## Passo 4 — Criar o serviço no Render

1. No painel do Render, clique em **"New +"** → **"Web Service"**
2. Conecte ao repositório `nfe-preco-liquido` (clique em "Connect")
3. Preencha:
   - **Name:** `nfe-preco-liquido`
   - **Region:** `Oregon (US West)` ou `Frankfurt (EU Central)` — qualquer um
   - **Branch:** `main`
   - **Runtime:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Em **Instance Type**, selecione **Free**
5. Clique em **"Create Web Service"**

O Render vai buildar e publicar automaticamente. Aguarde ~3 minutos.

---

## Passo 5 — Acessar o app

Após o deploy, o Render exibe uma URL no topo da página, parecida com:

```
https://nfe-preco-liquido.onrender.com
```

**Pronto!** Compartilhe essa URL com quem precisar acessar o app.

---

## Observações importantes

### App "adormece" no plano gratuito
No plano Free do Render, o app fica inativo após **15 minutos sem uso**.
Na próxima vez que alguém acessar, ele demora ~30 segundos para "acordar".

**Solução:** Plano Starter ($7/mês) mantém o app sempre ativo.
Ou avise os usuários para aguardar o carregamento inicial.

### Segurança
- O app **não salva nenhuma informação** — tudo é processado em memória.
- Os XMLs são processados e descartados a cada requisição.
- Para adicionar senha de acesso, informe e posso adicionar autenticação básica.

### Atualizar o app
Para atualizar qualquer arquivo, basta subir a nova versão no GitHub.
O Render detecta automaticamente e faz o redeploy.

---

## Solução de problemas

| Problema | Solução |
|---|---|
| "Application error" no Render | Verifique os logs em "Logs" no painel do Render |
| App lento no primeiro acesso | Normal no plano Free — aguarde ~30s |
| "Colunas não encontradas" no Confronto PC | Verifique se o Excel do PC tem as colunas: `Documento`, `Item`, `Vl.Líq.Unit.`, `Aliq.ICMS`, `Aliq.IPI`, `Aliq.ST ICMS`, `NCM`, `Origem` |
| Erro ao processar XML | Certifique-se que os arquivos são XMLs de NF-e válidos |
