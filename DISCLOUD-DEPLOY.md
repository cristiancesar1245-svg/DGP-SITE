# Deploy na Discloud

## 1. Ajustar o `discloud.config`

Edite o arquivo `discloud.config`:

```text
NAME=DGP
TYPE=site
ID=seu-subdominio
MAIN=main.py
RAM=512
VERSION=latest
```

- `ID` deve ser apenas o subdominio.
- Exemplo: se a URL for `https://dgp-pmesp.discloud.app`, use `ID=dgp-pmesp`.

## 2. Criar o `.env`

Use `.env.discloud.example` como base.

Exemplo:

```text
DGP_DB_HOST=host-do-seu-banco
DGP_DB_PORT=3306
DGP_DB_DATABASE=nome-do-banco
DGP_DB_USERNAME=usuario-do-banco
DGP_DB_PASSWORD=senha-do-banco

DGP_SECRET_KEY=chave-forte-e-longa
DGP_SESSION_HOURS=12

DGP_LOCAL_ADMIN_USER=admin
DGP_LOCAL_ADMIN_PASSWORD=senha-inicial-forte

DGP_DISCORD_CLIENT_ID=
DGP_DISCORD_CLIENT_SECRET=
DGP_DISCORD_REDIRECT_URI=https://seu-subdominio.discloud.app/auth/discord/callback
DGP_DISCORD_ALLOWED_IDS=
DGP_DISCORD_ADMIN_IDS=
```

## 3. O que precisa ir no upload

Envie a raiz do projeto com estes itens:

- `discloud.config`
- `main.py`
- `requirements.txt`
- `.env`
- pasta `python_app`
- pasta `data`

Arquivos do IIS e da maquina local nao devem subir.

## 4. Gerar o zip

No Windows, rode:

```powershell
powershell -ExecutionPolicy Bypass -File .\build-discloud-package.ps1
```

Isso gera:

```text
dgp-discloud-upload.zip
```

## 5. Banco de dados

O banco local `127.0.0.1:3308` nao existe na Discloud.

Voce precisa usar:

- banco MySQL externo
- ou banco MySQL da sua hospedagem, se houver

## 6. Discord OAuth

Se usar login do Discord, atualize no portal do Discord:

```text
https://seu-subdominio.discloud.app/auth/discord/callback
```

## 7. Observacao importante

Se a Discloud estiver tratando como arquivo estatico, o problema costuma ser um destes:

- `TYPE` errado no `discloud.config`
- `MAIN` errado
- subdominio `ID` nao configurado
- upload sem a raiz correta do projeto
- plano sem suporte a site/API Python
