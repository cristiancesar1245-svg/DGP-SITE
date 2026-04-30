# DGP PM

Sistema Python + HTML para gestao do Departamento de Gestao Pessoal da Policia Militar.

## Modulos

- Analytics com totais de membros, inscricoes e pagamentos.
- Registro de membros do departamento.
- Financeiro para controle de pagamentos, descontos e status.
- Inscricao no departamento com analise de solicitacoes.

## Requisitos

- Python 3.14 ou superior.
- MySQL 8 em `127.0.0.1:3308`.
- Pacotes Python em `requirements.txt`.

## Setup Python

```bash
python -m pip install -r requirements.txt
python python_app/app.py
```

Ou use no Windows:

```text
run-python-site.bat
```

Abra:

```text
http://127.0.0.1:8080
```

## Deploy na Discloud

Arquivos preparados para a Discloud:

- `main.py`: sobe o Flask em `0.0.0.0` na porta `8080`.
- `discloud.config`: configuracao da app `TYPE=site`.
- `.discloudignore`: evita enviar arquivos locais e do IIS.

Antes do upload:

1. Troque `ID=troque-pelo-seu-subdominio` no `discloud.config` pelo subdominio reservado na Discloud.
2. Ajuste o `.env` da hospedagem com os valores reais do banco e da chave secreta.
3. Se for usar Discord, ajuste `DGP_DISCORD_REDIRECT_URI` para o endereco publico da Discloud.

Exemplo:

```text
https://seu-subdominio.discloud.app/auth/discord/callback
```

Observacoes:

- A Discloud exige `TYPE=site`, subdominio registrado e a aplicacao ouvindo em `0.0.0.0:8080`.
- Para deploy, envie a raiz do projeto com `discloud.config` e `requirements.txt`.
- Consulte tambem `DISCLOUD-DEPLOY.md` para o passo a passo curto e o zip de upload.

## Deploy em VPS Linux (publico)

Melhor opcao para VPS Linux:

- `Gunicorn` executando a aplicacao Flask internamente.
- `Nginx` como proxy reverso publico (porta 80/443).
- `systemd` para manter o servico ativo.

Arquivos prontos incluidos no projeto:

- `deploy/systemd/dgp-site.service`
- `deploy/nginx/dgp-site.conf`
- `DEPLOY-VPS.md` (passo a passo completo)

Fluxo resumido:

1. Instalar dependencias (`python3`, `venv`, `nginx`).
2. Criar `.venv` e instalar `requirements.txt`.
3. Ajustar `.env` (principalmente `DGP_PUBLIC_BASE_URL` e callback do Discord).
4. Publicar service do systemd e iniciar `dgp-site`.
5. Publicar site do Nginx e recarregar.
6. Ativar HTTPS com Certbot.

## Login com Discord

O sistema agora possui:

- login local por usuario e senha;
- login com Discord OAuth2;
- painel `Acessos` para liberar, bloquear, criar usuarios locais e redefinir senha.

### Acesso local inicial

Se ainda nao houver outro administrador local criado, o sistema garante um acesso inicial:

```text
usuario: admin
senha: admin123
```

Altere esse acesso no painel `Acessos` assim que entrar.

### Ativar Discord

1. Crie uma aplicacao no portal do Discord.
2. Em OAuth2, cadastre o redirecionamento:

```text
http://127.0.0.1:8080/auth/discord/callback
```

3. Copie `.env.example` para `.env` e preencha:

```text
DGP_SECRET_KEY=sua-chave-segura
DGP_LOCAL_ADMIN_USER=admin
DGP_LOCAL_ADMIN_PASSWORD=admin123
DGP_DISCORD_CLIENT_ID=seu-client-id
DGP_DISCORD_CLIENT_SECRET=seu-client-secret
DGP_DISCORD_REDIRECT_URI=http://127.0.0.1:8080/auth/discord/callback
```

4. Opcionalmente, limite o acesso a contas especificas:

```text
DGP_DISCORD_ALLOWED_IDS=123456789012345678,987654321098765432
```

5. Defina pelo menos um administrador inicial para liberar os demais usuarios:

```text
DGP_DISCORD_ADMIN_IDS=123456789012345678
```

Quando `DGP_DISCORD_CLIENT_ID` e `DGP_DISCORD_CLIENT_SECRET` estiverem preenchidos, o painel interno passa a exigir login com Discord automaticamente.

O primeiro login do administrador cria um usuario ativo no banco. Os demais usuarios entram com a conta do Discord e ficam `pendente` ate um administrador liberar em `Acessos`.

## Banco de dados

O app Python le os dados do banco MySQL `dgp`, usuario `root`, porta `3308`.

Para criar as tabelas e importar o arquivo financeiro usando somente Python:

```bash
python python_app/init_db.py
```

Para recriar tudo do zero e importar novamente:

```bash
python python_app/init_db.py --reset
```

## Estrutura

- `python_app/app.py`: aplicacao web Flask.
- `python_app/init_db.py`: criacao do banco/tabelas e importacao dos dados.
- `python_app/templates`: paginas HTML.
- `python_app/static`: CSS e imagens.
- `data/message.txt`: dados financeiros importados.
