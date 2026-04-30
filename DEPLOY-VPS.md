# Deploy em VPS Linux (Nginx + Gunicorn)

Stack recomendada para este projeto em producao:

- `Gunicorn` para executar o Flask em `127.0.0.1:8000`.
- `Nginx` como proxy reverso publico na porta `80/443`.
- `systemd` para manter o servico ativo e reiniciar em falhas.

## 1) Preparar servidor (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
```

## 2) Publicar codigo

```bash
sudo mkdir -p /opt/dgp-site
sudo chown -R $USER:$USER /opt/dgp-site
cd /opt/dgp-site
# copie os arquivos do projeto para esta pasta
```

## 3) Ambiente Python

```bash
cd /opt/dgp-site
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4) Variaveis de ambiente

```bash
cp .env.example .env
nano .env
```

Campos importantes para producao:

- `DGP_SECRET_KEY` com valor forte.
- `DGP_PUBLIC_BASE_URL` com URL publica real (`https://seu-dominio`).
- `DGP_DISCORD_REDIRECT_URI` com callback publica real.
- `DGP_TRUST_PROXY=1`
- `DGP_SESSION_COOKIE_SECURE=1` (quando usar HTTPS).

## 5) Subir servico systemd

```bash
sudo cp deploy/systemd/dgp-site.service /etc/systemd/system/dgp-site.service
sudo systemctl daemon-reload
sudo systemctl enable --now dgp-site
sudo systemctl status dgp-site
```

## 6) Configurar Nginx

```bash
sudo cp deploy/nginx/dgp-site.conf /etc/nginx/sites-available/dgp-site
sudo nano /etc/nginx/sites-available/dgp-site  # ajuste server_name
sudo ln -s /etc/nginx/sites-available/dgp-site /etc/nginx/sites-enabled/dgp-site
sudo nginx -t
sudo systemctl reload nginx
```

## 7) HTTPS (recomendado)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d seu-dominio.com -d www.seu-dominio.com
```

Depois do HTTPS ativo, mantenha no `.env`:

- `DGP_PUBLIC_BASE_URL=https://seu-dominio.com`
- `DGP_DISCORD_REDIRECT_URI=https://seu-dominio.com/auth/discord/callback`
- `DGP_SESSION_COOKIE_SECURE=1`

## 8) Logs e diagnostico

```bash
sudo journalctl -u dgp-site -f
sudo tail -f /var/log/nginx/error.log
```
