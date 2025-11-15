# Tracking Meta Ads → LP → WhatsApp

Este projeto coleta parâmetros de campanha (UTMs, `fbclid`) e cria cookies `fbc`/`fbp` para uso em tracking avançado e posterior integração via webhook com o n8n.

## Como usar

- Edite `assets/js/config.js` e defina `webhookUrl` para `http://localhost:8080/api/track` (backend local).
- Opcional: ajuste `defaultWhatsAppPhone` e `defaultMessage`.
- Você também pode definir `data-whatsapp` e `data-message` no botão da LP (`index.html`).

## Backend local com MongoDB

- Variáveis de ambiente:
  - `MONGO_URI` (ex.: `mongodb://usuario:senha@host:27017/?authSource=admin`)
  - `MONGO_DB_NAME` (padrão: `track`)
- Rotas do backend:
  - `POST /api/track` → recebe evento de clique/auto-redirect e salva em `sessions`.
  - `POST /webhook/botconversa` → recebe mensagem inbound do WhatsApp, extrai `cliente#ID` e associa `user_phone` ao registro da sessão.
  - `POST /webhook/payment` → recebe pagamento, salva em `payments` e tenta vincular à sessão por `event_id`/`client_ref`/`phone`.
- CORS: o backend responde `OPTIONS` e adiciona `Access-Control-Allow-*` para facilitar testes locais.

## n8n rapidamente com seu Pixel ID

- Seu `pixel_id`: `1019661457030791`.
- Tokens: você forneceu um do System User e um do Pixel. Use o do Pixel para começar; guarde o token em credencial do n8n.
- Templates de payload prontos em `n8n/capi_payload_templates.md` (copie/cole nos nós Function e HTTP Request).
- HTTP Request:
  - URL: `https://graph.facebook.com/v19.0/1019661457030791/events`
  - Query: `access_token=<SEU_TOKEN>`
  - Body: usar os exemplos dos templates (`Contact` para clique; `Purchase` para pagamento).

### Passos sugeridos
- Crie dois workflows ou dois webhooks no mesmo workflow:
  - `POST /webhook/meta-ads-whatsapp` (Clique): normaliza e envia `Contact` com `event_id`.
  - `POST /webhook/payment` (Pagamento): normaliza e envia `Purchase`, usa `event_id` se disponível para dedup.
- Ative o Test Events no Events Manager e preencha `test_event_code` nos bodies dos requests durante validação.
- Depois de validar, remova o `test_event_code` dos bodies.

## Dados enviados ao webhook

- `event_name`: `whatsapp_click`
- `event_id`: gerado no clique (para deduplicação CAPI)
- `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, `utm_term`
- `fbclid`, `fbc`, `fbp`
- `gclid`, `msclkid` (se presentes)
- `page_url`, `event_source_url`, `referrer`, `user_agent`, `timestamp`
- `session_id`: persistido em cookie `sid`
- `whatsapp_destination` (número do WhatsApp da empresa)
- `message`
- `client_ip` (quando disponível via ipify)
- `message_reference` (ex.: `cliente#10323`)
- `client_ref` (ex.: `10323`)

## Associação no WhatsApp

- Referência por cliente: você pode usar texto como `abc cliente#10323`. O site detecta essa referência e envia `message_reference` e `client_ref` no webhook.
- Alternativa com token curto: opcionalmente, `appendTrackingTokenToMessage: true` adiciona `#e:AB12CD34` derivado de `event_id` ao texto.
- No webhook de entrada do WhatsApp (Cloud API/BSP), faça o parsing do texto e associe ao registro salvo:

```js
const body = $json;
const text = body.text || body.message || '';
const mCliente = text.match(/cliente#([A-Za-z0-9_-]+)/i);
const clientRef = mCliente ? mCliente[1] : null;
const mEvt = text.match(/#e:([A-Z0-9]{8})/);
const evtToken = mEvt ? mEvt[1] : null;
return { clientRef, evtToken, text, from: body.from };
```

- Com `clientRef` (ou `evtToken`), procure a sessão no seu storage (Data Store/DB) e associe `from` (telefone do cliente) com `event_id/session_id`. Depois, reutilize isso no webhook de pagamento para enviar `Purchase` com deduplicação.

## Execução com Node

- Requisitos: Node 16+.
- Instalação de dependências: `npm install` (instala `mongodb`).
- Execução:
  - `setx MONGO_URI "mongodb://usuario:senha@host:27017/?authSource=admin"`
  - `setx MONGO_DB_NAME "track"`
  - Reinicie o terminal (para carregar env) e rode `node server.js`.
  - Acesse: `http://localhost:8080/`.

## n8n (exemplo de fluxo)

- Node: Webhook (POST) → Função para normalizar payload → (opcional) Meta Conversions API → Banco/Planilha/CRM.
- Se for enviar para o Meta CAPI, aproveite `fbc`/`fbp`, `event_source_url`, `user_agent` e `ip` (capturar do `webhook` no n8n).

### Requisitos do Meta Ads para CAPI
- `Pixel ID` (do Events Manager).
- `Access Token` da Conversions API (não exponha no frontend; guarde no n8n/credencial).  
- `Test Event Code` para validar em modo teste.
- `Verified Domain` (recomendado) e uso de `event_source_url` de seu domínio.
- `event_name` mapeado (sugestões: `Contact` no clique do WhatsApp, `Lead` em qualificação, `Purchase` no webhook de pagamento).
- `event_id` para deduplicar entre web e servidor.
- `action_source` = `website`.

### Dados recomendados no webhook do gateway de pagamento
- `order_id`/`transaction_id`, `status`, `value`, `currency`.
- Identificadores do cliente: `email`, `phone` (ideal para `user_data` na CAPI; enviar como SHA256).  
- `timestamp` do pagamento.  
- `ip` e `user_agent` (se disponíveis).  
- Algum vínculo com a sessão: `session_id` ou `event_id` do clique (podemos propagar do n8n).

### Deduplicação e associação de eventos
- O `event_id` gerado no clique do WhatsApp pode ser guardado no n8n e reutilizado no evento `Purchase` do pagamento para deduplicar entre web e server.  
- O `session_id` (cookie `sid`) ajuda a associar múltiplos eventos do usuário.

### Boas práticas
- Nunca exponha `Access Token` da Meta no frontend.  
- Faça hashing SHA-256 de `email`/`phone` no n8n antes de enviar à CAPI (`user_data.em`, `user_data.ph`).
- Capture `client_ip_address` e `client_user_agent` no node Webhook do n8n.
- Use `fbc`/`fbp`, `event_source_url`, `action_source`=`website` e `event_time` do lado servidor.

## Observações

- O texto do WhatsApp é preenchido com UTMs para facilitar identificação pelo atendente.
- Se houver `fbclid` na URL, o cookie `fbc` é gerado automaticamente.
- `fbp` é sempre gerado caso não exista.
- Tokens (Access Token) devem permanecer no n8n (seguro) e nunca no frontend.
- Execução com Node
  - Requisitos: Node 16+.
  - Instalação: não há dependências; basta ter Node instalado.
  - Rodar: `node server.js` (porta padrão `8080`) ou `PORT=8000 node server.js`.
  - Acesse: `http://localhost:8080/` (ou porta definida).