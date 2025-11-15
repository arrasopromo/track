# Templates de payload para n8n → Meta Conversions API

Estes exemplos ajudam a montar os nós no n8n (Function + HTTP Request) para enviar eventos `Contact` e `Purchase` à Conversions API.

## Endpoint

- URL: `https://graph.facebook.com/v19.0/1019661457030791/events`
- Query: `access_token=<SEU_TOKEN>`  
  - Use o token do Pixel ou o token de System User (ambos que você enviou).  
  - Recomendação: guarde o token em credencial do n8n, não em texto.

Opcional (modo teste):
- Inclua `test_event_code` no corpo ao testar.

---

## Webhook de Clique (Contact)

- Recebe do site: `event_id`, `session_id`, `fbc`, `fbp`, `event_source_url`, `timestamp`, UTMs, `user_agent`, etc.
- No n8n, capture também `client_ip_address` do header `x-forwarded-for` ou `x-real-ip` se disponível.

Function (normalização), exemplo:
```js
// Entrada: $json com body do webhook e headers
const body = $json;
const headers = $json.headers || {};

function pickIp(h) {
  return h['x-forwarded-for']?.split(',')[0]?.trim() || h['x-real-ip'] || null;
}

const eventTime = Math.floor((Date.now()) / 1000); // segundos UNIX

return {
  event_name: 'Contact',
  event_id: body.event_id || body.session_id || body.timestamp,
  event_time: eventTime,
  action_source: 'website',
  event_source_url: body.event_source_url || body.page_url,
  client_ip_address: pickIp(headers),
  client_user_agent: headers['user-agent'] || body.user_agent,
  fbc: body.fbc,
  fbp: body.fbp,
  custom_data: {
    utm_source: body.utm_source,
    utm_medium: body.utm_medium,
    utm_campaign: body.utm_campaign,
    utm_content: body.utm_content,
    utm_term: body.utm_term,
    fbclid: body.fbclid
  }
};
```

HTTP Request (POST JSON) → Body:
```json
{
  "data": [
    {
      "event_name": "{{$json.event_name}}",
      "event_id": "{{$json.event_id}}",
      "event_time": {{$json.event_time}},
      "action_source": "{{$json.action_source}}",
      "event_source_url": "{{$json.event_source_url}}",
      "client_ip_address": "{{$json.client_ip_address}}",
      "client_user_agent": "{{$json.client_user_agent}}",
      "user_data": {},
      "custom_data": {{$json.custom_data}},
      "fbc": "{{$json.fbc}}",
      "fbp": "{{$json.fbp}}"
    }
  ],
  "test_event_code": "{{ $json.test_event_code || '' }}"
}
```

---

## Webhook de Pagamento (Purchase)

- Recebe do gateway: `order_id/transaction_id`, `status`, `value`, `currency`, `email`, `phone`, `timestamp`.
- Se o gateway puder enviar `event_id` do clique (ou `session_id`), usaremos para deduplicação; caso contrário, gere um novo.
- Faça hashing SHA‑256 de `email`/`phone` antes de enviar.

Function (normalização + hash), exemplo:
```js
const body = $json;
const headers = $json.headers || {};

function pickIp(h) {
  return h['x-forwarded-for']?.split(',')[0]?.trim() || h['x-real-ip'] || null;
}

function sha256(value) {
  if (!value) return null;
  const crypto = require('crypto');
  return crypto.createHash('sha256').update(String(value).trim().toLowerCase()).digest('hex');
}

const eventTime = Math.floor(( Date.parse(body.timestamp || new Date().toISOString()) ) / 1000);

return {
  event_name: 'Purchase',
  event_id: body.event_id || body.session_id || body.order_id || body.transaction_id,
  event_time: eventTime || Math.floor(Date.now()/1000),
  action_source: 'website',
  event_source_url: body.event_source_url,
  client_ip_address: pickIp(headers),
  client_user_agent: headers['user-agent'],
  fbc: body.fbc,
  fbp: body.fbp,
  user_data: {
    em: sha256(body.email),
    ph: sha256(body.phone)
  },
  custom_data: {
    value: body.value ? Number(body.value) : undefined,
    currency: body.currency || 'BRL',
    order_id: body.order_id,
    transaction_id: body.transaction_id,
    status: body.status
  }
};
```

HTTP Request (POST JSON) → Body:
```json
{
  "data": [
    {
      "event_name": "{{$json.event_name}}",
      "event_id": "{{$json.event_id}}",
      "event_time": {{$json.event_time}},
      "action_source": "{{$json.action_source}}",
      "event_source_url": "{{$json.event_source_url}}",
      "client_ip_address": "{{$json.client_ip_address}}",
      "client_user_agent": "{{$json.client_user_agent}}",
      "user_data": {{$json.user_data}},
      "custom_data": {{$json.custom_data}},
      "fbc": "{{$json.fbc}}",
      "fbp": "{{$json.fbp}}"
    }
  ],
  "test_event_code": "{{ $json.test_event_code || '' }}"
}
```

---

## Configuração no n8n

- No nó HTTP Request, configure:
  - Método: POST
  - URL: `https://graph.facebook.com/v19.0/1019661457030791/events`
  - Query → `access_token`: cole o token (Pixel ou System User)
- Em desenvolvimento, inclua `test_event_code` no corpo (Events Manager → Test Events).
- Guarde os tokens em credenciais (Secret/Env) do n8n e referencie por expressão.

## Observações

- `1019661457030791` é o seu Pixel ID.
- Token do System User e token do Pixel funcionam; recomendo usar o do Pixel para simplicidade.
- Deduplicação: reutilize `event_id` do clique no evento de `Purchase` quando possível.