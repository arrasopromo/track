(function () {
  function buildMessage(base, data, append) {
    if (!append) return base;
    var lines = [];
    if (data.utm_source) lines.push('utm_source=' + data.utm_source);
    if (data.utm_medium) lines.push('utm_medium=' + data.utm_medium);
    if (data.utm_campaign) lines.push('utm_campaign=' + data.utm_campaign);
    if (data.utm_content) lines.push('utm_content=' + data.utm_content);
    if (data.utm_term) lines.push('utm_term=' + data.utm_term);
    if (data.fbclid) lines.push('fbclid=' + data.fbclid);
    return base + (lines.length ? '\n\n' + lines.join('\n') : '');
  }

  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var ts = Date.now().toString(16);
    var rnd = Math.floor(Math.random() * 1e12).toString(16);
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    }) + '-' + ts + '-' + rnd;
  }


  function waLink(phone, message) {
    var clean = (phone || '').replace(/[^0-9]/g, '');
    return 'https://wa.me/' + clean + '?text=' + encodeURIComponent(message || '');
  }

  function waDeepLink(phone, message) {
    var clean = (phone || '').replace(/[^0-9]/g, '');
    return 'whatsapp://send?phone=' + clean + '&text=' + encodeURIComponent(message || '');
  }

  function androidIntentLink(phone, message) {
    var clean = (phone || '').replace(/[^0-9]/g, '');
    var txt = encodeURIComponent(message || '');
    var fb = encodeURIComponent('https://wa.me/' + clean + '?text=' + txt);
    return 'intent://send/?phone=' + clean + '&text=' + txt + '#Intent;scheme=whatsapp;package=com.whatsapp;S.browser_fallback_url=' + fb + ';end';
  }

  function isMobile() {
    var ua = navigator.userAgent || '';
    return /Android|iPhone|iPad|iPod|Windows Phone/i.test(ua);
  }

  function isAndroid() {
    var ua = navigator.userAgent || '';
    return /Android/i.test(ua);
  }

  function isIOS() {
    var ua = navigator.userAgent || '';
    return /iPhone|iPad|iPod/i.test(ua);
  }

  function postWebhook(url, payload) {
    if (!url) return Promise.resolve();
    var body = JSON.stringify(payload);
    var headers = { 'Content-Type': 'application/json' };
    return fetch(url, { method: 'POST', headers: headers, body: body, keepalive: true, mode: 'cors' }).catch(function () {
      if (navigator.sendBeacon) {
        try { var blob = new Blob([body], { type: 'application/json' }); navigator.sendBeacon(url, blob); } catch (e) {}
      }
    });
  }

  function postWebhookAndRead(url, payload) {
    if (!url) return Promise.resolve(null);
    var body = JSON.stringify(payload);
    var headers = { 'Content-Type': 'application/json' };
    return fetch(url, { method: 'POST', headers: headers, body: body, keepalive: true, mode: 'cors' })
      .then(function (r) { return r.json(); })
      .catch(function () { return null; });
  }

  function getClientIp() {
    return fetch('https://api.ipify.org?format=json', { cache: 'no-store' })
      .then(function (r) { return r.json(); })
      .then(function (j) { return (j && j.ip) ? j.ip : null; })
      .catch(function () { return null; });
  }

  function parseClientRef(msg) {
    var m = String(msg || '').match(/cliente#([A-Za-z0-9_-]+)/i);
    if (!m) return null;
    return { message_reference: 'cliente#' + m[1], client_ref: m[1] };
  }

  function setClientRefInMessage(baseMsg, clientRef) {
    var msg = String(baseMsg || '');
    if (!clientRef) return msg;
    if (/cliente#[A-Za-z0-9_-]+/i.test(msg)) {
      return msg.replace(/cliente#[A-Za-z0-9_-]+/ig, 'cliente#' + clientRef);
    }
    return msg ? (msg + ' cliente#' + clientRef) : ('cliente#' + clientRef);
  }

  function fetchNextClientRef(cfg) {
    var localUrl = '/api/next-client-ref';
    var remoteUrl = (cfg && cfg.webhookUrl) ? cfg.webhookUrl.replace(/\/api\/track$/, '/api/next-client-ref') : null;
    return fetch(localUrl, { method: 'GET', mode: 'cors' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        console.log('[client] next-client-ref(local) =>', j);
        return (j && j.client_ref) ? j.client_ref : null;
      })
      .catch(function (e) {
        console.warn('[client] next-client-ref(local) falhou', e);
        return null;
      })
      .then(function (ref) {
        if (ref) return ref;
        if (!remoteUrl) return null;
        return fetch(remoteUrl, { method: 'GET', mode: 'cors' })
          .then(function (r) { return r.json(); })
          .then(function (j) {
            console.log('[client] next-client-ref(remote) =>', j);
            return (j && j.client_ref) ? j.client_ref : null;
          })
          .catch(function (e) {
            console.warn('[client] next-client-ref(remote) falhou', e);
            return null;
          });
      });
  }

  function shortEventToken(id) {
    if (!id) return null;
    return String(id).replace(/-/g, '').slice(0, 8).toUpperCase();
  }

  function handleRedirect(triggerType, btn, cfg) {
    var phone = (btn && btn.dataset.whatsapp) || cfg.defaultWhatsAppPhone || '';
    var baseMsg = cfg.defaultMessage || ''; // force fixed message from config
    var data = window.tracking && window.tracking.getTrackingData ? window.tracking.getTrackingData() : {};
    var eventId = uuid();
    var clientRef = window._CLIENT_REF_CACHE || null;
    function ensureClientRefNow() {
      if (clientRef) return Promise.resolve(clientRef);
      return fetchNextClientRef(cfg).then(function (ref) {
        clientRef = ref || null;
        window._CLIENT_REF_CACHE = clientRef;
        return clientRef;
      }).catch(function(){ return null; });
    }
    ensureClientRefNow().then(function(){
      var finalMsgBase = setClientRefInMessage(baseMsg, clientRef) || baseMsg;
      if (cfg.appendTrackingTokenToMessage) {
        var token = shortEventToken(eventId);
        if (token) finalMsgBase = finalMsgBase + ' #e:' + token;
      }
      var finalMsg = buildMessage(finalMsgBase, data, !!cfg.appendUtmToMessage);
      var url = isMobile() ? (isAndroid() ? androidIntentLink(phone, finalMsg) : waLink(phone, finalMsg)) : waLink(phone, finalMsg);
      var ref = parseClientRef(finalMsgBase) || {};
      var payload = Object.assign({}, data, {
        event_name: triggerType === 'auto' ? 'whatsapp_auto_redirect' : 'whatsapp_click',
        event_id: eventId,
        whatsapp_destination: phone,
        message: finalMsgBase,
        message_reference: ref.message_reference || (clientRef ? ('cliente#' + clientRef) : null),
        client_ref: ref.client_ref || clientRef || null,
        event_source_url: window.location.href
      });
      try {
        if (navigator.sendBeacon) {
          var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
          navigator.sendBeacon(cfg.webhookUrl, blob);
        } else {
          fetch(cfg.webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), keepalive: true, mode: 'cors' }).catch(function(){});
        }
      } catch (e) {}
      window.location.replace(url);
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    var cfg = window.TRACK_CONFIG || {};
    if (window.tracking && window.tracking.ensureMetaCookies) window.tracking.ensureMetaCookies();

    // removido: pageview_store via frontend. PageView é marcado no webhook BotConversa.

    var btn = document.getElementById('whatsapp-cta');
    if (btn) {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        handleRedirect('click', btn, cfg);
      });
    }

    var search = window.location.search || '';
    var debug = /[?&]debug=1(&|$)/.test(search);
    fetchNextClientRef(cfg).then(function (ref) { window._CLIENT_REF_CACHE = ref; }).catch(function(){});
    if (cfg.autoRedirectOnLoad && !debug && !isIOS()) {
      var delay = typeof cfg.autoRedirectDelayMs === 'number' ? cfg.autoRedirectDelayMs : 1000;
      setTimeout(function () {
        handleRedirect('auto', btn, cfg);
      }, delay);
    }
    if (isIOS()) {
      var opened = false;
      function openOnce() { if (opened) return; opened = true; handleRedirect('auto', btn, cfg); }
      document.addEventListener('touchstart', openOnce, { once: true });
      document.addEventListener('click', openOnce, { once: true });
    }
  });
})();
