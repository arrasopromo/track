(function () {
  function q() {
    var params = {};
    var search = window.location.search || '';
    if (search.startsWith('?')) search = search.slice(1);
    if (search) {
      search.split('&').forEach(function (pair) {
        var kv = pair.split('=');
        var k = decodeURIComponent(kv[0] || '');
        var v = decodeURIComponent(kv[1] || '');
        if (k) params[k] = v;
      });
    }
    var hash = window.location.hash || '';
    var i = hash.indexOf('?');
    if (i >= 0) {
      var h = hash.slice(i + 1);
      h.split('&').forEach(function (pair) {
        var kv = pair.split('=');
        var k = decodeURIComponent(kv[0] || '');
        var v = decodeURIComponent(kv[1] || '');
        if (k) params[k] = v;
      });
    }
    return params;
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

  function gc(name) {
    var m = document.cookie.match('(?:^|; )' + name.replace(/([.$?*|{}()\[\]\\\/\+^])/g, '\\$1') + '=([^;]*)');
    return m ? decodeURIComponent(m[1]) : null;
  }

  function sc(name, value, days) {
    var expires = '';
    if (days) {
      var d = new Date();
      d.setTime(d.getTime() + days * 86400000);
      expires = '; expires=' + d.toUTCString();
    }
    document.cookie = name + '=' + encodeURIComponent(value) + expires + '; path=/';
  }

  function lsGet(k) {
    try { return window.localStorage.getItem(k); } catch (e) { return null; }
  }

  function lsSet(k, v) {
    try { window.localStorage.setItem(k, v); } catch (e) {}
  }

  function ensureSessionId() {
    var sid = gc('sid');
    if (!sid) {
      sid = 'sid.' + uuid();
      sc('sid', sid, 180);
    }
    return sid;
  }

  function ensureFbp() {
    var fbp = gc('_fbp');
    if (!fbp) {
      var ts = Date.now();
      var rnd = Math.floor(Math.random() * 10_000_000_000);
      fbp = 'fb.1.' + ts + '.' + rnd;
      sc('_fbp', fbp, 180);
    }
    return fbp;
  }

  function ensureFbc(query) {
    var fbc = gc('_fbc');
    var fbclid = query.fbclid || lsGet('fbclid') || null;
    if (query.fbclid) lsSet('fbclid', query.fbclid);
    if (fbclid && !fbc) {
      var ts = Date.now();
      fbc = 'fb.1.' + ts + '.' + fbclid;
      sc('_fbc', fbc, 180);
    }
    return fbc;
  }

  function baseData() {
    var query = q();
    var fbp = ensureFbp();
    var fbc = ensureFbc(query) || gc('_fbc');
    var sid = ensureSessionId();
    return {
      utm_source: query.utm_source || null,
      utm_medium: query.utm_medium || null,
      utm_campaign: query.utm_campaign || null,
      utm_content: query.utm_content || null,
      utm_term: query.utm_term || null,
      fbclid: query.fbclid || null,
      gclid: query.gclid || null,
      msclkid: query.msclkid || null,
      fbc: fbc || null,
      fbp: fbp || null,
      session_id: sid,
      page_url: window.location.href,
      referrer: document.referrer || null,
      user_agent: navigator.userAgent,
      timestamp: new Date().toISOString()
    };
  }

  function ensureMetaCookies() {
    ensureFbp();
    ensureFbc(q());
  }

  window.tracking = {
    getTrackingData: baseData,
    ensureMetaCookies: ensureMetaCookies
  };
})();