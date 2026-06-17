const axios = require('axios');

class ChaosEngine {
  constructor(serviceName, managerUrl) {
    this.serviceName = serviceName || process.env.CHAOS_SERVICE_NAME || process.env.FUNCTION_NAME || 'unknown';
    this.managerUrl = managerUrl || process.env.CHAOS_MANAGER_URL || 'http://chaos-manager:8088';
    this.rules = new Map();
    this.hitCount = {};

    if (this.managerUrl) {
      this._startSync();
    }

    setInterval(() => this._cleanupExpired(), 10000);
    console.log(`[Chaos] Engine initialized for service=${this.serviceName}`);
  }

  _startSync() {
    setInterval(async () => {
      try {
        const url = `${this.managerUrl}/api/rules?service=${this.serviceName}`;
        const resp = await axios.get(url, { timeout: 3000 });
        if (resp.data && Array.isArray(resp.data)) {
          const now = Date.now();
          for (const rule of resp.data) {
            if (rule.enabled && rule.expires_at) {
              const exp = new Date(rule.expires_at).getTime();
              if (exp > now) {
                this.rules.set(rule.id, rule);
              } else {
                this.rules.delete(rule.id);
              }
            }
          }
        }
      } catch (e) {}
    }, 10000);
  }

  _cleanupExpired() {
    const now = Date.now();
    for (const [id, rule] of this.rules) {
      if (rule.expires_at) {
        const exp = new Date(rule.expires_at).getTime();
        if (exp <= now) {
          this.rules.delete(id);
        }
      }
    }
  }

  addRule(rule) {
    rule.id = rule.id || `rule-${Date.now()}`;
    rule.created_at = new Date().toISOString();
    if (rule.duration_sec && !rule.expires_at) {
      rule.expires_at = new Date(Date.now() + rule.duration_sec * 1000).toISOString();
    }
    rule.enabled = true;
    this.rules.set(rule.id, rule);
    console.log(`[Chaos] Rule added: id=${rule.id} type=${rule.type} prob=${rule.probability}`);
    return rule;
  }

  removeRule(id) {
    this.rules.delete(id);
    console.log(`[Chaos] Rule removed: id=${id}`);
  }

  clearRules() {
    this.rules.clear();
    console.log(`[Chaos] All rules cleared`);
  }

  listRules() {
    return Array.from(this.rules.values());
  }

  _findMatchingRule(req) {
    const now = Date.now();
    for (const rule of this.rules.values()) {
      if (!rule.enabled) continue;
      if (rule.expires_at) {
        const exp = new Date(rule.expires_at).getTime();
        if (exp <= now) continue;
      }
      if (rule.paths && rule.paths.length > 0) {
        if (!rule.paths.includes(req.path)) continue;
      }
      if (rule.headers) {
        let matched = true;
        for (const [k, v] of Object.entries(rule.headers)) {
          if (req.header(k) !== v) { matched = false; break; }
        }
        if (!matched) continue;
      }
      const prob = rule.probability || 1.0;
      if (Math.random() < prob) {
        return rule;
      }
    }
    return null;
  }

  middleware() {
    return async (req, res, next) => {
      const rule = this._findMatchingRule(req);
      if (!rule) {
        return next();
      }

      this.hitCount[rule.id] = (this.hitCount[rule.id] || 0) + 1;
      console.log(`[Chaos] Injecting ${rule.type} for ${req.method} ${req.path} (rule=${rule.id})`);

      switch (rule.type) {
        case 'latency':
          const ms = rule.latency_ms || 500;
          await new Promise(r => setTimeout(r, ms));
          return next();

        case 'error':
          const status = rule.status_code || 500;
          const msg = rule.message || 'Chaos Engineering: Injected Error';
          return res.status(status).json({
            error: msg,
            chaos_rule: rule.id,
            chaos_type: rule.type
          });

        case 'abort':
          return res.status(rule.status_code || 503).end();

        case 'exception':
          throw new Error(rule.message || 'Chaos Engineering: Simulated Exception');

        default:
          return next();
      }
    };
  }

  registerAdminRoutes(app) {
    app.get('/admin/chaos/rules', (req, res) => {
      res.json({ service: this.serviceName, rules: this.listRules() });
    });

    app.post('/admin/chaos/rules', (req, res) => {
      const rule = this.addRule({
        ...req.body,
        service_name: this.serviceName,
        probability: req.body.probability || 1.0,
        enabled: true
      });
      res.status(201).json(rule);
    });

    app.delete('/admin/chaos/rules/:id', (req, res) => {
      this.removeRule(req.params.id);
      res.json({ status: 'removed', id: req.params.id });
    });

    app.post('/admin/chaos/clear', (req, res) => {
      this.clearRules();
      res.json({ status: 'cleared' });
    });

    app.post('/admin/chaos/inject/latency', (req, res) => {
      const rule = this.addRule({
        id: `latency-${Date.now()}`,
        service_name: this.serviceName,
        type: 'latency',
        probability: req.body.prob || 1.0,
        latency_ms: req.body.ms || 500,
        duration_sec: req.body.duration_sec || 60,
        enabled: true
      });
      res.json(rule);
    });

    app.post('/admin/chaos/inject/error', (req, res) => {
      const rule = this.addRule({
        id: `error-${Date.now()}`,
        service_name: this.serviceName,
        type: 'error',
        probability: req.body.prob || 1.0,
        status_code: req.body.status_code || 500,
        message: req.body.message || '',
        duration_sec: req.body.duration_sec || 60,
        enabled: true
      });
      res.json(rule);
    });
  }
}

let defaultEngine = null;
function defaultEngine() {
  if (!defaultEngine) {
    defaultEngine = new ChaosEngine();
  }
  return defaultEngine;
}

module.exports = {
  ChaosEngine,
  middleware: () => defaultEngine().middleware(),
  addRule: (rule) => defaultEngine().addRule(rule),
  removeRule: (id) => defaultEngine().removeRule(id),
  clearRules: () => defaultEngine().clearRules(),
  listRules: () => defaultEngine().listRules(),
  registerAdminRoutes: (app) => defaultEngine().registerAdminRoutes(app)
};
