/**
 * APEX MULTI-MARKET TJR ENGINE
 * PM2 Ecosystem Configuration — Phase 8: Always-On Process Management
 *
 * Usage:
 *   Start:       pm2 start ecosystem.config.cjs
 *   Stop:        pm2 stop apex-trader
 *   Restart:     pm2 restart apex-trader
 *   Reload:      pm2 reload apex-trader   (zero-downtime if stateless)
 *   Status:      pm2 status
 *   Logs:        pm2 logs apex-trader --lines 200
 *   Health:      curl http://localhost:8080/health
 *   Save config: pm2 save
 *   Startup:     pm2 startup   (generates OS-level startup command)
 *
 * Environment variables:
 *   All secrets must be in /home/user/apex-trader/.env (never committed to git).
 *   Required for live OANDA: APEX_OANDA_API_TOKEN, APEX_OANDA_ACCOUNT_ID
 *   Optional for alerts:     APEX_TELEGRAM_BOT_TOKEN, APEX_TELEGRAM_CHAT_ID
 *                            APEX_DISCORD_WEBHOOK_URL
 *                            APEX_SMTP_HOST, APEX_SMTP_USER, APEX_SMTP_PASSWORD
 */
module.exports = {
  apps: [
    {
      name: 'apex-trader',
      script: 'python3',
      args: 'live/api_server.py',
      cwd: '/home/user/apex-trader',

      // Environment
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/home/user/apex-trader',
      },

      // Process management
      watch: false,                // Disable PM2 file watching (scheduler handles its own loops)
      instances: 1,                // Single instance — state is in-process
      exec_mode: 'fork',

      // Auto-restart policy
      autorestart: true,
      max_restarts: 10,            // Max consecutive restarts before PM2 stops trying
      min_uptime: '10s',           // Process must run at least 10s to count as a successful start
      restart_delay: 5000,         // 5 second delay between restarts (ms)
      exp_backoff_restart_delay: 100,  // Exponential backoff on repeated restarts

      // Memory limit
      max_memory_restart: '1G',

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: 'logs/pm2-error.log',
      out_file: 'logs/pm2-out.log',
      merge_logs: true,
      log_type: 'json',            // Structured log output

      // Graceful shutdown
      kill_timeout: 10000,         // 10 seconds to shut down gracefully (ms)
      listen_timeout: 30000,       // 30 seconds to start listening (ms)

      // Health check support (requires custom health_check script)
      // Uncomment if using PM2 Pro or custom health checks:
      // health_check_grace_period: 60000,
    }
  ]
}
