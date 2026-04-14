module.exports = {
  apps: [
    {
      name: 'apex-trader',
      script: 'python3',
      args: 'live/api_server.py',
      cwd: '/home/user/apex-trader',
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1'
      },
      watch: false,
      instances: 1,
      exec_mode: 'fork',
      max_memory_restart: '512M',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: 'logs/pm2-error.log',
      out_file: 'logs/pm2-out.log'
    }
  ]
}
