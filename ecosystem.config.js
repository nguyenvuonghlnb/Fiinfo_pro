module.exports = {
  apps : [
      {
        name: "fiinteck",
        script: "./main.py",
        watch: false,
        instances: 1,
        cron_restart: "00 07 * * *",
        stop_exit_codes: [0],
        env: {
            app_env: "production"
        },
        args: [
            "--color"
        ],
        log_date_format: "YYYY-MM-DD HH:mm:ss.SSS Z",
        merge_logs: true
      }
  ]
}