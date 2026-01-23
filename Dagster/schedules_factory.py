from dagster import ScheduleDefinition


def schedule_factory(job_name: str, cron_schedule: str, group_name: str = "", run_config={}):
    return ScheduleDefinition(
        job=job_name,
        cron_schedule=cron_schedule,
        name=f"{job_name}_schedule",
        execution_timezone="UTC",  # Optional
        run_config=run_config,
        tags={"group": group_name}
    )


jobs_schedules = [schedule_factory(job.name, "0 * * * *", "...",
                  {"ops": {
                          'op_name': {
                                      "config": {
                                                ...
                                                }
                                      }
                         }
                   }) for job in ...]

